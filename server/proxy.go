package server

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
	minio "github.com/minio/minio-go/v7"
)

// zstdDecoderPool pools zstd decoders to reduce memory allocations.
// Each decoder holds ~130KB of window state; pooling avoids re-allocating
// that on every proxied narinfo request.
var zstdDecoderPool = sync.Pool{ //nolint:gochecknoglobals // sync.Pool should be global
	New: func() any {
		decoder, err := zstd.NewReader(nil)
		if err != nil {
			panic("failed to create zstd decoder: " + err.Error())
		}

		return decoder
	},
}

// Nix base32 alphabet: 0123456789abcdfghijklmnpqrsvwxyz
// (note: no 'e', 't', 'u' — differs from standard base32).
const nixBase32Alphabet = "0123456789abcdfghijklmnpqrsvwxyz"

var (
	// narinfo: {32-char nix-base32 hash}.narinfo
	narinfoRe = regexp.MustCompile(`^[` + nixBase32Alphabet + `]{32}\.narinfo$`)

	// nar: nar/{52-char nix-base32 hash}.nar[.zst|.xz|.bz2]
	narRe = regexp.MustCompile(`^nar/[` + nixBase32Alphabet + `]{52}\.nar(\.zst|\.xz|\.bz2)?$`)

	// ls: {32-char nix-base32 hash}.ls
	lsRe = regexp.MustCompile(`^[` + nixBase32Alphabet + `]{32}\.ls$`)

	// log: log/{name}.drv — name can contain alphanumerics, hyphens, dots, underscores
	logRe = regexp.MustCompile(`^log/[a-zA-Z0-9._-]+\.drv$`)

	// realisations: realisations/{hash-algo}:{hex}!{output}.doi
	realisationsRe = regexp.MustCompile(`^realisations/[a-z0-9]+:[a-zA-Z0-9+/=]+![a-zA-Z0-9_-]+\.doi$`)
)

// IsValidCachePath checks whether a path matches a known Nix binary cache object pattern.
// It rejects path traversal, leading slashes, and any pattern outside the allowlist.
func IsValidCachePath(path string) bool {
	if path == "" {
		return false
	}

	// Reject leading slash
	if strings.HasPrefix(path, "/") {
		return false
	}

	// Reject path traversal
	if strings.Contains(path, "..") {
		return false
	}

	// Special files
	if path == "nix-cache-info" || path == "index.html" {
		return true
	}

	// Check against known patterns
	if narinfoRe.MatchString(path) {
		return true
	}

	if narRe.MatchString(path) {
		return true
	}

	if lsRe.MatchString(path) {
		return true
	}

	if logRe.MatchString(path) {
		return true
	}

	if realisationsRe.MatchString(path) {
		return true
	}

	return false
}

// ReadProxyHandler proxies GET/HEAD requests for Nix binary cache objects from S3.
// It is registered without a method prefix (to avoid ServeMux conflicts) and
// rejects non-GET/HEAD methods itself.
func (s *Service) ReadProxyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)

		return
	}

	// Extract object key from URL path (strip leading /)
	key := strings.TrimPrefix(r.URL.Path, "/")

	// Root path: serve landing page or redirect, same as non-proxy mode.
	if key == "" {
		s.RootRedirectHandler(w, r)

		return
	}

	// Validate path against allowlist
	if !IsValidCachePath(key) {
		http.NotFound(w, r)

		return
	}

	// Wait for rate limiter
	if err := s.S3RateLimiter.Wait(r.Context()); err != nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)

		return
	}

	if r.Method == http.MethodHead {
		s.handleProxyHead(w, r, key)

		return
	}

	s.handleProxyGet(w, r, key)
}

func (s *Service) handleProxyHead(w http.ResponseWriter, r *http.Request, key string) {
	objInfo, err := s.MinioClient.StatObject(r.Context(), s.Bucket, key, minio.StatObjectOptions{})
	if err != nil {
		s.handleProxyS3Error(w, err, key)

		return
	}

	s.S3RateLimiter.RecordSuccess()

	setProxyHeaders(w, &objInfo)

	// For narinfos we decompress on GET, so the compressed Content-Length
	// from S3 would be wrong. Omit it — HTTP allows HEAD without Content-Length.
	if !strings.HasSuffix(key, ".narinfo") {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", objInfo.Size))
	} else {
		w.Header().Set("Content-Type", "text/x-nix-narinfo")
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Service) handleProxyGet(w http.ResponseWriter, r *http.Request, key string) {
	// First stat the object to get metadata and handle conditional requests
	// before committing to a full GET.
	objInfo, err := s.MinioClient.StatObject(r.Context(), s.Bucket, key, minio.StatObjectOptions{})
	if err != nil {
		s.handleProxyS3Error(w, err, key)

		return
	}

	s.S3RateLimiter.RecordSuccess()

	// Handle conditional requests (If-None-Match)
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		if ifNoneMatch == objInfo.ETag {
			w.WriteHeader(http.StatusNotModified)

			return
		}
	}

	// Handle conditional requests (If-Modified-Since)
	if ifModifiedSince := r.Header.Get("If-Modified-Since"); ifModifiedSince != "" {
		t, parseErr := http.ParseTime(ifModifiedSince)
		if parseErr == nil && !objInfo.LastModified.After(t) {
			w.WriteHeader(http.StatusNotModified)

			return
		}
	}

	// Wait for rate limiter again for the actual GET
	if err := s.S3RateLimiter.Wait(r.Context()); err != nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)

		return
	}

	obj, err := s.MinioClient.GetObject(r.Context(), s.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		s.handleProxyS3Error(w, err, key)

		return
	}

	defer func() {
		if err := obj.Close(); err != nil {
			slog.Warn("Failed to close S3 object", "key", key, "error", err)
		}
	}()

	s.S3RateLimiter.RecordSuccess()

	// Narinfos are stored zstd-compressed in S3, but Nix's HTTP binary cache
	// client expects plain text. Decompress on the fly (narinfos are ~500 bytes).
	if strings.HasSuffix(key, ".narinfo") {
		s.serveDecompressedNarinfo(w, obj, &objInfo)

		return
	}

	setProxyHeaders(w, &objInfo)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", objInfo.Size))
	w.WriteHeader(http.StatusOK)

	if _, err := io.Copy(w, obj); err != nil {
		slog.Debug("Failed to stream S3 object to client", "key", key, "error", err)
	}
}

// serveDecompressedNarinfo reads a zstd-compressed narinfo from S3 and writes
// the decompressed content to the response. Narinfos are tiny (~500 bytes
// compressed) so buffering the whole thing is fine.
func (s *Service) serveDecompressedNarinfo(w http.ResponseWriter, obj *minio.Object, info *minio.ObjectInfo) {
	compressed, err := io.ReadAll(obj)
	if err != nil {
		slog.Error("Failed to read narinfo from S3", "error", err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)

		return
	}

	decoder, ok := zstdDecoderPool.Get().(*zstd.Decoder)
	if !ok {
		slog.Error("Failed to get zstd decoder from pool")
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)

		return
	}
	defer zstdDecoderPool.Put(decoder)

	plain, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		slog.Error("Failed to decompress narinfo", "error", err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)

		return
	}

	w.Header().Set("Content-Type", "text/x-nix-narinfo")

	if info.ETag != "" {
		w.Header().Set("ETag", info.ETag)
	}

	if !info.LastModified.IsZero() {
		w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(plain)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(plain)
}

// setProxyHeaders sets response headers from S3 object metadata.
func setProxyHeaders(w http.ResponseWriter, info *minio.ObjectInfo) {
	if info.ContentType != "" {
		w.Header().Set("Content-Type", info.ContentType)
	}

	if info.ETag != "" {
		w.Header().Set("ETag", info.ETag)
	}

	if !info.LastModified.IsZero() {
		w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	}
}

// handleProxyS3Error handles S3 errors from proxy requests.
func (s *Service) handleProxyS3Error(w http.ResponseWriter, err error, key string) {
	if isRateLimitError(err) {
		s.S3RateLimiter.RecordThrottle()

		slog.Warn("S3 rate limit hit during proxy", "key", key, "error", err)
		w.Header().Set("Retry-After", "2")
		http.Error(w, "S3 rate limit exceeded, please retry", http.StatusTooManyRequests)

		return
	}

	errResp := minio.ToErrorResponse(err)
	if errResp.Code == "NoSuchKey" || errResp.StatusCode == http.StatusNotFound {
		http.NotFound(w, nil)

		return
	}

	slog.Error("S3 error during proxy", "key", key, "error", err)
	http.Error(w, "Bad Gateway", http.StatusBadGateway)
}
