package server

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	minio "github.com/minio/minio-go/v7"
)

const (
	// proxyMinThroughput is the slowest client we still serve before timing
	// out. 100 kB/s tolerates congested mobile and conference WiFi; slower
	// is indistinguishable from a stalled connection. This bounds how long a
	// slowloris attacker can hold one connection per byte sent, but the real
	// resource bound is connection/fd limits, not the timeout.
	proxyMinThroughput = 100_000 // bytes/sec

	// proxyTimeoutSlack absorbs TLS handshake, S3 first-byte latency, and
	// TCP slow start. Dominates for small objects (narinfos, listings).
	proxyTimeoutSlack = 5 * time.Minute
)

// ProxyWriteTimeout returns the per-request write deadline for streaming an
// object of the given size. The global server WriteTimeout is short to bound
// slowloris on API endpoints; large NAR streams need a budget proportional to
// their size.
func ProxyWriteTimeout(size int64) time.Duration {
	if size < 0 {
		size = 0
	}

	return proxyTimeoutSlack + time.Duration(size/proxyMinThroughput)*time.Second
}

// byteRange is a single parsed Range header span, inclusive.
type byteRange struct {
	start, end int64
}

func (br byteRange) length() int64 { return br.end - br.start + 1 }

// errUnsatisfiableRange marks a Range that cannot be served (RFC 7233 §4.4).
var errUnsatisfiableRange = errors.New("unsatisfiable range")

// parseSingleRange parses a single-span "bytes=" Range header against an
// object of the given size. Returns nil when no Range header is present
// (serve full object) or when the spec is multi-range / malformed (RFC 7233
// allows ignoring such requests). Returns errUnsatisfiableRange for ranges
// entirely past EOF.
func parseSingleRange(spec string, size int64) (*byteRange, error) {
	if spec == "" || size <= 0 {
		return nil, nil //nolint:nilnil // no Range header
	}

	const prefix = "bytes="
	if !strings.HasPrefix(spec, prefix) {
		return nil, nil //nolint:nilnil // unknown range unit, ignore per RFC 7233
	}

	spec = strings.TrimPrefix(spec, prefix)
	if strings.Contains(spec, ",") {
		// Multi-range needs multipart/byteranges responses; not worth the
		// complexity for binary cache traffic. Serve full object.
		return nil, nil //nolint:nilnil // multi-range, ignore per RFC 7233
	}

	startStr, endStr, ok := strings.Cut(spec, "-")
	if !ok {
		return nil, nil //nolint:nilnil // malformed Range header, ignore per RFC 7233
	}

	startStr = strings.TrimSpace(startStr)
	endStr = strings.TrimSpace(endStr)

	var br byteRange

	switch {
	case startStr == "" && endStr == "":
		return nil, nil //nolint:nilnil // malformed Range header, ignore per RFC 7233

	case startStr == "":
		// Suffix range: last N bytes.
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n <= 0 {
			return nil, nil //nolint:nilnil,nilerr // malformed Range header, ignore per RFC 7233
		}

		if n > size {
			n = size
		}

		br = byteRange{start: size - n, end: size - 1}

	default:
		start, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil || start < 0 {
			return nil, nil //nolint:nilnil,nilerr // malformed Range header, ignore per RFC 7233
		}

		if start >= size {
			return nil, errUnsatisfiableRange
		}

		end := size - 1

		if endStr != "" {
			end, err = strconv.ParseInt(endStr, 10, 64)
			if err != nil || end < start {
				return nil, nil //nolint:nilnil,nilerr // malformed Range header, ignore per RFC 7233
			}

			if end >= size {
				end = size - 1
			}
		}

		br = byteRange{start: start, end: end}
	}

	return &br, nil
}

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

	// log: log/{name}.drv — name alphabet matches nix's nameRegexStr
	// in src/libstore/path.cc: [A-Za-z0-9+\-._?=]
	logRe = regexp.MustCompile(`^log/[a-zA-Z0-9+._?=-]+\.drv$`)

	// realisations: realisations/{hash-algo}:{hex}!{output}.doi
	realisationsRe = regexp.MustCompile(`^realisations/[a-z0-9]+:[a-zA-Z0-9+/=]+![a-zA-Z0-9+._?=-]+\.doi$`)
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

	// Reject ".." path segments. Don't use a plain Contains check here:
	// store-path names can legitimately contain ".." (e.g. hm_..zlogout.drv).
	if strings.HasPrefix(path, "../") || strings.HasSuffix(path, "/..") ||
		strings.Contains(path, "/../") || path == ".." {
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
		w.Header().Set("Accept-Ranges", "bytes")
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
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" && ifNoneMatch == objInfo.ETag {
		w.WriteHeader(http.StatusNotModified)

		return
	}

	// Handle conditional requests (If-Modified-Since)
	if ifModifiedSince := r.Header.Get("If-Modified-Since"); ifModifiedSince != "" {
		t, parseErr := http.ParseTime(ifModifiedSince)
		if parseErr == nil && !objInfo.LastModified.After(t) {
			w.WriteHeader(http.StatusNotModified)

			return
		}
	}

	isNarinfo := strings.HasSuffix(key, ".narinfo")

	// Range support is for resuming large NAR downloads on flaky links. We
	// translate the Range header into an S3 range request rather than using
	// http.ServeContent: the latter seeks within minio.Object, which aborts
	// and re-issues the underlying S3 stream.
	var rng *byteRange

	if !isNarinfo {
		var rangeErr error

		rng, rangeErr = parseSingleRange(r.Header.Get("Range"), objInfo.Size)
		if errors.Is(rangeErr, errUnsatisfiableRange) {
			w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(objInfo.Size, 10))
			http.Error(w, "Requested Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)

			return
		}
	}

	// Wait for rate limiter again for the actual GET
	if err := s.S3RateLimiter.Wait(r.Context()); err != nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)

		return
	}

	getOpts := minio.GetObjectOptions{}
	if rng != nil {
		if err := getOpts.SetRange(rng.start, rng.end); err != nil {
			http.Error(w, "invalid range", http.StatusBadRequest)

			return
		}
	}

	obj, err := s.MinioClient.GetObject(r.Context(), s.Bucket, key, getOpts)
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
	if isNarinfo {
		s.serveDecompressedNarinfo(w, obj, &objInfo)

		return
	}

	// Override the global short WriteTimeout: large NARs need a budget
	// proportional to their size.
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Now().Add(ProxyWriteTimeout(objInfo.Size))); err != nil {
		slog.Debug("Failed to extend write deadline", "key", key, "error", err)
	}

	setProxyHeaders(w, &objInfo)
	w.Header().Set("Accept-Ranges", "bytes")

	if rng != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.start, rng.end, objInfo.Size))
		w.Header().Set("Content-Length", strconv.FormatInt(rng.length(), 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(objInfo.Size, 10))
		w.WriteHeader(http.StatusOK)
	}

	if _, err := io.Copy(w, obj); err != nil {
		slog.Debug("Failed to stream S3 object to client", "key", key, "error", err)
	}
}

// serveDecompressedNarinfo reads a narinfo from S3 and writes the decompressed
// content to the response. Narinfos are tiny (~500 bytes compressed) so
// buffering the whole thing is fine.
//
// Narinfos are stored zstd-compressed in S3 with Content-Encoding: zstd.
// A transparent proxy (e.g. Cloudflare Tunnel) may decompress the data and
// strip the Content-Encoding header before it reaches us. We only decompress
// when the Content-Encoding header is still present.
func (s *Service) serveDecompressedNarinfo(w http.ResponseWriter, obj *minio.Object, info *minio.ObjectInfo) {
	data, err := io.ReadAll(obj)
	if err != nil {
		slog.Error("Failed to read narinfo from S3", "error", err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)

		return
	}

	plain := data

	// S3 stores Content-Encoding either as a standard header or as user
	// metadata (X-Amz-Meta-Content-Encoding) depending on the implementation.
	contentEncoding := info.Metadata.Get("Content-Encoding")
	if contentEncoding == "" {
		contentEncoding = info.Metadata.Get("X-Amz-Meta-Content-Encoding")
	}

	if strings.EqualFold(contentEncoding, "zstd") {
		decoder, ok := zstdDecoderPool.Get().(*zstd.Decoder)
		if !ok {
			slog.Error("Failed to get zstd decoder from pool")
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)

			return
		}
		defer zstdDecoderPool.Put(decoder)

		plain, err = decoder.DecodeAll(data, nil)
		if err != nil {
			slog.Error("Failed to decompress narinfo", "error", err)
			http.Error(w, "Bad Gateway", http.StatusBadGateway)

			return
		}
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
