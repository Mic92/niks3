package server

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
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

// zstdWindowLimit caps decoder memory. Our client encodes with an 8 MiB window.
const zstdWindowLimit = 32 << 20

// ReadProxyHandler proxies GET/HEAD requests for Nix binary cache objects from S3.
// It is registered without a method prefix (to avoid ServeMux conflicts) and
// rejects non-GET/HEAD methods itself.
func (s *Service) ReadProxyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)

		return
	}

	key := strings.TrimPrefix(r.URL.Path, "/")

	if key == "" {
		s.RootRedirectHandler(w, r)

		return
	}

	class := ClassifyCacheKey(key)
	if class == nil {
		http.NotFound(w, r)

		return
	}

	if s.ReadRedirectTTL > 0 && class.Redirectable {
		s.redirectToS3(w, r, key)

		return
	}

	if err := s.S3RateLimiter.Wait(r.Context()); err != nil {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)

		return
	}

	s.serveObject(w, r, key, class)
}

// redirectToS3 sends the client to a presigned URL for key. Presigning is
// local; a missing object 404s from S3 rather than from here.
func (s *Service) redirectToS3(w http.ResponseWriter, r *http.Request, key string) {
	presigned, err := s.MinioClient.PresignedGetObject(r.Context(), s.Bucket, key, s.ReadRedirectTTL, nil)
	if err != nil {
		slog.Error("Failed to presign S3 object", "key", key, "error", err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)

		return
	}

	w.Header().Set("Cache-Control", "no-store") // URL expires

	// 307 keeps the method, so HEAD stays HEAD.
	//nolint:gosec // G710: host comes from config, key already matched an object class
	http.Redirect(w, r, presigned.String(), http.StatusTemporaryRedirect)
}

// storedZstd reports Content-Encoding: zstd. A transparent proxy (e.g.
// Cloudflare Tunnel) may already have decoded it and dropped the header.
func storedZstd(info *minio.ObjectInfo) bool {
	enc := info.Metadata.Get("Content-Encoding")
	if enc == "" {
		enc = info.Metadata.Get("X-Amz-Meta-Content-Encoding")
	}

	return strings.EqualFold(enc, "zstd")
}

func acceptsZstd(r *http.Request) bool {
	for part := range strings.SplitSeq(r.Header.Get("Accept-Encoding"), ",") {
		name, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		if !strings.EqualFold(strings.TrimSpace(name), "zstd") {
			continue
		}

		_, q, hasQ := strings.Cut(strings.ReplaceAll(params, " ", ""), "q=")

		return !hasQ || (q != "0" && q != "0.0" && q != "0.00" && q != "0.000")
	}

	return false
}

func notModified(r *http.Request, info *minio.ObjectInfo) bool {
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		return inm == info.ETag
	}

	if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, err := http.ParseTime(ims); err == nil && !info.LastModified.After(t) {
			return true
		}
	}

	return false
}

// serveObject streams key: passed through, range-sliced (raw only), or
// zstd-decoded when the client did not ask for zstd.
func (s *Service) serveObject(w http.ResponseWriter, r *http.Request, key string, class *ObjectClass) {
	info, err := s.MinioClient.StatObject(r.Context(), s.Bucket, key, minio.StatObjectOptions{})
	if err != nil {
		s.handleProxyS3Error(w, err, key)

		return
	}

	s.S3RateLimiter.RecordSuccess()

	h := w.Header()
	h.Set("Content-Type", class.ContentType)
	h.Set("X-Content-Type-Options", "nosniff")

	if info.ETag != "" {
		h.Set("ETag", info.ETag)
	}

	if !info.LastModified.IsZero() {
		h.Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	}

	if notModified(r, &info) {
		w.WriteHeader(http.StatusNotModified)

		return
	}

	compressed := class.Zstd && storedZstd(&info)
	decode := compressed && !acceptsZstd(r)

	if compressed && !decode {
		h.Set("Content-Encoding", "zstd")
		h.Add("Vary", "Accept-Encoding")
	}

	// Range only over bytes we pass through verbatim.
	var rng *byteRange

	if !compressed {
		h.Set("Accept-Ranges", "bytes")

		rng, err = parseSingleRange(r.Header.Get("Range"), info.Size)
		if errors.Is(err, errUnsatisfiableRange) {
			h.Set("Content-Range", "bytes */"+strconv.FormatInt(info.Size, 10))
			http.Error(w, "Requested Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)

			return
		}
	}

	status := http.StatusOK

	switch {
	case rng != nil:
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.start, rng.end, info.Size))
		h.Set("Content-Length", strconv.FormatInt(rng.length(), 10))

		status = http.StatusPartialContent
	case !decode:
		h.Set("Content-Length", strconv.FormatInt(info.Size, 10))
	}

	if r.Method == http.MethodHead {
		w.WriteHeader(status)

		return
	}

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

	var body io.Reader = obj

	if decode {
		dec, err := zstd.NewReader(obj, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxWindow(zstdWindowLimit))
		if err != nil {
			slog.Error("Failed to create zstd decoder", "error", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)

			return
		}
		defer dec.Close()

		body = dec
	}

	// Override the global short WriteTimeout: large NARs need a budget
	// proportional to their size.
	rc := http.NewResponseController(w)
	if err := rc.SetWriteDeadline(time.Now().Add(ProxyWriteTimeout(info.Size))); err != nil {
		slog.Debug("Failed to extend write deadline", "key", key, "error", err)
	}

	w.WriteHeader(status)

	if _, err := io.Copy(w, body); err != nil {
		slog.Debug("Failed to stream S3 object to client", "key", key, "error", err)
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
