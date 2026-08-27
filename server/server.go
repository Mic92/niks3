package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Mic92/niks3/ratelimit"
	"github.com/Mic92/niks3/server/oidc"
	"github.com/Mic92/niks3/server/pg"
	"github.com/Mic92/niks3/server/signing"
	"github.com/jackc/pgx/v5/pgxpool"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type options struct {
	DBConnectionString string
	HTTPAddr           string

	S3Endpoint     string
	S3AccessKey    string
	S3SecretKey    string
	S3UseSSL       bool
	S3UseIAM       bool
	S3Bucket       string
	S3Region       string
	S3BucketLookup minio.BucketLookupType
	S3Concurrency  int
	S3RateLimit    float64

	APIToken string

	SignKeyPaths    []string
	CacheURL        string
	ServerURL       string
	OIDCConfigPath  string
	EnableReadProxy bool

	// ReadRedirectTTL > 0 redirects NAR reads to presigned S3 URLs with this
	// lifetime instead of streaming them. Requires EnableReadProxy.
	ReadRedirectTTL time.Duration

	// MaxNarSize is the maximum uncompressed NAR size in bytes accepted for
	// upload. 0 means unlimited.
	MaxNarSize uint64

	// CachePriority is advertised in nix-cache-info; lower wins.
	CachePriority int

	// MTLSProxyHeader, when set, names a header the reverse proxy sets to
	// "SUCCESS" after verifying a client certificate (e.g. nginx's
	// $ssl_client_verify). Requests carrying it are accepted without a
	// bearer token. ONLY enable behind a proxy that overrides the header
	// on every request — including failed/anonymous ones.
	MTLSProxyHeader string

	// MTLSSubjectHeader names the header carrying the verified cert's
	// subject DN (e.g. nginx's $ssl_client_s_dn). Used with
	// MTLSBoundSubjects.
	MTLSSubjectHeader string

	// MTLSBoundSubjects restricts mTLS auth to certs whose subject DN
	// matches one of these glob patterns. If this and MTLSBoundSubjectsRead
	// are both empty, any verified cert is fully trusted.
	MTLSBoundSubjects []string

	// MTLSBoundSubjectsRead, when non-empty, gates the read proxy behind
	// mTLS: only clients presenting a cert whose subject DN matches one of
	// these globs may read. Empty = read proxy is public (default).
	MTLSBoundSubjectsRead []string

	// TLSCert/TLSKey, when both set, make the server terminate TLS itself
	// instead of expecting a reverse proxy.
	TLSCert string
	TLSKey  string

	// TLSClientCA, when set with TLSCert/TLSKey, enables native mTLS:
	// the server requests and verifies client certs against this CA bundle.
	// The cert subject is checked against MTLSBoundSubjects directly —
	// no proxy headers involved.
	TLSClientCA string

	Debug bool
}

type Service struct {
	Pool                  *pgxpool.Pool
	MinioClient           *minio.Client
	Bucket                string
	S3Concurrency         int
	S3RateLimiter         *ratelimit.AdaptiveRateLimiter
	APIToken              string
	SigningKeys           []*signing.Key
	CacheURL              string
	ServerURL             string
	OIDCValidator         *oidc.Validator
	EnableReadProxy       bool
	MTLSProxyHeader       string
	MTLSSubjectHeader     string
	MTLSBoundSubjects     []string
	MTLSBoundSubjectsRead []string

	// See options.ReadRedirectTTL.
	ReadRedirectTTL time.Duration

	// MaxNarSize is advertised via /api/cache-config and enforced on
	// pending-closure creation. 0 means unlimited.
	MaxNarSize uint64

	// CachePriority is written to nix-cache-info. 0 means defaultCachePriority.
	CachePriority int

	// NativeMTLS is set when the server terminates TLS itself with a
	// client CA — mtlsCheck reads r.TLS.PeerCertificates directly
	// instead of trusting proxy headers.
	NativeMTLS bool

	GCTasks *GCTaskStore
	Metrics *Metrics
}

// Close closes the database connection pool.
func (s *Service) Close() {
	s.Pool.Close()
}

const (
	dbConnectionTimeout = 10 * time.Second

	// shutdownTimeout bounds how long we wait for in-flight requests to
	// finish on SIGINT/SIGTERM before forcing the connections closed. Kept
	// short so orchestrators don't escalate to SIGKILL; large NAR streams
	// that exceed it are dropped rather than holding up the shutdown.
	shutdownTimeout = 10 * time.Second
)

func runServer(opts *options) error {
	ctx, cancel := context.WithTimeout(context.Background(), dbConnectionTimeout)
	defer cancel()

	pool, err := pg.Connect(ctx, opts.DBConnectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	var creds *credentials.Credentials
	if opts.S3UseIAM {
		creds = credentials.NewIAM("")
	} else {
		creds = credentials.NewStaticV4(opts.S3AccessKey, opts.S3SecretKey, "")
	}

	minioClient, err := minio.New(opts.S3Endpoint, &minio.Options{
		Creds:        creds,
		Secure:       opts.S3UseSSL,
		Region:       opts.S3Region,
		BucketLookup: opts.S3BucketLookup,
	})
	if err != nil {
		return fmt.Errorf("failed to create minio s3 client: %w", err)
	}

	service := &Service{
		Pool:                  pool,
		MinioClient:           minioClient,
		Bucket:                opts.S3Bucket,
		S3Concurrency:         opts.S3Concurrency,
		S3RateLimiter:         ratelimit.NewAdaptiveRateLimiter(opts.S3RateLimit, "s3"),
		APIToken:              opts.APIToken,
		MTLSProxyHeader:       opts.MTLSProxyHeader,
		MTLSSubjectHeader:     opts.MTLSSubjectHeader,
		MTLSBoundSubjects:     opts.MTLSBoundSubjects,
		MTLSBoundSubjectsRead: opts.MTLSBoundSubjectsRead,
		CacheURL:              opts.CacheURL,
		ServerURL:             opts.ServerURL,
		MaxNarSize:            opts.MaxNarSize,
		CachePriority:         opts.CachePriority,
		GCTasks:               NewGCTaskStore(pool),
		Metrics:               NewMetrics(),
	}

	// Initialize OIDC validator if configured
	if opts.OIDCConfigPath != "" {
		oidcCfg, err := oidc.LoadConfig(opts.OIDCConfigPath)
		if err != nil {
			return fmt.Errorf("failed to load OIDC config: %w", err)
		}

		initCtx, initCancel := context.WithTimeout(context.Background(), 30*time.Second)
		validator, err := oidc.NewValidator(initCtx, oidcCfg)

		initCancel()

		if err != nil {
			return fmt.Errorf("failed to initialize OIDC validator: %w", err)
		}

		service.OIDCValidator = validator

		slog.Info("OIDC authentication enabled", "config", opts.OIDCConfigPath)
	}

	// Load signing keys
	if len(opts.SignKeyPaths) == 0 {
		slog.Warn("No signing keys configured; narinfo signing will rely on CA entries only (if any)")
	} else {
		service.SigningKeys = make([]*signing.Key, 0, len(opts.SignKeyPaths))
	}

	for _, path := range opts.SignKeyPaths {
		key, err := signing.LoadKeyFromFile(path)
		if err != nil {
			return fmt.Errorf("failed to load signing key from %s: %w", path, err)
		}

		service.SigningKeys = append(service.SigningKeys, key)
		slog.Info("Loaded signing key", "name", key.Name, "path", path)
	}

	// Initialize the bucket with nix-cache-info if it doesn't exist
	// Use a 30-second timeout to prevent hanging indefinitely
	initCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := service.InitializeBucket(initCtx); err != nil {
		return fmt.Errorf("failed to initialize bucket: %w", err)
	}

	metricsCtx, stopMetrics := context.WithCancel(context.Background())
	defer stopMetrics()
	service.StartInventoryRefresh(metricsCtx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", service.HealthCheckHandler)
	mux.HandleFunc("GET /healthz", service.HealthCheckHandler)
	mux.HandleFunc("GET /readyz", service.ReadinessHandler)
	mux.Handle("GET /metrics", service.Metrics.Handler())
	mux.HandleFunc("GET /api/cache-config", service.CacheConfigHandler)
	mux.HandleFunc("GET /api/cache-stats", service.CacheStatsHandler)

	mux.HandleFunc("POST /api/pending_closures", service.RequireScope(oidc.ScopeWrite, service.CreatePendingClosureHandler))
	mux.HandleFunc("DELETE /api/pending_closures", service.RequireScope(oidc.ScopeAdmin, service.CleanupPendingClosuresHandler))
	mux.HandleFunc("POST /api/pending_closures/{id}/sign", service.RequireScope(oidc.ScopeWrite, service.SignNarinfosHandler))
	mux.HandleFunc("POST /api/pending_closures/{id}/complete", service.RequireScope(oidc.ScopeWrite, service.CommitPendingClosureHandler))
	mux.HandleFunc("POST /api/multipart/complete", service.RequireScope(oidc.ScopeWrite, service.CompleteMultipartUploadHandler))
	mux.HandleFunc("POST /api/uploads/complete", service.RequireScope(oidc.ScopeWrite, service.CompleteUploadHandler))
	mux.HandleFunc("POST /api/uploads/skipped", service.RequireScope(oidc.ScopeWrite, service.SkippedUploadsHandler))
	mux.HandleFunc("POST /api/multipart/request-parts", service.RequireScope(oidc.ScopeWrite, service.RequestMorePartsHandler))
	mux.HandleFunc("HEAD /api/objects/{key...}", service.RequireScope(oidc.ScopeWrite, service.ObjectExistsHandler))
	mux.HandleFunc("GET /api/closures/{key}", service.RequireScope(oidc.ScopeWrite, service.GetClosureHandler))
	mux.HandleFunc("DELETE /api/closures", service.RequireScope(oidc.ScopeAdmin, service.CleanupClosuresOlder))
	mux.HandleFunc("GET /api/gc/status", service.RequireScope(oidc.ScopeAdmin, service.GCStatusHandler))
	mux.HandleFunc("GET /api/pins", service.RequireScope(oidc.ScopeWrite, service.ListPinsHandler))
	mux.HandleFunc("POST /api/pins/{name}", service.RequireScope(oidc.ScopeWrite, service.CreatePinHandler))
	mux.HandleFunc("DELETE /api/pins/{name}", service.RequireScope(oidc.ScopeAdmin, service.DeletePinHandler))

	if opts.EnableReadProxy {
		service.EnableReadProxy = true
		service.ReadRedirectTTL = opts.ReadRedirectTTL
		// Register without method prefix to avoid ServeMux conflicts with
		// auto-generated HEAD routes. The handler rejects non-GET/HEAD itself.
		mux.HandleFunc("/{path...}", service.RequireScope(oidc.ScopeRead, service.ReadProxyHandler))
		slog.Info("Read proxy enabled — serving cache objects from S3")

		if opts.ReadRedirectTTL > 0 {
			slog.Info("NAR reads redirect to presigned S3 URLs", "ttl", opts.ReadRedirectTTL)
		}
	} else {
		mux.HandleFunc("GET /", service.RootRedirectHandler)
	}

	server := &http.Server{
		Addr:    opts.HTTPAddr,
		Handler: service.Metrics.Instrument(mux),
		// Bound slowloris on API endpoints. Bodies are JSON (<128 MB) and
		// responses are small. The read proxy extends its own write deadline
		// per-request via ProxyWriteTimeout for large NAR streams.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	useTLS := opts.TLSCert != ""
	if useTLS {
		tlsCfg, err := serverTLSConfig(opts.TLSClientCA)
		if err != nil {
			return err
		}

		// Load the keypair up front so a missing/invalid cert fails startup
		// before we signal readiness, rather than inside ServeTLS afterwards.
		cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
		if err != nil {
			return fmt.Errorf("loading TLS keypair: %w", err)
		}

		tlsCfg.Certificates = []tls.Certificate{cert}
		server.TLSConfig = tlsCfg
		service.NativeMTLS = opts.TLSClientCA != ""
	}

	// DB pool ping is the watchdog liveness signal.
	return serveWithGracefulShutdown(server, opts, useTLS, service.Pool.Ping)
}

// serveWithGracefulShutdown runs the HTTP server until a SIGINT/SIGTERM
// arrives, then drains in-flight requests within shutdownTimeout.
func serveWithGracefulShutdown(server *http.Server, opts *options, useTLS bool, healthCheck func(context.Context) error) error {
	ln, err := makeListener(opts)
	if err != nil {
		return err
	}

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return serve(sigCtx, server, ln, useTLS, healthCheck)
}

// serve runs the HTTP server on ln until shutdownCtx is done, then drains
// in-flight requests within shutdownTimeout. Split out from
// serveWithGracefulShutdown so tests can supply their own listener and drive
// shutdown without sending real signals.
func serve(shutdownCtx context.Context, server *http.Server, ln net.Listener, useTLS bool, healthCheck func(context.Context) error) error {
	serveErr := make(chan error, 1)

	go func() {
		serveErr <- serveListener(server, ln, useTLS)
	}()

	// Listener is bound, so we are ready to accept connections.
	notifySystemd("READY=1")

	if interval := watchdogInterval(); interval > 0 && healthCheck != nil {
		slog.Info("systemd watchdog enabled", "interval", interval)

		wdCtx, wdCancel := context.WithCancel(shutdownCtx)
		defer wdCancel()

		go runWatchdog(wdCtx, interval, healthCheck)
	}

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("failed to start server: %w", err)
		}

		return nil
	case <-shutdownCtx.Done():
		slog.Info("Shutdown signal received, draining in-flight requests", "timeout", shutdownTimeout)
		notifySystemd("STOPPING=1")
	}

	//nolint:contextcheck // drain context is intentionally independent of the shutdown signal
	return drain(server)
}

// makeListener returns a socket-activated listener when systemd passed one,
// otherwise it binds the configured HTTP address.
func makeListener(opts *options) (net.Listener, error) {
	sdListener, err := systemdListener()
	if err != nil {
		return nil, err
	}

	if sdListener != nil {
		slog.Info("Using socket-activated listener", "address", sdListener.Addr())

		return sdListener, nil
	}

	ln, err := net.Listen("tcp", opts.HTTPAddr) //nolint:noctx // listener lives for the whole server lifetime
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", opts.HTTPAddr, err)
	}

	return ln, nil
}

func serveListener(server *http.Server, ln net.Listener, useTLS bool) error {
	if useTLS {
		slog.Info("Starting HTTPS server", "address", ln.Addr(), "mtls", server.TLSConfig.ClientAuth != 0)

		// Certificates were loaded into TLSConfig before readiness was signalled.
		return server.ServeTLS(ln, "", "") //nolint:wrapcheck // classified by caller (ErrServerClosed vs real failure)
	}

	slog.Info("Starting HTTP server", "address", ln.Addr())

	return server.Serve(ln) //nolint:wrapcheck // classified by caller (ErrServerClosed vs real failure)
}

// drain gives in-flight requests up to shutdownTimeout to finish, then
// force-closes anything left so the process exits promptly. The drain context
// is detached on purpose so it survives the cancelled shutdown signal.
func drain(server *http.Server) error {
	drainCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(drainCtx); err == nil {
		return nil
	}

	slog.Warn("Graceful shutdown timed out, forcing close")

	if err := server.Close(); err != nil {
		return fmt.Errorf("failed to force-close server: %w", err)
	}

	return nil
}

// defaultCachePriority sorts before cache.nixos.org (40).
const defaultCachePriority = 30

// InitializeBucket uploads nix-cache-info and the landing page. Both are
// rewritten on every start so configuration changes take effect.
func (s *Service) InitializeBucket(ctx context.Context) error {
	storeDir := os.Getenv("NIX_STORE_DIR")
	if storeDir == "" {
		storeDir = "/nix/store"
	}

	priority := s.CachePriority
	if priority == 0 {
		priority = defaultCachePriority
	}

	cacheInfo := fmt.Sprintf("StoreDir: %s\nWantMassQuery: 1\nPriority: %d\n", storeDir, priority)

	if err := s.S3RateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("uploading nix-cache-info: %w", err)
	}

	_, err := s.MinioClient.PutObject(ctx, s.Bucket, "nix-cache-info",
		strings.NewReader(cacheInfo), int64(len(cacheInfo)),
		minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		if isRateLimitError(err) {
			s.S3RateLimiter.RecordThrottle()
		}

		return fmt.Errorf("uploading nix-cache-info: %w", err)
	}

	s.S3RateLimiter.RecordSuccess()

	if s.CacheURL != "" {
		s.uploadLandingPage(ctx)
	}

	return nil
}

// logAuthFailure logs detailed information about an authentication failure.
func (s *Service) logAuthFailure(token string, oidcErr *oidc.ValidationError) {
	// Truncate token for logging (show first and last 10 chars)
	tokenPreview := token
	if len(token) > 25 {
		tokenPreview = token[:10] + "..." + token[len(token)-10:]
	}

	switch {
	case oidcErr != nil:
		// Log OIDC-specific failure details
		slog.Warn(
			"Authentication failed",
			"token_preview", tokenPreview,
			"token_length", len(token),
			"oidc_error", oidcErr.Reason,
			"oidc_provider", oidcErr.Provider,
			"tried_providers", oidcErr.TriedProviders,
		)
		// Log claims if available (helps debug bound_claims/bound_subject mismatches)
		if oidcErr.Claims != nil {
			slog.Debug("OIDC token claims", "claims", oidcErr.Claims)
		}
	case s.OIDCValidator != nil:
		// OIDC configured but we didn't get a ValidationError (shouldn't happen normally)
		slog.Warn(
			"Authentication failed",
			"token_preview", tokenPreview,
			"token_length", len(token),
			"reason", "token did not match OIDC or static API token",
		)
	default:
		// No OIDC configured, just static token mismatch
		slog.Warn(
			"Authentication failed",
			"token_preview", tokenPreview,
			"token_length", len(token),
			"reason", "static API token mismatch",
		)
	}
}

// uploadLandingPage generates and uploads the landing page to S3.
func (s *Service) uploadLandingPage(ctx context.Context) {
	landingHTML, err := s.GenerateLandingPage(s.CacheURL)
	if err != nil {
		slog.Warn("Failed to generate landing page", "error", err)

		return
	}

	// Wait for rate limiter
	if err := s.S3RateLimiter.Wait(ctx); err != nil {
		slog.Warn("Rate limiter context canceled for landing page upload", "error", err)

		return
	}

	_, err = s.MinioClient.PutObject(ctx, s.Bucket, "index.html",
		bytes.NewReader([]byte(landingHTML)), int64(len(landingHTML)),
		minio.PutObjectOptions{ContentType: "text/html; charset=utf-8"})
	if err != nil {
		if isRateLimitError(err) {
			s.S3RateLimiter.RecordThrottle()
		}

		slog.Warn("Failed to upload landing page", "error", err)

		return
	}

	s.S3RateLimiter.RecordSuccess()
	slog.Info("Uploaded landing page to bucket", "bucket", s.Bucket)
}
