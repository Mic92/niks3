package server

import (
	"bytes"
	"context"
	"crypto/subtle"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
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

	S3Endpoint    string
	S3AccessKey   string
	S3SecretKey   string
	S3UseSSL      bool
	S3UseIAM      bool
	S3Bucket      string
	S3Concurrency int
	S3RateLimit   float64

	APIToken string

	SignKeyPaths    []string
	CacheURL        string
	OIDCConfigPath  string
	EnableReadProxy bool

	Debug bool
}

type Service struct {
	Pool            *pgxpool.Pool
	MinioClient     *minio.Client
	Bucket          string
	S3Concurrency   int
	S3RateLimiter   *ratelimit.AdaptiveRateLimiter
	APIToken        string
	SigningKeys     []*signing.Key
	CacheURL        string
	OIDCValidator   *oidc.Validator
	EnableReadProxy bool
}

// Close closes the database connection pool.
func (s *Service) Close() {
	s.Pool.Close()
}

const (
	dbConnectionTimeout = 10 * time.Second
)

func (s *Service) AuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)

			return
		}

		const bearerPrefix = "Bearer "
		if !strings.HasPrefix(authHeader, bearerPrefix) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)

			return
		}

		token := strings.TrimPrefix(authHeader, bearerPrefix)

		// Try static API token first (faster, no network calls)
		if s.APIToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.APIToken)) == 1 {
			next.ServeHTTP(w, r)

			return
		}

		// Fall back to OIDC validation if configured
		var oidcErr *oidc.ValidationError
		if s.OIDCValidator != nil {
			claims, err := s.OIDCValidator.ValidateToken(r.Context(), token)
			if err == nil {
				slog.Info("OIDC auth successful", "provider", claims.Provider)
				slog.Debug("OIDC auth details", "subject", claims.Subject)
				next.ServeHTTP(w, r)

				return
			}
			// Store the OIDC error for later logging
			if validationErr, ok := err.(*oidc.ValidationError); ok {
				oidcErr = validationErr
			}
			slog.Debug("OIDC validation failed")
		}

		// Both static token and OIDC failed - log details for debugging
		s.logAuthFailure(token, oidcErr)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	}
}

// logAuthFailure logs detailed information about an authentication failure.
func (s *Service) logAuthFailure(token string, oidcErr *oidc.ValidationError) {
	// Truncate token for logging (show first and last 10 chars)
	tokenPreview := token
	if len(token) > 25 {
		tokenPreview = token[:10] + "..." + token[len(token)-10:]
	}

	if oidcErr != nil {
		// Log OIDC-specific failure details
		slog.Warn("Authentication failed",
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
	} else if s.OIDCValidator != nil {
		// OIDC configured but we didn't get a ValidationError (shouldn't happen normally)
		slog.Warn("Authentication failed",
			"token_preview", tokenPreview,
			"token_length", len(token),
			"reason", "token did not match OIDC or static API token",
		)
	} else {
		// No OIDC configured, just static token mismatch
		slog.Warn("Authentication failed",
			"token_preview", tokenPreview,
			"token_length", len(token),
			"reason", "static API token mismatch",
		)
	}
}

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
		Creds:  creds,
		Secure: opts.S3UseSSL,
	})
	if err != nil {
		return fmt.Errorf("failed to create minio s3 client: %w", err)
	}

	service := &Service{
		Pool:          pool,
		MinioClient:   minioClient,
		Bucket:        opts.S3Bucket,
		S3Concurrency: opts.S3Concurrency,
		S3RateLimiter: ratelimit.NewAdaptiveRateLimiter(opts.S3RateLimit, "s3"),
		APIToken:      opts.APIToken,
		CacheURL:      opts.CacheURL,
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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", service.HealthCheckHandler)

	mux.HandleFunc("POST /api/pending_closures", service.AuthMiddleware(service.CreatePendingClosureHandler))
	mux.HandleFunc("DELETE /api/pending_closures", service.AuthMiddleware(service.CleanupPendingClosuresHandler))
	mux.HandleFunc("POST /api/pending_closures/{id}/sign", service.AuthMiddleware(service.SignNarinfosHandler))
	mux.HandleFunc("POST /api/pending_closures/{id}/complete", service.AuthMiddleware(service.CommitPendingClosureHandler))
	mux.HandleFunc("POST /api/multipart/complete", service.AuthMiddleware(service.CompleteMultipartUploadHandler))
	mux.HandleFunc("POST /api/multipart/request-parts", service.AuthMiddleware(service.RequestMorePartsHandler))
	mux.HandleFunc("GET /api/closures/{key}", service.AuthMiddleware(service.GetClosureHandler))
	mux.HandleFunc("DELETE /api/closures", service.AuthMiddleware(service.CleanupClosuresOlder))

	if opts.EnableReadProxy {
		service.EnableReadProxy = true
		// Register without method prefix to avoid ServeMux conflicts with
		// auto-generated HEAD routes. The handler rejects non-GET/HEAD itself.
		mux.HandleFunc("/{path...}", service.ReadProxyHandler)
		slog.Info("Read proxy enabled — serving cache objects from S3")
	} else {
		mux.HandleFunc("GET /", service.RootRedirectHandler)
	}

	server := &http.Server{
		Addr:              opts.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 1 * time.Second,
	}

	slog.Info("Starting HTTP server", "address", opts.HTTPAddr)

	if err = server.ListenAndServe(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	return nil
}

// InitializeBucket ensures the bucket has the required nix-cache-info file.
func (s *Service) InitializeBucket(ctx context.Context) error {
	// Wait for rate limiter
	if err := s.S3RateLimiter.Wait(ctx); err != nil {
		return err
	}

	// Check if nix-cache-info already exists
	_, err := s.MinioClient.StatObject(ctx, s.Bucket, "nix-cache-info", minio.StatObjectOptions{})
	if err != nil {
		if isRateLimitError(err) {
			s.S3RateLimiter.RecordThrottle()
		}

		// Check if this is a "not found" error vs other errors
		errResp := minio.ToErrorResponse(err)
		if errResp.Code != minio.NoSuchKey {
			// This is not a "not found" error - could be network, permissions, etc.
			return fmt.Errorf("failed to stat nix-cache-info object: %w", err)
		}

		// Object doesn't exist, create it
		// Priority 30 is higher than the default nixos.org cache (priority 40)
		// Use NIX_STORE_DIR from environment if set, otherwise default to /nix/store
		storeDir := os.Getenv("NIX_STORE_DIR")
		if storeDir == "" {
			storeDir = "/nix/store"
		}

		cacheInfo := fmt.Sprintf(`StoreDir: %s
WantMassQuery: 1
Priority: 30
`, storeDir)

		// Wait for rate limiter before PutObject
		if err := s.S3RateLimiter.Wait(ctx); err != nil {
			return err
		}

		// Upload nix-cache-info to the bucket
		_, err = s.MinioClient.PutObject(ctx, s.Bucket, "nix-cache-info",
			bytes.NewReader([]byte(cacheInfo)), int64(len(cacheInfo)),
			minio.PutObjectOptions{ContentType: "text/plain"})
		if err != nil {
			if isRateLimitError(err) {
				s.S3RateLimiter.RecordThrottle()
			}

			return fmt.Errorf("failed to create nix-cache-info: %w", err)
		}

		s.S3RateLimiter.RecordSuccess()
		slog.Info("Created nix-cache-info in bucket", "bucket", s.Bucket)
	} else {
		s.S3RateLimiter.RecordSuccess()
	}

	// Generate and upload landing page if we have a cache URL
	// This runs on every startup to keep the landing page up-to-date with current signing keys
	if s.CacheURL != "" {
		s.uploadLandingPage(ctx)
	}

	return nil
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
