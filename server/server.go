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

	"github.com/Mic92/niks3/server/pg"
	"github.com/Mic92/niks3/server/signing"
	"github.com/jackc/pgx/v5/pgxpool"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type options struct {
	DBConnectionString string
	HTTPAddr           string

	// TODO: Document how to use this with AWS.
	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3UseSSL    bool
	S3Bucket    string

	APIToken string

	SignKeyPaths []string
}

type Service struct {
	Pool        *pgxpool.Pool
	MinioClient *minio.Client
	Bucket      string
	APIToken    string
	SigningKeys []*signing.Key
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
		authToken := r.Header.Get("Authorization")
		if authToken == "" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)

			return
		}

		const bearerPrefix = "Bearer "
		if !strings.HasPrefix(authToken, bearerPrefix) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)

			return
		}

		authToken = strings.TrimPrefix(authToken, bearerPrefix)
		if subtle.ConstantTimeCompare([]byte(authToken), []byte(s.APIToken)) != 1 {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)

			return
		}

		next.ServeHTTP(w, r)
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

	minioClient, err := minio.New(opts.S3Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(opts.S3AccessKey, opts.S3SecretKey, ""),
		Secure: opts.S3UseSSL,
	})
	if err != nil {
		return fmt.Errorf("failed to create minio s3 client: %w", err)
	}

	service := &Service{Pool: pool, MinioClient: minioClient, Bucket: opts.S3Bucket, APIToken: opts.APIToken}

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
	mux.HandleFunc("POST /api/pending_closures/{id}/complete", service.AuthMiddleware(service.CommitPendingClosureHandler))
	mux.HandleFunc("POST /api/multipart/complete", service.AuthMiddleware(service.CompleteMultipartUploadHandler))
	mux.HandleFunc("GET /api/closures/{key}", service.AuthMiddleware(service.GetClosureHandler))
	mux.HandleFunc("DELETE /api/closures", service.AuthMiddleware(service.CleanupClosuresOlder))

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
	// Check if nix-cache-info already exists
	_, err := s.MinioClient.StatObject(ctx, s.Bucket, "nix-cache-info", minio.StatObjectOptions{})
	if err == nil {
		// File already exists
		return nil
	}

	// Check if this is a "not found" error vs other errors
	errResp := minio.ToErrorResponse(err)
	if errResp.Code != "NoSuchKey" {
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

	// Upload nix-cache-info to the bucket
	_, err = s.MinioClient.PutObject(ctx, s.Bucket, "nix-cache-info",
		bytes.NewReader([]byte(cacheInfo)), int64(len(cacheInfo)),
		minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		return fmt.Errorf("failed to create nix-cache-info: %w", err)
	}

	slog.Info("Created nix-cache-info in bucket", "bucket", s.Bucket)

	return nil
}
