package server

import (
	"context"
	"crypto/subtle"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Mic92/niks3/server/pg"
	"github.com/jackc/pgx/v5/pgxpool"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Options struct {
	DBConnectionString string
	HTTPAddr           string

	// TODO: Document how to use this with AWS.
	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3UseSSL    bool
	S3Bucket    string

	APIToken string
}

type Service struct {
	Pool        *pgxpool.Pool
	MinioClient *minio.Client
	Bucket      string
	APIToken    string
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

		bearerPrefix := "Bearer "
		if !strings.HasPrefix(authToken, bearerPrefix) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)

			return
		}

		authToken = authToken[len(bearerPrefix):]
		if subtle.ConstantTimeCompare([]byte(authToken), []byte(s.APIToken)) != 1 {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)

			return
		}

		next.ServeHTTP(w, r)
	}
}

func RunServer(opts *Options) error {
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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", service.HealthCheckHandler)

	mux.HandleFunc("POST /api/pending_closures", service.AuthMiddleware(service.CreatePendingClosureHandler))
	mux.HandleFunc("DELETE /api/pending_closures", service.AuthMiddleware(service.CleanupPendingClosuresHandler))
	mux.HandleFunc("POST /api/pending_closures/{id}/complete", service.AuthMiddleware(service.CommitPendingClosureHandler))
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

func (s *Service) Close() {
	s.Pool.Close()
}
