package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Mic92/niks3/pg"
	"github.com/jackc/pgx/v5/pgxpool"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Options struct {
	DBConnectionString string
	HTTPAddr           string

	// TODO: Document how to use this with AWS.
	S3Endpoint   string
	S3AccessKey  string
	S3SecretKey  string
	S3UseSSL     bool
	S3BucketName string
}

type Server struct {
	pool        *pgxpool.Pool
	minioClient *minio.Client
	bucketName  string
}

const (
	dbConnectionTimeout = 10 * time.Second
)

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

	service := &Server{pool: pool, minioClient: minioClient, bucketName: opts.S3BucketName}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", service.healthCheckHandler)

	mux.HandleFunc("POST /api/pending_closures", service.createPendingClosureHandler)
	mux.HandleFunc("DELETE /api/pending_closures", service.cleanupPendingClosuresHandler)
	mux.HandleFunc("POST /api/pending_closures/{id}/complete", service.commitPendingClosureHandler)
	mux.HandleFunc("GET /api/closures/{key}", service.getClosureHandler)
	mux.HandleFunc("DELETE /api/closures", service.cleanupClosuresOlder)

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

func (s *Server) Close() {
	s.pool.Close()
}
