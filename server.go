package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Mic92/niks3/pg"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Options struct {
	DBConnectionString string
	HTTPAddr           string
	MigrateDB          bool
}

type Server struct {
	pool *pgxpool.Pool
}

func RunServer(opts *Options) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pg.Connect(ctx, opts.DBConnectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	service := &Server{pool: pool}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", service.healthCheckHandler)
	mux.HandleFunc("/uploads", service.startUploadHandler)
	mux.HandleFunc("/uploads/{upload_id}/complete", service.completeUploadHandler)

	server := &http.Server{
		Addr:    opts.HTTPAddr,
		Handler: mux,
	}

	slog.Info("Starting HTTP server", "address", opts.HTTPAddr)
	return server.ListenAndServe()
}

func (s *Server) Close() {
	s.pool.Close()
}
