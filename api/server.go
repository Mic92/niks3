package api

import (
	"fmt"
	"log/slog"
	"net/http"

	bolt "go.etcd.io/bbolt"
)

type Options struct {
	DBPath   string
	HTTPAddr string
}

type Server struct {
	db *bolt.DB
}

func RunServer(opts *Options) error {
	db, err := bolt.Open(opts.DBPath, 0o600, nil)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	service := &Server{db: db}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", service.healthCheckHandler)

	server := &http.Server{
		Addr:    opts.HTTPAddr,
		Handler: mux,
	}

	slog.Info("Starting HTTP server", "address", opts.HTTPAddr)
	return server.ListenAndServe()
}
