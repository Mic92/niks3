package main

import (
	"fmt"
	"log/slog"
	"net/http"
)

type Options struct {
	DBConnectionString string
	HTTPAddr           string
}

type Server struct {
	db *DB
}

func RunServer(opts *Options) error {
	db, err := ConnectDB(opts.DBConnectionString)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	service := &Server{db: db}

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
	s.db.Close()
}
