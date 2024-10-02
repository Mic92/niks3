package main

import (
	"flag"
	"log"
	"os"

	"github.com/Mic92/nix-s3-gc/api"
)

func getEnvOrDefault(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

func parseArgs() *api.Options {
	var opts api.Options
	flag.StringVar(&opts.DBPath, "db", getEnvOrDefault("NIX_S3_GC_DB", "nix-s3-gc.db"), "Path to the database file")
	flag.StringVar(&opts.HTTPAddr, "http-addr", getEnvOrDefault("NIX_S3_GC_HTTP_ADDR", ":5751"), "HTTP address to listen on")
	flag.Parse()
	return &opts
}

func main() {
	opts := parseArgs()
	if err := api.RunServer(opts); err != nil {
		log.Fatalf("Failed to run gc service: %v", err)
	}
	api.RunServer(opts)
}
