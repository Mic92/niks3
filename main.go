package main

import (
	"flag"
	"log"
	"os"
	"strconv"
)

func getEnvOrDefault(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

func parseArgs() *Options {
	var opts Options
	flag.StringVar(&opts.DBConnectionString, "db", getEnvOrDefault("NIX_S3_GC_DB", ""), "Postgres connection string, see https://pkg.go.dev/github.com/lib/pq#hdr-Connection_String_Parameters")
	flag.StringVar(&opts.HTTPAddr, "http-addr", getEnvOrDefault("NIX_S3_GC_HTTP_ADDR", ":5751"), "HTTP address to listen on")

	defaultMigrate, err := strconv.ParseBool(getEnvOrDefault("NIX_S3_GC_DB_MIGRATE", "true"))
	if err != nil {
		log.Fatalf("Failed to parse migrate flag: %v", err)
	}

	flag.BoolVar(&opts.MigrateDB, "db-migrate", defaultMigrate, "Run database migrations")

	flag.Parse()
	if opts.DBConnectionString == "" {
		log.Fatalf("Missing required flag: --db")
	}
	return &opts
}

func main() {
	opts := parseArgs()
	if err := RunServer(opts); err != nil {
		log.Fatalf("Failed to run gc service: %v", err)
	}
}
