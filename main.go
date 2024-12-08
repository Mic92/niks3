package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
)

func getEnvOrDefault(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}

	return defaultValue
}

func parseArgs() (*Options, error) {
	var opts Options

	s3AccessKeyPath := ""
	s3SecretKeyPath := ""

	flag.StringVar(&opts.DBConnectionString, "db", getEnvOrDefault("NIKS3_DB", ""),
		"Postgres connection string, see https://pkg.go.dev/github.com/lib/pq#hdr-Connection_String_Parameters")
	flag.StringVar(&opts.HTTPAddr, "http-addr", getEnvOrDefault("NIKS3_HTTP_ADDR", ":5751"), "HTTP address to listen on")
	flag.StringVar(&opts.S3Endpoint, "s3-endpoint", getEnvOrDefault("NIKS3_S3_ENDPOINT", ""), "S3 endpoint")
	flag.StringVar(&opts.S3AccessKey, "s3-access-key", getEnvOrDefault("NIKS3_S3_ACCESS_KEY", ""), "S3 access key")
	flag.StringVar(&opts.S3SecretKey, "s3-secret-key", getEnvOrDefault("NIKS3_S3_SECRET_KEY", ""), "S3 secret key")
	flag.BoolVar(&opts.S3UseSSL, "s3-use-ssl", getEnvOrDefault("NIKS3_S3_USE_SSL", "true") == "true", "Use SSL for S3")
	flag.StringVar(&opts.S3BucketName, "s3-bucket-name", getEnvOrDefault("NIKS3_S3_BUCKET_NAME", ""), "S3 bucket name")
	flag.StringVar(&s3AccessKeyPath, "s3-access-key-path", getEnvOrDefault("NIKS3_S3_ACCESS_KEY_PATH", ""),
		"Path to file containing S3 access key")
	flag.StringVar(&s3SecretKeyPath, "s3-secret-key-path", getEnvOrDefault("NIKS3_S3_SECRET_KEY_PATH", ""),
		"Path to file containing S3 secret key")
	flag.Parse()

	if opts.DBConnectionString == "" {
		return nil, errors.New("missing required flag: --db")
	}

	if s3AccessKeyPath != "" {
		accessKey, err := os.ReadFile(s3AccessKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read S3 access key file: %w", err)
		}

		opts.S3AccessKey = string(accessKey)
	}

	if s3SecretKeyPath != "" {
		secretKey, err := os.ReadFile(s3SecretKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read S3 secret key file: %w", err)
		}

		opts.S3SecretKey = string(secretKey)
	}

	if opts.S3Endpoint == "" {
		return nil, errors.New("missing required flag: --s3-endpoint")
	}

	if opts.S3AccessKey == "" {
		return nil, errors.New("missing required flag: --s3-access-key")
	}

	if opts.S3SecretKey == "" {
		return nil, errors.New("missing required flag: --s3-secret-key")
	}

	return &opts, nil
}

func main() {
	opts, err := parseArgs()
	if err != nil {
		log.Fatalf("Failed to parse args: %v", err)
	}

	if err := RunServer(opts); err != nil {
		log.Fatalf("Failed to run gc service: %v", err)
	}
}
