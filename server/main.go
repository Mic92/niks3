package server

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

func getEnvOrDefault(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}

	return defaultValue
}

func readSecretFile(path string) (string, error) {
	if path == "" {
		return "", nil
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read file: %w", err)
	}

	return strings.TrimSpace(string(content)), nil
}

const (
	minAPITokenLength = 36
)

func parseArgs() (*options, error) {
	var opts options

	s3AccessKeyPath := ""
	s3SecretKeyPath := ""
	apiTokenPath := ""

	flag.StringVar(&opts.DBConnectionString, "db", getEnvOrDefault("NIKS3_DB", ""),
		"Postgres connection string, see https://pkg.go.dev/github.com/lib/pq#hdr-Connection_String_Parameters")
	flag.StringVar(&opts.HTTPAddr, "http-addr", getEnvOrDefault("NIKS3_HTTP_ADDR", ":5751"), "HTTP address to listen on")
	flag.StringVar(&opts.S3Endpoint, "s3-endpoint", getEnvOrDefault("NIKS3_S3_ENDPOINT", ""), "S3 endpoint")
	flag.StringVar(&opts.S3AccessKey, "s3-access-key", getEnvOrDefault("NIKS3_S3_ACCESS_KEY", ""), "S3 access key")
	flag.StringVar(&opts.S3SecretKey, "s3-secret-key", getEnvOrDefault("NIKS3_S3_SECRET_KEY", ""), "S3 secret key")
	flag.BoolVar(&opts.S3UseSSL, "s3-use-ssl", getEnvOrDefault("NIKS3_S3_USE_SSL", "true") == "true", "Use SSL for S3")
	flag.StringVar(&opts.S3Bucket, "s3-bucket", getEnvOrDefault("NIKS3_S3_BUCKET", ""), "S3 bucket name")
	flag.StringVar(&s3AccessKeyPath, "s3-access-key-path", getEnvOrDefault("NIKS3_S3_ACCESS_KEY_PATH", ""),
		"Path to file containing S3 access key")
	flag.StringVar(&s3SecretKeyPath, "s3-secret-key-path", getEnvOrDefault("NIKS3_S3_SECRET_KEY_PATH", ""),
		"Path to file containing S3 secret key")
	flag.StringVar(&opts.APIToken, "api-token", getEnvOrDefault("NIKS3_API_TOKEN", ""), "API token for authentication")
	flag.StringVar(&apiTokenPath, "api-token-path", getEnvOrDefault("NIKS3_API_TOKEN_PATH", ""), "API token file path")
	flag.Parse()

	if opts.DBConnectionString == "" {
		return nil, errors.New("missing required flag: --db")
	}

	var err error

	var secret string

	if secret, err = readSecretFile(s3AccessKeyPath); err != nil {
		return nil, fmt.Errorf("failed to read S3 access key file: %w", err)
	} else if secret != "" {
		opts.S3AccessKey = secret
	}

	if secret, err = readSecretFile(s3SecretKeyPath); err != nil {
		return nil, fmt.Errorf("failed to read S3 secret key file: %w", err)
	} else if secret != "" {
		opts.S3SecretKey = secret
	}

	if secret, err = readSecretFile(apiTokenPath); err != nil {
		return nil, fmt.Errorf("failed to read API token file: %w", err)
	} else if secret != "" {
		opts.APIToken = secret
	}

	if opts.S3Endpoint == "" {
		return nil, errors.New("missing required flag: --s3-endpoint")
	}

	if opts.S3AccessKey == "" {
		return nil, errors.New("missing required flag: --s3-access-key or --s3-access-key-path")
	}

	if opts.S3SecretKey == "" {
		return nil, errors.New("missing required flag: --s3-secret-key or --s3-secret-key-path")
	}

	if opts.S3Bucket == "" {
		return nil, errors.New("missing required flag: --s3-bucket")
	}

	if opts.APIToken == "" {
		return nil, errors.New("missing required flag: --api-token or --api-token-path")
	}

	if len(opts.APIToken) < minAPITokenLength {
		return nil, errors.New("API token must be at least 36 characters long")
	}

	return &opts, nil
}

func Main() {
	opts, err := parseArgs()
	if err != nil {
		slog.Error("Failed to parse args", "error", err)
		os.Exit(1)
	}

	if err := runServer(opts); err != nil {
		slog.Error("Failed to run server", "error", err)
		os.Exit(1)
	}
}
