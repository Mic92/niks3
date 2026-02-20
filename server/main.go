package server

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

func getEnvOrDefault(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}

	return defaultValue
}

func getEnvOrDefaultInt(key string, defaultValue int) int {
	if value, ok := os.LookupEnv(key); ok {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}

	return defaultValue
}

func getEnvOrDefaultFloat(key string, defaultValue float64) float64 {
	if value, ok := os.LookupEnv(key); ok {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
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
	minAPITokenLength    = 36
	defaultS3Concurrency = 100
)

// stringSliceFlag implements flag.Value for repeatable string flags.
type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	return strings.Join(*s, ", ")
}

func (s *stringSliceFlag) Set(value string) error {
	*s = append(*s, value)

	return nil
}

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
	flag.BoolVar(&opts.S3UseIAM, "s3-use-iam", getEnvOrDefault("NIKS3_S3_USE_IAM", "false") == "true",
		"Use IAM credentials from the environment (IRSA, EC2 instance profile, etc.) instead of static keys")
	flag.StringVar(&opts.S3Bucket, "s3-bucket", getEnvOrDefault("NIKS3_S3_BUCKET", ""), "S3 bucket name")
	flag.StringVar(&s3AccessKeyPath, "s3-access-key-path", getEnvOrDefault("NIKS3_S3_ACCESS_KEY_PATH", ""),
		"Path to file containing S3 access key")
	flag.StringVar(&s3SecretKeyPath, "s3-secret-key-path", getEnvOrDefault("NIKS3_S3_SECRET_KEY_PATH", ""),
		"Path to file containing S3 secret key")
	flag.StringVar(&opts.APIToken, "api-token", getEnvOrDefault("NIKS3_API_TOKEN", ""), "API token for authentication")
	flag.StringVar(&apiTokenPath, "api-token-path", getEnvOrDefault("NIKS3_API_TOKEN_PATH", ""), "API token file path")
	flag.StringVar(&opts.CacheURL, "cache-url", getEnvOrDefault("NIKS3_CACHE_URL", ""),
		"Public cache URL for the landing page (e.g., https://cache.example.com)")
	flag.StringVar(&opts.OIDCConfigPath, "oidc-config", getEnvOrDefault("NIKS3_OIDC_CONFIG", ""),
		"Path to OIDC configuration file (JSON format)")
	flag.IntVar(&opts.S3Concurrency, "s3-concurrency", getEnvOrDefaultInt("NIKS3_S3_CONCURRENCY", defaultS3Concurrency),
		"Maximum concurrent S3 operations (default: 100)")
	flag.Float64Var(&opts.S3RateLimit, "s3-rate-limit", getEnvOrDefaultFloat("NIKS3_S3_RATE_LIMIT", 0),
		"Initial S3 requests per second (0 = unlimited, adapts on 429)")
	flag.BoolVar(&opts.EnableReadProxy, "enable-read-proxy",
		getEnvOrDefault("NIKS3_ENABLE_READ_PROXY", "false") == "true",
		"Serve cache objects by proxying reads from S3 (for private buckets)")
	flag.BoolVar(&opts.Debug, "debug", getEnvOrDefault("NIKS3_DEBUG", "false") == "true",
		"Enable debug logging (may leak sensitive information)")

	if opts.Debug {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})))
	}

	// Parse signing key paths from environment variable (space-separated for backward compatibility)
	signKeyPaths := (*stringSliceFlag)(&opts.SignKeyPaths)

	if envPaths := getEnvOrDefault("NIKS3_SIGN_KEY_PATHS", ""); envPaths != "" {
		for path := range strings.FieldsSeq(envPaths) {
			if err := signKeyPaths.Set(path); err != nil {
				return nil, fmt.Errorf("failed to parse NIKS3_SIGN_KEY_PATHS: %w", err)
			}
		}
	}

	flag.Var(signKeyPaths, "sign-key-path", "Path to signing key file (can be specified multiple times)")
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

	hasStaticKeys := opts.S3AccessKey != "" || opts.S3SecretKey != ""
	if opts.S3UseIAM && hasStaticKeys {
		return nil, errors.New("--s3-use-iam cannot be combined with --s3-access-key / --s3-secret-key")
	}

	if !opts.S3UseIAM {
		if opts.S3AccessKey == "" {
			return nil, errors.New("missing required flag: --s3-access-key or --s3-access-key-path (or use --s3-use-iam)")
		}

		if opts.S3SecretKey == "" {
			return nil, errors.New("missing required flag: --s3-secret-key or --s3-secret-key-path (or use --s3-use-iam)")
		}
	}

	if opts.S3Bucket == "" {
		return nil, errors.New("missing required flag: --s3-bucket")
	}

	// API token is always required (for GC and admin operations)
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
