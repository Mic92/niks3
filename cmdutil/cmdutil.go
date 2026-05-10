// Package cmdutil provides shared flag parsing and configuration helpers
// for niks3 CLI binaries.
package cmdutil

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/Mic92/niks3/client"
)

// SetupLogger configures the global slog logger with the specified level.
func SetupLogger(debug bool) {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}

	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(handler))
}

// DefaultAuthTokenPath returns the default XDG-compliant path for the auth token.
func DefaultAuthTokenPath() string {
	configDir := os.Getenv("XDG_CONFIG_HOME")
	if configDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}

		configDir = filepath.Join(home, ".config")
	}

	return filepath.Join(configDir, "niks3", "auth-token")
}

// GetAuthToken reads the auth token from NIKS3_AUTH_TOKEN_FILE or the default XDG path.
// The file should contain the token as a single line (trailing whitespace is trimmed).
func GetAuthToken() (string, error) {
	tokenFile := os.Getenv("NIKS3_AUTH_TOKEN_FILE")
	if tokenFile == "" {
		tokenFile = DefaultAuthTokenPath()
		if tokenFile == "" {
			return "", nil
		}

		if _, err := os.Stat(tokenFile); os.IsNotExist(err) {
			return "", nil
		}
	}

	data, err := os.ReadFile(tokenFile) //nolint:gosec // token path is user-provided config, not attacker-controlled
	if err != nil {
		return "", fmt.Errorf("reading auth token from file %q: %w", tokenFile, err)
	}

	return strings.TrimSpace(string(data)), nil
}

// ResolveAuthToken returns the token from the file at flagTokenPath if set,
// otherwise falls back to flagToken (from --auth-token or the env/XDG default).
func ResolveAuthToken(flagToken, flagTokenPath string) (string, error) {
	if flagTokenPath != "" {
		data, err := os.ReadFile(flagTokenPath)
		if err != nil {
			return "", fmt.Errorf("reading auth token file: %w", err)
		}

		return strings.TrimSpace(string(data)), nil
	}

	return flagToken, nil
}

// CommonFlags holds pointers to flags shared across subcommands.
type CommonFlags struct {
	ServerURL *string
	AuthToken *string
	Debug     *bool
	Help      *bool
}

// AddCommonFlags registers --server-url, --auth-token, --debug, and -h/--help
// on the given FlagSet and returns pointers to them.
func AddCommonFlags(fs *flag.FlagSet, defaultAuthToken string) CommonFlags {
	fs.Usage = func() {} // Suppress default usage; each command prints its own.
	cf := CommonFlags{
		ServerURL: fs.String("server-url", os.Getenv("NIKS3_SERVER_URL"), "Server URL"),
		AuthToken: fs.String("auth-token", defaultAuthToken, "Auth token"),
		Debug:     fs.Bool("debug", false, "Enable debug logging"),
		Help:      fs.Bool("help", false, "Show help"),
	}
	fs.BoolVar(cf.Help, "h", false, "Show help")

	return cf
}

// RequireServerURL returns an error if the URL is empty.
func RequireServerURL(url string) error {
	if url == "" {
		return errors.New("server URL is required (use --server-url or NIKS3_SERVER_URL env var)")
	}

	return nil
}

// RequireAuthToken returns an error if the token is empty.
func RequireAuthToken(token string) error {
	if token == "" {
		return errors.New("auth token is required (use --auth-token, --auth-token-path, NIKS3_AUTH_TOKEN_FILE, or $XDG_CONFIG_HOME/niks3/auth-token)")
	}

	return nil
}

//nolint:gosec // G101: help text, not credentials
const AuthTokenHelp = `  --auth-token string
        Auth token (default: reads from $XDG_CONFIG_HOME/niks3/auth-token or NIKS3_AUTH_TOKEN_FILE)
        WARNING: tokens passed on the command line are visible in /proc and shell history;
        prefer --auth-token-path or NIKS3_AUTH_TOKEN_FILE`

//nolint:gosec // G101: help text, not credentials
const AuthTokenPathHelp = `  --auth-token-path string
        Path to file containing the auth token (preferred over --auth-token)`

const TLSHelp = `  --client-cert string
        Client certificate file for mTLS authentication
  --client-key string
        Client private key file for mTLS authentication
  --ca-cert string
        CA certificate file for server verification (optional)`

// TLSFlags holds pointers to the mTLS-related flags shared across subcommands.
type TLSFlags struct {
	ClientCert *string
	ClientKey  *string
	CACert     *string
}

// AddTLSFlags registers --client-cert, --client-key, and --ca-cert on the
// given FlagSet and returns pointers to them.
func AddTLSFlags(fs *flag.FlagSet) TLSFlags {
	return TLSFlags{
		ClientCert: fs.String("client-cert", "", "Client certificate file for mTLS"),
		ClientKey:  fs.String("client-key", "", "Client private key file for mTLS"),
		CACert:     fs.String("ca-cert", "", "CA certificate file for server verification (optional)"),
	}
}

// Configure sets up mTLS on the client when a certificate/key pair is
// supplied. It is a no-op when neither is set and errors when only one is set.
func (tf TLSFlags) Configure(c *client.Client) error {
	certFile, keyFile, caFile := *tf.ClientCert, *tf.ClientKey, *tf.CACert

	if certFile == "" && keyFile == "" {
		return nil
	}

	if certFile == "" || keyFile == "" {
		return errors.New("both --client-cert and --client-key must be provided for mTLS")
	}

	slog.Info("Configuring client TLS", "cert", certFile, "key", keyFile, "ca", caFile)

	if err := c.SetClientTLS(certFile, keyFile, caFile); err != nil {
		return fmt.Errorf("setting up client TLS: %w", err)
	}

	return nil
}
