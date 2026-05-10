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

// envAuthTokenPath returns the token file from NIKS3_AUTH_TOKEN_FILE or the
// XDG default path, or "" if neither is set / the default doesn't exist.
func envAuthTokenPath() string {
	if p := os.Getenv("NIKS3_AUTH_TOKEN_FILE"); p != "" {
		return p
	}

	p := DefaultAuthTokenPath()
	if p == "" {
		return ""
	}

	if _, err := os.Stat(p); os.IsNotExist(err) {
		return ""
	}

	return p
}

// ResolveTokenSource picks a client.TokenSource from the auth flags, in
// priority order: script > file > literal token > env/XDG file. All file
// sources (--auth-token-path, NIKS3_AUTH_TOKEN_FILE, the XDG default) get
// the same FileToken behavior with periodic re-reads, so an external
// refresher rotating the file works regardless of how the path was supplied.
func ResolveTokenSource(flagToken, flagTokenPath, flagTokenScript string) (client.TokenSource, error) {
	switch {
	case flagTokenScript != "":
		return client.ScriptToken(flagTokenScript), nil
	case flagTokenPath != "":
		return client.FileToken(flagTokenPath), nil
	case flagToken != "":
		return client.StaticToken(flagToken), nil
	}

	if p := envAuthTokenPath(); p != "" {
		return client.FileToken(p), nil
	}

	return nil, errors.New("auth token is required (use --auth-token-path, --auth-token-script, NIKS3_AUTH_TOKEN_FILE, or $XDG_CONFIG_HOME/niks3/auth-token)")
}

// CommonFlags holds pointers to flags shared across subcommands.
type CommonFlags struct {
	ServerURL       *string
	AuthToken       *string
	AuthTokenPath   *string
	AuthTokenScript *string
	Debug           *bool
	Help            *bool
}

// AddCommonFlags registers --server-url, the auth flags, --debug, and
// -h/--help on the given FlagSet and returns pointers to them.
func AddCommonFlags(fs *flag.FlagSet) CommonFlags {
	fs.Usage = func() {} // Suppress default usage; each command prints its own.
	cf := CommonFlags{
		ServerURL:       fs.String("server-url", os.Getenv("NIKS3_SERVER_URL"), "Server URL"),
		AuthToken:       fs.String("auth-token", "", "Auth token (deprecated)"),
		AuthTokenPath:   fs.String("auth-token-path", "", "Path to auth token file"),
		AuthTokenScript: fs.String("auth-token-script", "", "Command that emits a token JSON document"),
		Debug:           fs.Bool("debug", false, "Enable debug logging"),
		Help:            fs.Bool("help", false, "Show help"),
	}
	fs.BoolVar(cf.Help, "h", false, "Show help")

	return cf
}

// TokenSource resolves the auth flags via ResolveTokenSource and warns if
// the deprecated --auth-token flag was set explicitly on the command line
// (as opposed to being filled in from NIKS3_AUTH_TOKEN_FILE or the XDG
// default). fs must be the FlagSet the flags were registered on.
func (cf CommonFlags) TokenSource(fs *flag.FlagSet) (client.TokenSource, error) {
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "auth-token" {
			slog.Warn("--auth-token is deprecated: tokens on the command line are visible in /proc and shell history; use --auth-token-path or --auth-token-script")
		}
	})

	return ResolveTokenSource(*cf.AuthToken, *cf.AuthTokenPath, *cf.AuthTokenScript)
}

// RequireServerURL returns an error if the URL is empty.
func RequireServerURL(url string) error {
	if url == "" {
		return errors.New("server URL is required (use --server-url or NIKS3_SERVER_URL env var)")
	}

	return nil
}

//nolint:gosec // G101: help text, not credentials
const AuthTokenHelp = `  --auth-token string
        DEPRECATED: tokens passed on the command line are visible in /proc and
        shell history. Use --auth-token-path, --auth-token-script, or
        NIKS3_AUTH_TOKEN_FILE instead.
        When unset, falls back to NIKS3_AUTH_TOKEN_FILE or $XDG_CONFIG_HOME/niks3/auth-token`

//nolint:gosec // G101: help text, not credentials
const AuthTokenPathHelp = `  --auth-token-path string
        Path to file containing the auth token (preferred over --auth-token).
        Re-read periodically so an external refresher can rotate it`

//nolint:gosec // G101: help text, not credentials
const AuthTokenScriptHelp = `  --auth-token-script string
        Command that prints {"token":"...","expires_at":"RFC3339"} on stdout.
        Run on first use and again before expiry. Use for short-lived OIDC tokens`

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
