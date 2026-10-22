package cmdutil

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Mic92/niks3/client"
	"github.com/Mic92/niks3/client/oidclogin"
)

// OIDCFlags are opt-in CLI flags. Hooks do not register these flags and retain
// the existing credential resolution without inspecting saved OIDC profiles.
type OIDCFlags struct {
	Device   *bool
	Profile  *bool
	Issuer   *string
	ClientID *string
	Scopes   *string
}

// AddOIDCFlags enables native login and saved-profile fallback for this command.
func (cf CommonFlags) AddOIDCFlags(fs *flag.FlagSet) CommonFlags {
	cf.OIDC = &OIDCFlags{
		Device:   fs.Bool("oidc-device", false, "Explicitly start ephemeral OIDC device login (no persistence)"),
		Profile:  fs.Bool("oidc-profile", false, "Use saved OIDC login ahead of default token files and mTLS"),
		Issuer:   fs.String("oidc-issuer", "", "OIDC issuer URL for device login"),
		ClientID: fs.String("oidc-client-id", "", "Public OIDC client ID for device login"),
		Scopes:   fs.String("oidc-scopes", "openid profile offline_access", "Space-separated OIDC scopes (must include openid)"),
	}

	return cf
}

func (cf CommonFlags) resolveOIDC(hasMTLS bool) (client.TokenSource, error) {
	explicit := *cf.AuthToken != "" || *cf.AuthTokenPath != "" || *cf.AuthTokenScript != ""
	if cf.OIDC == nil {
		return ResolveTokenSource(*cf.AuthToken, *cf.AuthTokenPath, *cf.AuthTokenScript, hasMTLS)
	}
	o := cf.OIDC
	if *o.Device {
		if explicit || *o.Profile {
			return nil, errors.New("--oidc-device cannot be combined with token flags or --oidc-profile")
		}
		if *o.Issuer == "" || *o.ClientID == "" {
			return nil, errors.New("--oidc-device requires --oidc-issuer and --oidc-client-id")
		}
		if _, err := oidclogin.CanonicalServer(*cf.ServerURL); err != nil {
			return nil, fmt.Errorf("selecting OIDC credentials: %w", err)
		}

		return oidclogin.Ephemeral(oidclogin.Config{Issuer: *o.Issuer, ClientID: *o.ClientID, Scopes: strings.Fields(*o.Scopes)}, os.Stderr), nil
	}
	if *o.Issuer != "" || *o.ClientID != "" {
		return nil, errors.New("--oidc-issuer and --oidc-client-id require --oidc-device")
	}
	// Explicit token flags always win. Saved profiles are a fallback after the
	// existing env/XDG-file and mTLS sources unless explicitly selected.
	if explicit || !*o.Profile {
		ts, err := ResolveTokenSource(*cf.AuthToken, *cf.AuthTokenPath, *cf.AuthTokenScript, hasMTLS)
		if err == nil {
			return ts, nil
		}
	}
	manager, err := oidclogin.DefaultManager()
	if err != nil {
		return nil, fmt.Errorf("selecting OIDC credentials: %w", err)
	}
	exists, err := manager.HasProfile(*cf.ServerURL)
	if err != nil {
		if !*o.Profile {
			return ResolveTokenSource(*cf.AuthToken, *cf.AuthTokenPath, *cf.AuthTokenScript, hasMTLS)
		}

		return nil, fmt.Errorf("selecting OIDC credentials: %w", err)
	}
	if !exists {
		if *o.Profile {
			return nil, oidclogin.ErrNotFound
		}

		return ResolveTokenSource(*cf.AuthToken, *cf.AuthTokenPath, *cf.AuthTokenScript, hasMTLS)
	}

	return manager.TokenSource(*cf.ServerURL), nil
}

// OIDCHelp documents only flags registered by AddOIDCFlags.
const OIDCHelp = `  --oidc-device
        Start explicit device login for this command; keep tokens in memory only
  --oidc-issuer string, --oidc-client-id string
        Issuer and public client ID (required with --oidc-device)
  --oidc-scopes string
        Space-separated scopes (default: "openid profile offline_access")
  --oidc-profile
        Select the saved server login ahead of default token files and mTLS.
        Explicit token/script/file flags still win. Without this flag, a saved
        login is used only when no existing authentication method is configured`
