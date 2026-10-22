package oidclogin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ErrNotFound means a secure store has no matching credential.
var ErrNotFound = errors.New("OIDC credential not found; run niks3 auth login")

// CredentialStore atomically replaces a whole secret record. Callers hold a
// per-server process lock during Load, Save and Delete, including refresh.
type CredentialStore interface {
	Load(ctx context.Context, key string) ([]byte, error)
	Save(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

// Profile contains only public connection metadata. A successful explicit
// login makes it the fallback profile for this exact canonical server URL.
type Profile struct {
	Server string `json:"server"`
	Config Config `json:"config"`
}

// Manager coordinates profiles, secure storage, and cross-process refresh.
// Dir is a private directory for public profiles and lock files, never tokens.
type Manager struct {
	Dir   string
	Store CredentialStore
}

// DefaultManager uses the XDG config directory and the supported OS keyring.
func DefaultManager() (*Manager, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return nil, fmt.Errorf("finding OIDC config directory: %w", err)
	}

	return &Manager{Dir: filepath.Join(dir, "niks3", "oidc"), Store: systemStore{}}, nil
}

// CanonicalServer scopes credentials to HTTPS server URLs, including any path.
// Query strings, fragments and embedded user credentials are not supported.
func CanonicalServer(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return "", errors.New("OIDC server URL must be HTTPS without credentials, query or fragment")
	}
	u.Host = strings.ToLower(u.Host)
	if u.Port() == "443" {
		u.Host = strings.TrimSuffix(u.Host, ":443")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = strings.TrimRight(u.RawPath, "/")

	return u.String(), nil
}

func digest(raw string) string {
	sum := sha256.Sum256([]byte(raw))

	return hex.EncodeToString(sum[:])
}

// HasProfile checks public metadata only; it never contacts the keyring or IdP.
func (m *Manager) HasProfile(raw string) (bool, error) {
	server, err := CanonicalServer(raw)
	if err != nil {
		return false, err
	}
	_, err = m.load(server)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}

	return err == nil, err
}

// Login explicitly authorizes a public client and saves its verified tokens.
// Login, logout, status and refresh share the same per-server lock.
func (m *Manager) Login(ctx context.Context, raw string, cfg Config, instructions io.Writer) error {
	server, err := CanonicalServer(raw)
	if err != nil {
		return err
	}
	lock, err := m.lock(ctx, server)
	if err != nil {
		return err
	}
	defer lock.Close()
	old, err := m.load(server)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if old != nil && (old.Config.Issuer != cfg.Issuer || old.Config.ClientID != cfg.ClientID) {
		return errors.New("a different OIDC profile exists for this server; log out before changing issuer or client ID")
	}
	p := &Profile{Server: server, Config: cfg}
	// Fail before asking the user to authorize if their secure store cannot
	// be accessed. A missing record is expected for a first login.
	if _, err := m.Store.Load(ctx, p.key()); err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("checking OIDC credential store: %w", err)
	}
	flow, err := New(ctx, cfg)
	if err != nil {
		return err
	}
	c, err := flow.Login(ctx, instructions)
	if err != nil {
		return err
	}
	if err := m.save(ctx, p, c); err != nil {
		return err
	}

	return m.saveProfile(p)
}

// Status contains no tokens. It reports cached expiry without contacting the IdP
// or refreshing. A login with refresh credentials may still be usable after the
// cached ID token expires; actual renewal is tested by the next operation.
type Status struct {
	Profile     Profile   `json:"profile"`
	ExpiresAt   time.Time `json:"expires_at"`
	Refreshable bool      `json:"refreshable"`
}

// Status returns local session information without interactive authentication.
func (m *Manager) Status(ctx context.Context, raw string) (*Status, error) {
	server, err := CanonicalServer(raw)
	if err != nil {
		return nil, err
	}
	lock, err := m.lock(ctx, server)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	p, err := m.load(server)
	if err != nil {
		return nil, err
	}
	c, err := m.credential(ctx, p)
	if err != nil {
		return nil, err
	}

	return &Status{Profile: *p, ExpiresAt: c.ExpiresAt, Refreshable: c.RefreshToken != "" && !c.MissingIDToken}, nil
}

// Logout deletes local credentials, not the provider's authorization grant.
// It leaves the lock file in place so concurrent processes use the same inode.
func (m *Manager) Logout(ctx context.Context, raw string) error {
	server, err := CanonicalServer(raw)
	if err != nil {
		return err
	}
	lock, err := m.lock(ctx, server)
	if err != nil {
		return err
	}
	defer lock.Close()
	p, err := m.load(server)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := m.Store.Delete(ctx, p.key()); err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("deleting OIDC credentials: %w", err)
	}
	if err := os.Remove(m.path(server)); err != nil {
		return fmt.Errorf("removing OIDC profile: %w", err)
	}

	return nil
}

func (p Profile) key() string {
	return digest(p.Server + "\x00" + p.Config.Issuer + "\x00" + p.Config.ClientID)
}

// TokenSource reloads secure state under the process lock on every call, so
// refresh rotation, login and logout by other processes are observed. It never
// initiates interactive login and never saves an OAuth access token.
func (m *Manager) TokenSource(raw string) func(context.Context) (string, error) {
	var flow *Flow
	var identity string

	return func(ctx context.Context) (string, error) {
		server, err := CanonicalServer(raw)
		if err != nil {
			return "", err
		}
		lock, err := m.lock(ctx, server)
		if err != nil {
			return "", err
		}
		defer lock.Close()
		p, err := m.load(server)
		if err != nil {
			return "", err
		}
		c, err := m.credential(ctx, p)
		if err != nil {
			return "", err
		}
		if flow == nil || identity != p.key() {
			flow, err = New(ctx, p.Config)
			if err != nil {
				return "", err
			}
			identity = p.key()
		}
		next, tokenErr := flow.Token(ctx, c)
		if next != nil && next != c {
			if err := m.save(ctx, p, next); err != nil {
				return "", err
			}
		}
		if tokenErr != nil {
			return "", tokenErr
		}

		return next.IDToken, nil
	}
}

func (m *Manager) path(server string) string { return filepath.Join(m.Dir, digest(server)+".json") }

func (m *Manager) lock(ctx context.Context, server string) (io.Closer, error) {
	if err := os.MkdirAll(m.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("creating OIDC profile directory: %w", err)
	}

	return lockProfile(ctx, filepath.Join(m.Dir, digest(server)+".lock"))
}

func (m *Manager) load(server string) (*Profile, error) {
	data, err := os.ReadFile(m.path(server))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("reading OIDC profile: %w", err)
	}
	var p Profile
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, errors.New("invalid OIDC profile; log out and log in again")
	}
	if p.Server != server || p.Config.ClientID == "" {
		return nil, errors.New("OIDC profile identity mismatch")
	}
	if err := secureURL(p.Config.Issuer, true); err != nil {
		return nil, err
	}

	return &p, nil
}

func (m *Manager) saveProfile(p *Profile) error {
	data, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("encoding OIDC profile: %w", err)
	}
	tmp, err := os.CreateTemp(m.Dir, ".profile-*")
	if err != nil {
		return fmt.Errorf("creating OIDC profile: %w", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()

		return fmt.Errorf("writing OIDC profile: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing OIDC profile: %w", err)
	}
	if err := os.Rename(tmp.Name(), m.path(p.Server)); err != nil {
		return fmt.Errorf("replacing OIDC profile: %w", err)
	}

	return nil
}

func (m *Manager) save(ctx context.Context, p *Profile, c *Credential) error {
	data, err := json.Marshal(c) //nolint:gosec // G117: secret record is sent only to CredentialStore, never a file or log
	if err != nil {
		return errors.New("encoding OIDC credentials failed")
	}

	if err := m.Store.Save(ctx, p.key(), data); err != nil {
		return fmt.Errorf("saving OIDC credentials: %w", err)
	}

	return nil
}

func (m *Manager) credential(ctx context.Context, p *Profile) (*Credential, error) {
	data, err := m.Store.Load(ctx, p.key())
	if err != nil {
		return nil, fmt.Errorf("loading OIDC credentials: %w", err)
	}
	var c Credential
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, errors.New("invalid stored OIDC credentials; log in again")
	}

	return &c, nil
}
