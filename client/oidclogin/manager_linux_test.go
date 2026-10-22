package oidclogin //nolint:testpackage // exercises internal validation, clock, locking and wire protocol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fileTestStore is a test-only substitute for the keyring, inside t.TempDir.
// Production never uses filesystem storage for credentials.
type fileTestStore struct {
	dir      string
	failSave bool
}

func (s fileTestStore) Load(_ context.Context, key string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("test store read: %w", err)
	}

	return data, nil
}

func (s fileTestStore) Save(_ context.Context, key string, data []byte) error {
	if s.failSave {
		return errors.New("test store unavailable")
	}

	if err := os.WriteFile(filepath.Join(s.dir, key), data, 0o600); err != nil {
		return fmt.Errorf("test store write: %w", err)
	}

	return nil
}

func (s fileTestStore) Delete(_ context.Context, key string) error {
	if err := os.Remove(filepath.Join(s.dir, key)); err != nil {
		return fmt.Errorf("test store delete: %w", err)
	}

	return nil
}

func testManager(t *testing.T, m *testIssuer) (*Manager, *Profile) {
	t.Helper()
	dir := t.TempDir()
	storeDir := filepath.Join(dir, "secrets")
	if err := os.Mkdir(storeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{Dir: filepath.Join(dir, "profiles"), Store: fileTestStore{dir: storeDir}}
	p := &Profile{Server: "https://cache.example.com", Config: m.config()}

	return manager, p
}

func saveTestSession(t *testing.T, manager *Manager, p *Profile, c *Credential) {
	t.Helper()
	lock, err := manager.lock(t.Context(), p.Server)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := manager.save(t.Context(), p, c); err != nil {
		t.Fatal(err)
	}
	if err := manager.saveProfile(p); err != nil {
		t.Fatal(err)
	}
}

func TestStoredLoginLifecycle(t *testing.T) {
	t.Parallel()
	m := newTestIssuer(t, issuerResponse{})
	manager, p := testManager(t, m)
	if err := manager.Login(t.Context(), p.Server, p.Config, io.Discard); err != nil {
		t.Fatal(err)
	}
	source := manager.TokenSource(p.Server)
	token, err := source(t.Context())
	if err != nil || token == "" || token == "opaque-access" {
		t.Fatal("saved session not usable")
	}
	status, err := manager.Status(t.Context(), p.Server)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret-") || strings.Contains(string(data), token) {
		t.Fatal("status leaks token")
	}
	profileData, err := os.ReadFile(manager.path(p.Server))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(profileData), token) || strings.Contains(string(profileData), "secret-") {
		t.Fatal("profile contains secrets")
	}
	if _, err := manager.TokenSource("https://other.example.com")(t.Context()); !errors.Is(err, ErrNotFound) {
		t.Fatal("credential leaked to another server")
	}
	other := p.Config
	other.ClientID = "other"
	if err := manager.Login(t.Context(), p.Server, other, io.Discard); err == nil {
		t.Fatal("identity changed without logout")
	}
	if err := manager.Logout(t.Context(), p.Server); err != nil {
		t.Fatal(err)
	}
	if _, err := source(t.Context()); !errors.Is(err, ErrNotFound) {
		t.Fatal("logged out source still serves tokens")
	}
	if err := manager.Logout(t.Context(), p.Server); err != nil {
		t.Fatal(err)
	}
}

func TestStoreFailureDoesNotReturnRefreshedToken(t *testing.T) {
	t.Parallel()
	m := newTestIssuer(t, issuerResponse{})
	manager, p := testManager(t, m)
	c := initialCredential(t, testFlow(t, m), m)
	c.RefreshAt = time.Time{}
	saveTestSession(t, manager, p, c)
	store, ok := manager.Store.(fileTestStore)
	if !ok {
		t.Fatal("unexpected test store")
	}
	store.failSave = true
	manager.Store = store
	token, err := manager.TokenSource(p.Server)(t.Context())
	if err == nil || token != "" {
		t.Fatal("token returned without saving refresh rotation")
	}
}

func TestStoredNoIDRefresh(t *testing.T) {
	t.Parallel()
	m := newTestIssuer(t, issuerResponse{omitID: true})
	manager, p := testManager(t, m)
	c := initialCredential(t, testFlow(t, m), m)
	c.RefreshAt = time.Time{}
	saveTestSession(t, manager, p, c)
	for range 3 {
		// Use a fresh source each time to model separate CLI invocations.
		token, err := manager.TokenSource(p.Server)(t.Context())
		if err != nil || token != c.IDToken {
			t.Fatal("valid ID token not retained")
		}
	}
	stored, err := manager.credential(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	if m.refreshes.Load() != 1 || !stored.MissingIDToken || stored.RefreshToken != "secret-rotated" || !stored.ExpiresAt.Equal(c.ExpiresAt) {
		t.Fatal("ID-less refresh repeatedly retried or lost rotation")
	}
}

func TestCrossProcessRefresh(t *testing.T) {
	t.Parallel()
	m := newTestIssuer(t, issuerResponse{})
	manager, p := testManager(t, m)
	c := initialCredential(t, testFlow(t, m), m)
	c.RefreshAt = time.Time{}
	saveTestSession(t, manager, p, c)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	children := make([]*exec.Cmd, 0, 2)
	store, ok := manager.Store.(fileTestStore)
	if !ok {
		t.Fatal("unexpected test store")
	}
	for range 2 {
		cmd := exec.CommandContext(t.Context(), exe, "-test.run=^TestStoredTokenProcess$")
		cmd.Env = append(os.Environ(), "NIKS3_TEST_PROFILE_DIR="+manager.Dir, "NIKS3_TEST_STORE_DIR="+store.dir)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		children = append(children, cmd)
	}
	for _, cmd := range children {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("child token lookup failed: %v", err)
		}
	}
	if m.refreshes.Load() != 1 {
		t.Fatalf("concurrent processes refreshed %d times, want 1", m.refreshes.Load())
	}
	stored, err := manager.credential(t.Context(), p)
	if err != nil {
		t.Fatal(err)
	}
	if stored.RefreshToken != "secret-rotated" {
		t.Fatal("rotation not committed")
	}
}

func TestStoredTokenProcess(t *testing.T) {
	t.Parallel()
	dir := os.Getenv("NIKS3_TEST_PROFILE_DIR")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	manager := &Manager{Dir: dir, Store: fileTestStore{dir: os.Getenv("NIKS3_TEST_STORE_DIR")}}
	if token, err := manager.TokenSource("https://cache.example.com")(t.Context()); err != nil || token == "" {
		t.Fatal("child could not obtain stored token")
	}
}

func TestProfileLockCancellation(t *testing.T) {
	t.Parallel()
	manager := &Manager{Dir: t.TempDir()}
	lock, err := manager.lock(t.Context(), "https://cache.example.com")
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := manager.lock(ctx, "https://cache.example.com"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("contended lock did not honor cancellation")
	}
}

func TestLogoutWaitsForRefresh(t *testing.T) {
	t.Parallel()
	m := newTestIssuer(t, issuerResponse{})
	manager, p := testManager(t, m)
	c := initialCredential(t, testFlow(t, m), m)
	saveTestSession(t, manager, p, c)
	lock, err := manager.lock(t.Context(), p.Server)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- manager.Logout(t.Context(), p.Server) }()
	select {
	case <-finished:
		t.Fatal("logout did not participate in the credential lock")
	case <-time.After(100 * time.Millisecond):
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if _, err := manager.TokenSource(p.Server)(t.Context()); !errors.Is(err, ErrNotFound) {
		t.Fatal("logout failed")
	}
}

func TestConcurrentSources(t *testing.T) {
	t.Parallel()
	m := newTestIssuer(t, issuerResponse{})
	manager, p := testManager(t, m)
	c := initialCredential(t, testFlow(t, m), m)
	c.RefreshAt = time.Time{}
	saveTestSession(t, manager, p, c)
	source := manager.TokenSource(p.Server)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if token, err := source(t.Context()); err != nil || token == "" {
				t.Error("concurrent token lookup failed")
			}
		})
	}
	wg.Wait()
	if m.refreshes.Load() != 1 {
		t.Fatal("multiple refreshes for concurrent token lookups")
	}
}

func TestCanonicalServer(t *testing.T) {
	t.Parallel()
	server, err := CanonicalServer("https://CACHE.example.com:443/path/")
	if err != nil || server != "https://cache.example.com/path" {
		t.Fatalf("canonical URL = %q: %v", server, err)
	}
	for _, raw := range []string{"http://cache.example.com", "https://user@cache.example.com", "https://cache.example.com?x=y", "https://cache.example.com#fragment"} {
		if _, err := CanonicalServer(raw); err == nil {
			t.Fatalf("unsafe server URL accepted: %s", raw)
		}
	}
	p := Profile{Server: server, Config: Config{Issuer: "https://auth.example.com", ClientID: "one"}}
	q := p
	q.Config.ClientID = "two"
	if p.key() == q.key() {
		t.Fatal("client identities share a key")
	}
	q = p
	q.Config.Issuer += "/other"
	if p.key() == q.key() {
		t.Fatal("issuers share a key")
	}
}
