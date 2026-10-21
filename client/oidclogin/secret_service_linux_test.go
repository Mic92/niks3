package oidclogin //nolint:testpackage // exercises internal validation, clock, locking and wire protocol

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/godbus/dbus/v5"
)

// fakeSecrets exercises the production backend over an isolated real D-Bus,
// without accessing the developer's desktop keyring.
type fakeSecrets struct {
	mu      sync.Mutex
	conn    *dbus.Conn
	records map[string]secretValue
	locked  bool
	prompt  bool
}

func (s *fakeSecrets) OpenSession(_ string, _ dbus.Variant) (dbus.Variant, dbus.ObjectPath, *dbus.Error) {
	return dbus.MakeVariant(""), "/session", nil
}

func (s *fakeSecrets) SearchItems(attrs map[string]string) ([]dbus.ObjectPath, []dbus.ObjectPath, *dbus.Error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := dbus.ObjectPath("/items/" + attrs["profile"])
	if _, ok := s.records[string(path)]; !ok {
		return []dbus.ObjectPath{}, []dbus.ObjectPath{}, nil
	}
	if s.locked {
		return []dbus.ObjectPath{}, []dbus.ObjectPath{path}, nil
	}

	return []dbus.ObjectPath{path}, []dbus.ObjectPath{}, nil
}
func (*fakeSecrets) ReadAlias(_ string) (dbus.ObjectPath, *dbus.Error) { return "/collection", nil }
func (s *fakeSecrets) Get(_, _ string) (dbus.Variant, *dbus.Error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return dbus.MakeVariant(s.locked), nil
}

func (s *fakeSecrets) CreateItem(props map[string]dbus.Variant, value secretValue, _ bool) (dbus.ObjectPath, dbus.ObjectPath, *dbus.Error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.prompt {
		return "/", "/prompt", nil
	}
	attrs, ok := props[itemInterface+".Attributes"].Value().(map[string]string)
	if !ok {
		return "/", "/", dbus.MakeFailedError(errors.New("invalid attributes"))
	}
	path := dbus.ObjectPath("/items/" + attrs["profile"])
	s.records[string(path)] = value
	if err := s.conn.Export(&fakeSecretItem{service: s, path: path}, path, itemInterface); err != nil {
		return "/", "/", dbus.MakeFailedError(err)
	}

	return path, "/", nil
}

type fakeSecretItem struct {
	service *fakeSecrets
	path    dbus.ObjectPath
}

func (i *fakeSecretItem) GetSecret(session dbus.ObjectPath) (secretValue, *dbus.Error) {
	i.service.mu.Lock()
	defer i.service.mu.Unlock()
	value := i.service.records[string(i.path)]
	value.Session = session

	return value, nil
}

func (i *fakeSecretItem) SetSecret(value secretValue) *dbus.Error {
	i.service.mu.Lock()
	defer i.service.mu.Unlock()
	i.service.records[string(i.path)] = value

	return nil
}

func (i *fakeSecretItem) Delete() (dbus.ObjectPath, *dbus.Error) {
	i.service.mu.Lock()
	defer i.service.mu.Unlock()
	delete(i.service.records, string(i.path))

	return "/", nil
}

func privateSecretService(t *testing.T) *fakeSecrets {
	t.Helper()
	exe, err := exec.LookPath("dbus-daemon")
	if err != nil {
		t.Skip("dbus-daemon unavailable for private Secret Service test")
	}
	// Do not depend on /etc/dbus-1/session.conf, which is intentionally
	// absent in the Nix sandbox. The private bus needs no service activation.
	configPath := filepath.Join(t.TempDir(), "bus.conf")
	config := `<busconfig>
  <type>session</type>
  <listen>unix:tmpdir=/tmp</listen>
  <auth>EXTERNAL</auth>
  <policy context="default">
    <allow send_destination="*" eavesdrop="true"/>
    <allow eavesdrop="true"/>
    <allow own="*"/>
  </policy>
</busconfig>`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), exe, "--config-file="+configPath, "--nofork", "--print-address=1", "--nopidfile")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		// Wait before inspecting the buffer written by exec's stderr copier.
		_ = cmd.Wait()
		t.Fatalf("private D-Bus did not start: %s", stderr.String())
	}
	address := scanner.Text()
	t.Setenv("DBUS_SESSION_BUS_ADDRESS", address)
	conn, err := dbus.Connect(address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	reply, err := conn.RequestName(secretService, dbus.NameFlagDoNotQueue)
	if err != nil || reply != dbus.RequestNameReplyPrimaryOwner {
		t.Fatal("cannot own private Secret Service name")
	}
	fake := &fakeSecrets{conn: conn, records: make(map[string]secretValue)}
	for _, entry := range []struct {
		path  dbus.ObjectPath
		iface string
	}{{servicePath, serviceInterface}, {"/collection", collectionInterface}, {"/collection", "org.freedesktop.DBus.Properties"}} {
		if err := conn.Export(fake, entry.path, entry.iface); err != nil {
			t.Fatal(err)
		}
	}

	return fake
}

func TestSecretServiceBackend(t *testing.T) {
	fake := privateSecretService(t)
	store := systemStore{}
	ctx := t.Context()
	if _, err := store.Load(ctx, "one"); !errors.Is(err, ErrNotFound) {
		t.Fatal("missing secret not reported")
	}
	for _, value := range []string{"initial", "rotated"} {
		if err := store.Save(ctx, "one", []byte(value)); err != nil {
			t.Fatal(err)
		}
		got, err := store.Load(ctx, "one")
		if err != nil || !bytes.Equal(got, []byte(value)) {
			t.Fatal("Secret Service round trip failed")
		}
	}
	if _, err := store.Load(ctx, "two"); !errors.Is(err, ErrNotFound) {
		t.Fatal("secret leaked across keys")
	}
	fake.mu.Lock()
	fake.locked = true
	fake.mu.Unlock()
	if _, err := store.Load(ctx, "one"); err == nil {
		t.Fatal("locked item loaded")
	}
	if err := store.Save(ctx, "one", []byte("rejected")); err == nil {
		t.Fatal("locked item overwritten")
	}
	if err := store.Delete(ctx, "one"); err == nil {
		t.Fatal("locked item deleted")
	}
	fake.mu.Lock()
	fake.locked = false
	fake.prompt = true
	fake.mu.Unlock()
	if err := store.Save(ctx, "two", []byte("rejected")); err == nil {
		t.Fatal("keyring prompt accepted")
	}
	fake.mu.Lock()
	fake.prompt = false
	fake.mu.Unlock()
	if err := store.Delete(ctx, "one"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(ctx, "one"); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted secret still available")
	}
	t.Setenv("DBUS_SESSION_BUS_ADDRESS", "")
	if _, err := store.Load(context.Background(), "one"); err == nil {
		t.Fatal("missing session bus accepted")
	}
}
