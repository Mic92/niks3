package oidclogin //nolint:testpackage // exercises native Keychain queries without accessing the user's keychain

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"testing"
	"unsafe"
)

func TestKeychainBindings(t *testing.T) {
	t.Parallel()
	api, err := loadKeychain()
	if err != nil {
		t.Fatal(err)
	}
	// Exercise framework loading, constant lookup and CF ownership without
	// contacting securityd or accessing any real credentials in the sandbox.
	query := api.query("binding-test")
	if query == 0 {
		t.Fatal("could not allocate Keychain query")
	}
	api.release(query)
}

func TestMacOSKeychainIntegration(t *testing.T) {
	t.Parallel()
	if os.Getenv("NIKS3_TEST_MACOS_KEYCHAIN") != "1" {
		t.Skip("requires an explicitly prepared disposable Keychain")
	}
	store := systemStore{}
	key := "integration-" + rand.Text()
	t.Cleanup(func() {
		if err := store.Delete(context.Background(), key); err != nil && !errors.Is(err, ErrNotFound) {
			t.Error("cleaning up synthetic Keychain credential:", err)
		}
	})
	for _, value := range [][]byte{bytes.Repeat([]byte("synthetic-token"), 1024), []byte("rotated-token")} {
		if err := store.Save(t.Context(), key, value); err != nil {
			t.Fatal(err)
		}
		got, err := store.Load(t.Context(), key)
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("native Keychain round trip failed: %v", err)
		}
	}
	if _, err := store.Load(t.Context(), key+"-absent"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("native Keychain missing-item error: %v", err)
	}
	if err := store.Delete(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(t.Context(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("native Keychain credential survived deletion: %v", err)
	}
}

func fakeKeychain(t *testing.T) (*keychainAPI, *int32) {
	t.Helper()
	api := &keychainAPI{constants: make(map[string]uintptr)}
	var next uintptr
	allocate := func() uintptr {
		next++

		return next
	}
	for _, name := range []string{
		"kSecClass", "kSecClassGenericPassword", "kSecAttrService", "kSecAttrAccount",
		"kSecValueData", "kSecReturnData", "kSecMatchLimit", "kSecMatchLimitOne",
		"kSecUseAuthenticationUI", "kSecUseAuthenticationUIFail", "kCFBooleanTrue",
	} {
		api.constants[name] = allocate()
	}
	texts := make(map[uintptr]string)
	data := make(map[uintptr][]byte)
	dicts := make(map[uintptr]map[uintptr]uintptr)
	records := make(map[string][]byte)
	var failure int32
	api.stringCreate = func(_ uintptr, value string, _ uint32) uintptr {
		ref := allocate()
		texts[ref] = value

		return ref
	}
	api.dataCreate = func(_ uintptr, value *byte, length int) uintptr {
		ref := allocate()
		data[ref] = append([]byte(nil), unsafe.Slice(value, length)...)

		return ref
	}
	api.dictCreate = func(_ uintptr, _ int, _, _ uintptr) uintptr {
		ref := allocate()
		dicts[ref] = make(map[uintptr]uintptr)

		return ref
	}
	api.dictSet = func(dict, key, value uintptr) { dicts[dict][key] = value }
	api.release = func(uintptr) {}
	api.dataLength = func(ref uintptr) int { return len(data[ref]) }
	api.dataBytes = func(ref uintptr) *byte { return &data[ref][0] }
	account := func(query uintptr) string {
		t.Helper()
		values := dicts[query]
		if values[api.constants["kSecUseAuthenticationUI"]] != api.constants["kSecUseAuthenticationUIFail"] {
			t.Fatal("Keychain query permits interactive prompts")
		}
		if texts[values[api.constants["kSecAttrService"]]] != keychainService {
			t.Fatal("Keychain query uses the wrong service")
		}

		return texts[values[api.constants["kSecAttrAccount"]]]
	}
	api.read = func(query uintptr, result *uintptr) int32 {
		key := account(query)
		if failure != 0 {
			return failure
		}
		value, exists := records[key]
		if !exists {
			return keychainMissing
		}
		*result = api.dataCreate(0, &value[0], len(value))

		return 0
	}
	api.update = func(query, attributes uintptr) int32 {
		key := account(query)
		if failure != 0 {
			return failure
		}
		if _, exists := records[key]; !exists {
			return keychainMissing
		}
		records[key] = data[dicts[attributes][api.constants["kSecValueData"]]]

		return 0
	}
	api.add = func(query uintptr, _ *uintptr) int32 {
		records[account(query)] = data[dicts[query][api.constants["kSecValueData"]]]

		return 0
	}
	api.remove = func(query uintptr) int32 {
		key := account(query)
		if failure != 0 {
			return failure
		}
		if _, exists := records[key]; !exists {
			return keychainMissing
		}
		delete(records, key)

		return 0
	}

	return api, &failure
}

func TestKeychainBackend(t *testing.T) { //nolint:paralleltest // temporarily replaces process-wide framework bindings
	api, failure := fakeKeychain(t)
	original := macKeychain
	macKeychain = func() (*keychainAPI, error) { return api, nil }
	t.Cleanup(func() { macKeychain = original })
	store := systemStore{}
	ctx := t.Context()
	if _, err := store.Load(ctx, "one"); !errors.Is(err, ErrNotFound) {
		t.Fatal("missing credential not reported")
	}
	// Saved sessions can exceed security(1)'s interactive command-size limit.
	for _, value := range [][]byte{bytes.Repeat([]byte("synthetic-token"), 1024), []byte("rotated-token")} {
		if err := store.Save(ctx, "one", value); err != nil {
			t.Fatal(err)
		}
		got, err := store.Load(ctx, "one")
		if err != nil || !bytes.Equal(got, value) {
			t.Fatal("Keychain credential round trip failed")
		}
	}
	if _, err := store.Load(ctx, "two"); !errors.Is(err, ErrNotFound) {
		t.Fatal("credential leaked across profiles")
	}
	*failure = -25308 // errSecInteractionNotAllowed
	if _, err := store.Load(ctx, "one"); err == nil || !strings.Contains(err.Error(), "macOS Keychain") {
		t.Fatal("locked Keychain not reported")
	}
	if err := store.Save(ctx, "one", []byte("rejected")); err == nil {
		t.Fatal("locked credential overwritten")
	}
	if err := store.Delete(ctx, "one"); err == nil {
		t.Fatal("locked credential deleted")
	}
	*failure = 0
	got, err := store.Load(ctx, "one")
	if err != nil || string(got) != "rotated-token" {
		t.Fatal("failed rotation lost the existing credential")
	}
	if err := store.Delete(ctx, "one"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(ctx, "one"); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted credential still available")
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Load(ctx, "one"); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled read accepted")
	}
	if err := store.Save(ctx, "one", []byte("cancelled")); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled write accepted")
	}
	if err := store.Delete(ctx, "one"); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled delete accepted")
	}
}
