package oidclogin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

const (
	keychainService = "io.github.Mic92.niks3"
	keychainMissing = -25300 // errSecItemNotFound
)

type systemStore struct{}

// Keychain calls stay in-process: secrets never enter argv, environment
// variables or temporary files. purego preserves CGO_ENABLED=0 releases.
type keychainAPI struct {
	stringCreate func(uintptr, string, uint32) uintptr
	dataCreate   func(uintptr, *byte, int) uintptr
	dictCreate   func(uintptr, int, uintptr, uintptr) uintptr
	dictSet      func(uintptr, uintptr, uintptr)
	release      func(uintptr)
	dataLength   func(uintptr) int
	dataBytes    func(uintptr) *byte
	add          func(uintptr, *uintptr) int32
	update       func(uintptr, uintptr) int32
	read         func(uintptr, *uintptr) int32
	remove       func(uintptr) int32
	constants    map[string]uintptr
	keyCallbacks uintptr
	valCallbacks uintptr
}

var macKeychain = sync.OnceValues(loadKeychain) //nolint:gochecknoglobals // framework bindings are initialized once and retained for the process lifetime

func loadKeychain() (*keychainAPI, error) {
	cf, err := purego.Dlopen("/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation", purego.RTLD_NOW|purego.RTLD_LOCAL)
	if err != nil {
		return nil, fmt.Errorf("loading macOS CoreFoundation: %w", err)
	}
	sec, err := purego.Dlopen("/System/Library/Frameworks/Security.framework/Security", purego.RTLD_NOW|purego.RTLD_LOCAL)
	if err != nil {
		return nil, fmt.Errorf("loading macOS Security: %w", err)
	}
	api := &keychainAPI{constants: make(map[string]uintptr)}
	var bundleGet func(uintptr) uintptr
	var bundleSymbol func(uintptr, uintptr) *uintptr
	for _, binding := range []struct {
		library uintptr
		name    string
		target  any
	}{
		{cf, "CFStringCreateWithCString", &api.stringCreate},
		{cf, "CFDataCreate", &api.dataCreate},
		{cf, "CFDictionaryCreateMutable", &api.dictCreate},
		{cf, "CFDictionarySetValue", &api.dictSet},
		{cf, "CFRelease", &api.release},
		{cf, "CFDataGetLength", &api.dataLength},
		{cf, "CFDataGetBytePtr", &api.dataBytes},
		{cf, "CFBundleGetBundleWithIdentifier", &bundleGet},
		{cf, "CFBundleGetDataPointerForName", &bundleSymbol},
		{sec, "SecItemAdd", &api.add},
		{sec, "SecItemUpdate", &api.update},
		{sec, "SecItemCopyMatching", &api.read},
		{sec, "SecItemDelete", &api.remove},
	} {
		symbol, err := purego.Dlsym(binding.library, binding.name)
		if err != nil {
			return nil, fmt.Errorf("loading macOS Keychain function %s: %w", binding.name, err)
		}
		purego.RegisterFunc(binding.target, symbol)
	}
	for _, framework := range []struct {
		identifier string
		names      []string
	}{
		{"com.apple.CoreFoundation", []string{"kCFBooleanTrue"}},
		{"com.apple.security", []string{
			"kSecClass", "kSecClassGenericPassword", "kSecAttrService", "kSecAttrAccount",
			"kSecValueData", "kSecReturnData", "kSecMatchLimit", "kSecMatchLimitOne",
			"kSecUseAuthenticationUI", "kSecUseAuthenticationUIFail",
		}},
	} {
		identifier := api.cfString(framework.identifier)
		bundle := bundleGet(identifier)
		api.release(identifier)
		if bundle == 0 {
			return nil, fmt.Errorf("loading macOS Keychain bundle %s", framework.identifier)
		}
		for _, name := range framework.names {
			key := api.cfString(name)
			value := bundleSymbol(bundle, key)
			api.release(key)
			if value == nil || *value == 0 {
				return nil, fmt.Errorf("loading macOS Keychain constant %s", name)
			}
			api.constants[name] = *value
		}
	}
	api.keyCallbacks, err = purego.Dlsym(cf, "kCFTypeDictionaryKeyCallBacks")
	if err != nil {
		return nil, fmt.Errorf("loading macOS dictionary key callbacks: %w", err)
	}
	api.valCallbacks, err = purego.Dlsym(cf, "kCFTypeDictionaryValueCallBacks")
	if err != nil {
		return nil, fmt.Errorf("loading macOS dictionary value callbacks: %w", err)
	}

	// The frameworks remain loaded for the process lifetime, as do the
	// function pointers and constants retained by this singleton.
	return api, nil
}

func (api *keychainAPI) cfString(value string) uintptr {
	return api.stringCreate(0, value, 0x08000100) // kCFStringEncodingUTF8
}

func (api *keychainAPI) dictionary() uintptr {
	return api.dictCreate(0, 0, api.keyCallbacks, api.valCallbacks)
}

func (api *keychainAPI) set(dict uintptr, key string, value uintptr) {
	api.dictSet(dict, api.constants[key], value)
}

func (api *keychainAPI) query(key string) uintptr {
	query := api.dictionary()
	service, account := api.cfString(keychainService), api.cfString(key)
	defer api.release(service)
	defer api.release(account)
	api.set(query, "kSecClass", api.constants["kSecClassGenericPassword"])
	api.set(query, "kSecAttrService", service)
	api.set(query, "kSecAttrAccount", account)
	// Never open a Keychain approval/unlock dialog during login, refresh or
	// logout. An inaccessible item is an actionable error instead.
	api.set(query, "kSecUseAuthenticationUI", api.constants["kSecUseAuthenticationUIFail"])

	return query
}

func keychainError(status int32) error {
	if status == 0 {
		return nil
	}
	if status == keychainMissing {
		return ErrNotFound
	}

	return fmt.Errorf("macOS Keychain is unavailable, locked, or denied access (status %d); unlock your keychain in the desktop session, or use --oidc-device without persistence", status)
}

func (systemStore) Load(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // preserve context cancellation
	}
	api, err := macKeychain()
	if err != nil {
		return nil, err
	}
	query := api.query(key)
	defer api.release(query)
	api.set(query, "kSecReturnData", api.constants["kCFBooleanTrue"])
	api.set(query, "kSecMatchLimit", api.constants["kSecMatchLimitOne"])
	var result uintptr
	if err := keychainError(api.read(query, &result)); err != nil {
		return nil, err
	}
	defer api.release(result)
	// Copy CFData before releasing it. No Go slice outlives the native data.
	value := append([]byte(nil), unsafe.Slice(api.dataBytes(result), api.dataLength(result))...)

	return value, nil
}

func (systemStore) Save(ctx context.Context, key string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // preserve context cancellation
	}
	if len(value) == 0 {
		return errors.New("cannot save an empty macOS Keychain credential")
	}
	api, err := macKeychain()
	if err != nil {
		return err
	}
	query := api.query(key)
	defer api.release(query)
	data := api.dataCreate(0, &value[0], len(value))
	defer api.release(data)
	attributes := api.dictionary()
	defer api.release(attributes)
	api.set(attributes, "kSecValueData", data)
	status := api.update(query, attributes)
	if status == keychainMissing {
		api.set(query, "kSecValueData", data)
		status = api.add(query, nil)
	}

	return keychainError(status)
}

func (systemStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // preserve context cancellation
	}
	api, err := macKeychain()
	if err != nil {
		return err
	}
	query := api.query(key)
	defer api.release(query)

	return keychainError(api.remove(query))
}
