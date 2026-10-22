package oidclogin

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/godbus/dbus/v5"
)

const (
	secretService       = "org.freedesktop.secrets"
	serviceInterface    = "org.freedesktop.Secret.Service"
	collectionInterface = "org.freedesktop.Secret.Collection"
	itemInterface       = "org.freedesktop.Secret.Item"
	servicePath         = dbus.ObjectPath("/org/freedesktop/secrets")
)

type systemStore struct{}

// secretValue is the Secret Service (oayays) wire structure. Plain sessions
// rely on the authenticated local session bus, not on filesystem storage.
type secretValue struct {
	Session     dbus.ObjectPath
	Parameters  []byte
	Value       []byte
	ContentType string
}

type secretSession struct {
	conn       *dbus.Conn
	path       dbus.ObjectPath
	collection dbus.ObjectPath
}

func keyringError() error {
	return errors.New("the Linux Secret Service is unavailable, locked, or requires a prompt; unlock your default keyring in the desktop session, or use --oidc-device without persistence")
}

func openSecrets(ctx context.Context) (*secretSession, error) {
	// Never auto-launch a bus or display a keyring-unlock prompt from a hook,
	// CI process, or token refresh. The desktop session must already be ready.
	address := os.Getenv("DBUS_SESSION_BUS_ADDRESS")
	if address == "" {
		return nil, keyringError()
	}
	conn, err := dbus.Connect(address, dbus.WithContext(ctx))
	if err != nil {
		return nil, keyringError()
	}
	s := &secretSession{conn: conn}
	var output dbus.Variant
	err = s.conn.Object(secretService, servicePath).CallWithContext(ctx, serviceInterface+".OpenSession", 0,
		"plain", dbus.MakeVariant("")).Store(&output, &s.path)
	if err != nil || !s.path.IsValid() || s.path == "/" {
		s.close()

		return nil, keyringError()
	}
	if err := s.conn.Object(secretService, servicePath).CallWithContext(ctx, serviceInterface+".ReadAlias", 0, "default").Store(&s.collection); err != nil || s.collection == "/" {
		s.close()

		return nil, keyringError()
	}
	var locked dbus.Variant
	if err := s.conn.Object(secretService, s.collection).CallWithContext(ctx, "org.freedesktop.DBus.Properties.Get", 0, collectionInterface, "Locked").Store(&locked); err != nil {
		s.close()

		return nil, keyringError()
	}
	if isLocked, ok := locked.Value().(bool); !ok || isLocked {
		s.close()

		return nil, keyringError()
	}

	return s, nil
}

func (s *secretSession) close() { _ = s.conn.Close() }
func secretAttributes(key string) map[string]string {
	return map[string]string{"application": "io.github.Mic92.niks3", "profile": key}
}

func (s *secretSession) find(ctx context.Context, key string) (dbus.ObjectPath, error) {
	var unlocked, locked []dbus.ObjectPath
	err := s.conn.Object(secretService, servicePath).CallWithContext(ctx, serviceInterface+".SearchItems", 0, secretAttributes(key)).Store(&unlocked, &locked)
	if err != nil || len(locked) != 0 || len(unlocked) > 1 {
		return "", keyringError()
	}
	if len(unlocked) == 0 {
		return "", ErrNotFound
	}

	return unlocked[0], nil
}

func (systemStore) Load(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	s, err := openSecrets(ctx)
	if err != nil {
		return nil, err
	}
	defer s.close()
	item, err := s.find(ctx, key)
	if err != nil {
		return nil, err
	}
	var secret secretValue
	if err := s.conn.Object(secretService, item).CallWithContext(ctx, itemInterface+".GetSecret", 0, s.path).Store(&secret); err != nil {
		return nil, keyringError()
	}
	if secret.Session != s.path || len(secret.Parameters) != 0 {
		return nil, keyringError()
	}

	return secret.Value, nil
}

func (systemStore) Save(ctx context.Context, key string, value []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	s, err := openSecrets(ctx)
	if err != nil {
		return err
	}
	defer s.close()
	secret := secretValue{Session: s.path, Parameters: []byte{}, Value: value, ContentType: "application/json"}
	item, err := s.find(ctx, key)
	if err == nil {
		if err := s.conn.Object(secretService, item).CallWithContext(ctx, itemInterface+".SetSecret", 0, secret).Err; err != nil {
			return keyringError()
		}

		return nil
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	props := map[string]dbus.Variant{
		itemInterface + ".Label":      dbus.MakeVariant("niks3 OIDC login"),
		itemInterface + ".Attributes": dbus.MakeVariant(secretAttributes(key)),
	}
	var prompt dbus.ObjectPath
	if err := s.conn.Object(secretService, s.collection).CallWithContext(ctx, collectionInterface+".CreateItem", 0, props, secret, true).Store(&item, &prompt); err != nil || prompt != "/" || item == "/" {
		return keyringError()
	}

	return nil
}

func (systemStore) Delete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	s, err := openSecrets(ctx)
	if err != nil {
		return err
	}
	defer s.close()
	item, err := s.find(ctx, key)
	if err != nil {
		return err
	}
	var prompt dbus.ObjectPath
	if err := s.conn.Object(secretService, item).CallWithContext(ctx, itemInterface+".Delete", 0).Store(&prompt); err != nil || prompt != "/" {
		return keyringError()
	}

	return nil
}
