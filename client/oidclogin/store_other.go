//go:build !linux && !darwin

package oidclogin

import (
	"context"
	"errors"
)

type systemStore struct{}

func unsupportedStore() error {
	return errors.New("stored OIDC login requires Linux Secret Service or macOS Keychain; use --oidc-device for ephemeral login")
}
func (systemStore) Load(context.Context, string) ([]byte, error) { return nil, unsupportedStore() }
func (systemStore) Save(context.Context, string, []byte) error   { return unsupportedStore() }
func (systemStore) Delete(context.Context, string) error         { return unsupportedStore() }
