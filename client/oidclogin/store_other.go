//go:build !linux

package oidclogin

import (
	"context"
	"errors"
	"io"
)

type systemStore struct{}

func unsupportedStore() error {
	return errors.New("stored OIDC login currently supports Linux Secret Service only; use --oidc-device for ephemeral login")
}
func (systemStore) Load(context.Context, string) ([]byte, error) { return nil, unsupportedStore() }
func (systemStore) Save(context.Context, string, []byte) error   { return unsupportedStore() }
func (systemStore) Delete(context.Context, string) error         { return unsupportedStore() }
func lockProfile(context.Context, string) (io.Closer, error)     { return nil, unsupportedStore() }
