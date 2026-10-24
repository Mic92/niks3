//go:build !linux && !darwin

package oidclogin

import (
	"context"
	"io"
)

func lockProfile(context.Context, string) (io.Closer, error) {
	return nil, unsupportedStore()
}
