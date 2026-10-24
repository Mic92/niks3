//go:build linux || darwin

package oidclogin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func lockProfile(ctx context.Context, path string) (io.Closer, error) {
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening OIDC profile lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	timer := time.NewTicker(50 * time.Millisecond)
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			_ = file.Close()

			return nil, fmt.Errorf("waiting for OIDC profile lock: %w", err)
		}
		err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EINTR) {
			_ = file.Close()

			return nil, fmt.Errorf("locking OIDC profile: %w", err)
		}
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
	}
}
