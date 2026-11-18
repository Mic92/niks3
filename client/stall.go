package client

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"
)

// stallTimeout is how long a connection may move no data, and how long a
// request may wait for its answer. A throttled server takes minutes to
// answer a push with many objects.
const stallTimeout = 5 * time.Minute

// stallConn fails a read or write once the connection moved no data for the
// timeout.
type stallConn struct {
	net.Conn

	timeout time.Duration
}

func (c *stallConn) Read(p []byte) (int, error) {
	_ = c.SetReadDeadline(time.Now().Add(c.timeout))

	n, err := c.Conn.Read(p)

	return n, c.explain(err)
}

// The transport reads all the time, also during an upload that only writes.
// A write therefore moves the deadline of that read too.
func (c *stallConn) Write(p []byte) (int, error) {
	_ = c.SetDeadline(time.Now().Add(c.timeout))

	n, err := c.Conn.Write(p)

	return n, c.explain(err)
}

func (c *stallConn) explain(err error) error {
	if os.IsTimeout(err) {
		return fmt.Errorf("connection moved no data for %s: %w", c.timeout, err)
	}

	return err
}

type dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

func dialStalling(dial dialFunc, timeout time.Duration) dialFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		return &stallConn{Conn: conn, timeout: timeout}, nil
	}
}
