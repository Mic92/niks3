package server_test

import (
	"testing"

	"github.com/Mic92/niks3/server"
)

// An S3 concurrency of zero would make every errgroup.Go block forever, so
// it must be refused up front rather than hang uploads, GC and shutdown.
func TestValidateS3Concurrency(t *testing.T) {
	t.Parallel()

	for _, n := range []int{0, -1} {
		if err := server.ValidateS3Concurrency(n); err == nil {
			t.Errorf("--s3-concurrency %d accepted", n)
		}
	}

	for _, n := range []int{1, 100} {
		if err := server.ValidateS3Concurrency(n); err != nil {
			t.Errorf("--s3-concurrency %d rejected: %v", n, err)
		}
	}
}
