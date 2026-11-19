package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Mic92/niks3/client"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
)

// A failed build has no output whose narinfo could keep its log.
func TestClientPushesTheLogOfAFailedBuild(t *testing.T) {
	t.Parallel()

	service := createTestServiceWithAuth(t, testAuthToken)
	defer service.Close()

	mux := http.NewServeMux()
	registerTestHandlers(mux, service)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	root := t.TempDir()
	name := strings.Repeat("a", 32) + "-failed.drv"
	logDir := filepath.Join(root, "var", "log", "nix", "drvs", name[:2])
	ok(t, os.MkdirAll(logDir, 0o700))
	ok(t, os.WriteFile(filepath.Join(logDir, name[2:]), []byte("builder failed with exit code 1\n"), 0o600))

	c, err := client.NewClient(t.Context(), ts.URL, testAuthToken)
	ok(t, err)
	ok(t, c.PushBuildLog(t.Context(), filepath.Join(root, "store", name)))

	collectGarbage(t, service)

	obj, err := service.MinioClient.GetObject(t.Context(), service.Bucket, "log/"+name, minio.GetObjectOptions{})
	ok(t, err)

	plain, err := zstd.NewReader(obj)
	ok(t, err)

	defer plain.Close()

	log, err := io.ReadAll(plain)
	ok(t, err)
	if string(log) != "builder failed with exit code 1\n" {
		t.Fatalf("the cache serves %q", log)
	}
}
