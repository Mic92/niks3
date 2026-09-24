package server_test

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Mic92/niks3/server"
	minio "github.com/minio/minio-go/v7"
)

// callPinHandler drives a pin handler and returns the response. Safe to call
// from a goroutine: it reports nothing through t.
func callPinHandler(ctx context.Context, handler http.HandlerFunc, method, name, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(ctx, method, "/api/pins/"+name, strings.NewReader(body))
	req.SetPathValue("name", name)

	w := httptest.NewRecorder()
	handler(w, req)

	return w
}

func createPin(ctx context.Context, service *server.Service, name, storePath string) *httptest.ResponseRecorder {
	return callPinHandler(ctx, service.CreatePinHandler, http.MethodPost, name, `{"store_path":"`+storePath+`"}`)
}

// insertClosureRow records a committed closure root, which is all a pin
// request checks for.
func insertClosureRow(t *testing.T, service *server.Service, hash string) {
	t.Helper()

	_, err := service.Pool.Exec(t.Context(),
		"INSERT INTO closures (key, updated_at) VALUES ($1, timezone('UTC', now()))", hash+".narinfo")
	ok(t, err)
}

// pinInBothStores returns the pin's store path as the database (the GC root)
// and as S3 (what consumers fetch) record it; "" where there is none.
func pinInBothStores(t *testing.T, service *server.Service, name string) (string, string) {
	t.Helper()

	var db string

	err := service.Pool.QueryRow(t.Context(), "SELECT store_path FROM pins WHERE name = $1", name).Scan(&db)
	if err != nil && !strings.Contains(err.Error(), "no rows") {
		ok(t, err)
	}

	obj, err := service.MinioClient.GetObject(t.Context(), service.Bucket, "pins/"+name, minio.GetObjectOptions{})
	ok(t, err)

	defer func() { _ = obj.Close() }()

	content, err := io.ReadAll(obj)
	if err != nil && minio.ToErrorResponse(err).Code != minio.NoSuchKey {
		ok(t, err)
	}

	return db, string(content)
}

// heldPinPutProxy forwards requests to S3 and holds the response to the first
// PUT of a pin object after S3 stored it, until release is closed.
type heldPinPutProxy struct {
	target  *url.URL
	once    sync.Once
	stored  chan struct{}
	release chan struct{}
}

func (p *heldPinPutProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	proxy := httputil.NewSingleHostReverseProxy(p.target)

	first := false
	if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/pins/") {
		p.once.Do(func() { first = true })
	}

	if !first {
		proxy.ServeHTTP(w, r)

		return
	}

	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, r)
	close(p.stored)
	<-p.release

	maps.Copy(w.Header(), rec.Header())
	w.WriteHeader(rec.Code)
	_, _ = w.Write(rec.Body.Bytes())
}

// Two writers moving one pin to different closures at the same time must
// leave the database and S3 naming the same store path. Otherwise consumers
// fetch a path whose closure the pin does not protect from GC.
func TestConcurrentPinUpdatesAgree(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	hashA := strings.Repeat("a", 32)
	hashB := strings.Repeat("b", 32)
	pathA := "/nix/store/" + hashA + "-app"
	pathB := "/nix/store/" + hashB + "-app"

	insertClosureRow(t, service, hashA)
	insertClosureRow(t, service, hashB)

	target, err := url.Parse(fmt.Sprintf("http://localhost:%d", testRustfsServer.port))
	ok(t, err)

	proxy := &heldPinPutProxy{target: target, stored: make(chan struct{}), release: make(chan struct{})}

	proxyServer := httptest.NewServer(proxy)
	defer proxyServer.Close()

	proxyURL, err := url.Parse(proxyServer.URL)
	ok(t, err)

	direct := service.MinioClient
	service.MinioClient = testRustfsServer.ClientWithEndpoint(t, proxyURL.Host)

	ctx := t.Context()
	first := make(chan int, 1)

	go func() { first <- createPin(ctx, service, "app", pathA).Code }()

	select {
	case <-proxy.stored:
	case <-time.After(10 * time.Second):
		t.Fatal("first pin request never wrote S3")
	}

	// The second writer runs while the first has written S3 but not yet
	// finished. Give it a moment to complete; with writers serialised it
	// cannot, and the first is released regardless.
	second := make(chan int, 1)

	go func() { second <- createPin(ctx, service, "app", pathB).Code }()

	var codes []int

	select {
	case code := <-second:
		codes = append(codes, code)
	case <-time.After(500 * time.Millisecond):
	}

	close(proxy.release)

	codes = append(codes, <-first)
	if len(codes) == 1 {
		codes = append(codes, <-second)
	}

	for _, code := range codes {
		if code != http.StatusNoContent {
			t.Fatalf("pin request answered %d", code)
		}
	}

	service.MinioClient = direct

	db, s3 := pinInBothStores(t, service, "app")
	if db != s3 {
		t.Errorf("pin app protects %q in the database but S3 serves %q", db, s3)
	}
}

// The pin body comes from any writer and its store path is served verbatim
// to consumers who substitute it into a command line. It must be bounded and
// be exactly one absolute store path, even when its hash names a closure.
func TestCreatePinRejectsBadInput(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	hash := strings.Repeat("d", 32)
	insertClosureRow(t, service, hash)

	for _, c := range []struct {
		name, storePath string
	}{
		{"relative", "nix/store/" + hash + "-app"},
		{"bare base name", hash + "-app"},
		{"second argument", "/nix/store/" + hash + "-app --option substituters http://elsewhere"},
		{"second line", `/nix/store/` + hash + `-app\n/nix/store/` + hash + `-app`},
		{"dot-dot component", "/nix/store/../store/" + hash + "-app"},
		{"short hash", "/nix/store/" + hash[:31] + "-app"},
		{"hash outside nix32", "/nix/store/" + strings.Repeat("e", 32) + "-app"},
		{"name outside store alphabet", "/nix/store/" + hash + "-a/pp"},
	} {
		if w := createPin(t.Context(), service, "deploy", c.storePath); w.Code != http.StatusBadRequest {
			t.Errorf("%s: %q answered %d, want 400", c.name, c.storePath, w.Code)
		}
	}

	huge := `{"store_path":"/nix/store/` + hash + "-" + strings.Repeat("a", 9<<20) + `"}`
	if w := callPinHandler(t.Context(), service.CreatePinHandler, http.MethodPost, "deploy", huge); w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("9 MiB body answered %d, want 413", w.Code)
	}

	if db, s3 := pinInBothStores(t, service, "deploy"); db != "" || s3 != "" {
		t.Errorf("rejected requests left pin deploy behind: %d bytes in the database, %d in S3", len(db), len(s3))
	}

	if w := createPin(t.Context(), service, "deploy", "/nix/store/"+hash+"-app-1.0+git_x?y=z"); w.Code != http.StatusNoContent {
		t.Errorf("valid store path answered %d: %s", w.Code, w.Body.String())
	}
}

// A pin whose S3 object could not be removed must keep its row: the object
// stays public, and without the row its closure is no longer a GC root.
func TestDeletePinKeepsRowWhenS3Fails(t *testing.T) {
	t.Parallel()

	service := createTestService(t)
	defer service.Close()

	hash := strings.Repeat("c", 32)
	storePath := "/nix/store/" + hash + "-app"

	insertClosureRow(t, service, hash)

	if w := createPin(t.Context(), service, "app", storePath); w.Code != http.StatusNoContent {
		t.Fatalf("create pin: %d %s", w.Code, w.Body.String())
	}

	good := service.MinioClient
	service.MinioClient = brokenS3Client(t)

	w := callPinHandler(t.Context(), service.DeletePinHandler, http.MethodDelete, "app", "")

	service.MinioClient = good

	if w.Code == http.StatusNoContent {
		t.Errorf("delete answered 204 although S3 could not be reached")
	}

	db, s3 := pinInBothStores(t, service, "app")
	if db != s3 {
		t.Errorf("pin app protects %q in the database but S3 serves %q", db, s3)
	}

	// With S3 back the delete goes through in both stores.
	if w := callPinHandler(t.Context(), service.DeletePinHandler, http.MethodDelete, "app", ""); w.Code != http.StatusNoContent {
		t.Fatalf("delete pin: %d %s", w.Code, w.Body.String())
	}

	if db, s3 := pinInBothStores(t, service, "app"); db != "" || s3 != "" {
		t.Errorf("pin app left behind: database %q, S3 %q", db, s3)
	}
}
