package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
	"bytes"
)

type PostgresServer struct {
	cmd     *exec.Cmd
	tempDir string
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		tb.Errorf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}
func httpOkDepth(tb testing.TB, rr *httptest.ResponseRecorder, depth int) {
	if rr.Code < 200 || rr.Code >= 300 {
		_, file, line, _ := runtime.Caller(depth)
		tb.Errorf("\033[31m%s:%d: unexpected http status=%d body=%s\033[39m\n\n", filepath.Base(file), line, rr.Code, rr.Body.String())
		tb.FailNow()
	}
}

func httpOk(tb testing.TB, rr *httptest.ResponseRecorder) {
	httpOkDepth(tb, rr, 2)
}

func (s *PostgresServer) Cleanup() {
	err := syscall.Kill(s.cmd.Process.Pid, syscall.SIGTERM)
	if err != nil {
		slog.Error("failed to kill postgres", "error", err)
	}
	err = s.cmd.Wait()
	if err != nil {
		slog.Error("failed to wait for postgres", "error", err)
	}

	os.RemoveAll(s.tempDir)
}

func startPostgresServer(t *testing.T) *PostgresServer {
	tempdir, err := os.MkdirTemp("", "postgres")
	defer func() {
		if err != nil {
			os.RemoveAll(tempdir)
		}
	}()
	ok(t, err)
	configFile := filepath.Join(tempdir, "postgresql.conf")
	// only listen on a unix socket
	configContent := `
unix_socket_directories = '` + tempdir + `'
	`
	err = os.WriteFile(configFile, []byte(configContent), 0o644)
	ok(t, err)
	// initialize the database
	dbPath := filepath.Join(tempdir, "data")
	initdb := exec.Command("initdb", "-D", dbPath, "-U", "postgres")
	initdb.Stdout = os.Stdout
	initdb.Stderr = os.Stderr
	ok(t, initdb.Run())

	postgresProc := exec.Command("postgres", "-D", dbPath, "-k", tempdir, "-c", "listen_addresses=")
	postgresProc.Stdout = os.Stdout
	postgresProc.Stderr = os.Stderr
	ok(t, postgresProc.Start())
	return &PostgresServer{
		cmd:     postgresProc,
		tempDir: tempdir,
	}
}

type testServer struct {
	postgres *PostgresServer
	server   *Server
}

func (s *testServer) Cleanup() {
	s.server.db.Close()
	s.postgres.Cleanup()
}

func createTestEnv(t *testing.T) *testServer {
	tempServer := startPostgresServer(t)
	// wait for postgres to start

	var err error
	for i := 0; i < 30; i++ {
		waitForPostgres := exec.Command("pg_isready", "-h", tempServer.tempDir, "-U", "postgres")
		waitForPostgres.Stdout = os.Stdout
		waitForPostgres.Stderr = os.Stderr
		err = waitForPostgres.Run()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		tempServer.Cleanup()
		ok(t, err)
	}

	connectionString := fmt.Sprintf("postgres://?dbname=postgres&user=postgres&host=%s", tempServer.tempDir)
	db, err := ConnectDB(connectionString)
	if err != nil {
		tempServer.Cleanup()
		ok(t, err)
	}
	return &testServer{
		postgres: tempServer,
		server: &Server{
			db: db,
		},
	}
}

type TestRequest struct {
	method string
	path string
	body []byte
	handler http.HandlerFunc
	// function to checkResponse the response
	checkResponse *func(*testing.T, *httptest.ResponseRecorder)
	header map[string]string
}

func testRequest(req *TestRequest, tb *testing.T) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(req.handler)
	httpReq, err := http.NewRequest(req.method, req.path, bytes.NewBuffer(req.body))
	for k, v := range req.header {
		httpReq.Header.Set(k, v)
	}
	ok(tb, err)
	handler.ServeHTTP(rr, httpReq)
	if req.checkResponse == nil {
		if rr.Code < 200 || rr.Code >= 300 {
			httpOkDepth(tb, rr, 2)
		}
	} else {
		(*req.checkResponse)(tb, rr)
	}
	return rr
}

func TestServer_startUploadHandler(t *testing.T) {
	env := createTestEnv(t)
	defer env.Cleanup()

	testRequest(&TestRequest{
		method: "GET",
	  path: "/health",
	  handler: env.server.healthCheckHandler,
	}, t)

	body, err := json.Marshal(map[string]interface{}{
		"closure_nar_hash": "00000000000000000000000000000000",
		"store_paths":      []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
	})
	ok(t, err)

	rr := testRequest(&TestRequest{
		method: "POST",
	  path: "/upload",
	  body: body,
	  handler: env.server.startUploadHandler,
	}, t)

	var uploadResponse UploadResponse
	err = json.Unmarshal(rr.Body.Bytes(), &uploadResponse)
	slog.Info("upload response", "response", rr.Body.String(), "status", rr.Code)
	ok(t, err)
	if uploadResponse.ID == "" {
		t.Errorf("handler returned empty upload id")
	}

}
