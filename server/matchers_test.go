package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Mic92/niks3/api"
	"github.com/Mic92/niks3/server"
)

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	tb.Helper()

	if err != nil {
		tb.Errorf("\033[31m unexpected error: %s\033[39m\n\n", err.Error())
		tb.FailNow()
	}
}

func httpOkDepth(tb testing.TB, rr *httptest.ResponseRecorder) {
	tb.Helper()

	if rr.Code < 200 || rr.Code >= 300 {
		tb.Errorf(
			"\033[31m unexpected http status=%d body=%s\033[39m\n\n", rr.Code, rr.Body.String(),
		)
		tb.FailNow()
	}
}

// waitForGC polls the task store until the GC task reaches a terminal state.
func waitForGC(tb testing.TB, service *server.Service) api.GCTaskStatus {
	tb.Helper()

	deadline := time.Now().Add(30 * time.Second)

	for {
		status, found := service.GCTasks.Get()
		if !found {
			tb.Fatal("no GC task found")
		}

		if status.State == api.GCTaskStateSucceeded || status.State == api.GCTaskStateFailed {
			return status
		}

		if time.Now().After(deadline) {
			tb.Fatalf("Timeout waiting for GC (state=%s, phase=%s)", status.State, status.Phase)
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// registerTestHandlers registers common test handlers on the given mux.
func registerTestHandlers(mux *http.ServeMux, testService *server.Service) {
	mux.HandleFunc("POST /api/pending_closures", testService.AuthMiddleware(testService.CreatePendingClosureHandler))
	mux.HandleFunc("POST /api/pending_closures/{id}/sign", testService.AuthMiddleware(testService.SignNarinfosHandler))
	mux.HandleFunc("POST /api/pending_closures/{id}/complete", testService.AuthMiddleware(testService.CommitPendingClosureHandler))
	mux.HandleFunc("POST /api/multipart/complete", testService.AuthMiddleware(testService.CompleteMultipartUploadHandler))
	mux.HandleFunc("GET /health", testService.HealthCheckHandler)
}
