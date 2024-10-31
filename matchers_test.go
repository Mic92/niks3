package main

import (
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"testing"
)

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
		tb.Errorf(
			"\033[31m%s:%d: unexpected http status=%d body=%s\033[39m\n\n",
			filepath.Base(file), line, rr.Code, rr.Body.String(),
		)
		tb.FailNow()
	}
}

// func httpOk(tb testing.TB, rr *httptest.ResponseRecorder) {
// 	httpOkDepth(tb, rr, 2)
// }
