package main

import (
	"net/http/httptest"
	"testing"
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

// func httpOk(tb testing.TB, rr *httptest.ResponseRecorder) {
// 	httpOkDepth(tb, rr)
// }
