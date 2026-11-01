package server_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/Mic92/niks3/server"
	"github.com/minio/minio-go/v7"
)

func TestAbortSucceeded(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "no error", err: nil, want: true},
		{name: "garage 200", err: minio.ErrorResponse{StatusCode: http.StatusOK}, want: true},
		{name: "already gone", err: minio.ErrorResponse{StatusCode: http.StatusNotFound, Code: minio.NoSuchUpload}, want: true},
		{name: "server error", err: minio.ErrorResponse{StatusCode: http.StatusInternalServerError, Code: "InternalError"}, want: false},
		{name: "transport error", err: errors.New("connection reset"), want: false},
	}

	for _, tc := range cases {
		if got := server.AbortSucceeded(tc.err); got != tc.want {
			t.Errorf("%s: AbortSucceeded = %v, want %v", tc.name, got, tc.want)
		}
	}
}
