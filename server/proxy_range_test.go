package server_test

import (
	"errors"
	"testing"

	"github.com/Mic92/niks3/server"
)

func TestParseSingleRange(t *testing.T) {
	t.Parallel()

	const size = 10000

	tests := []struct {
		name       string
		spec       string
		start, end int64
		wantNil    bool
		err        error
	}{
		{name: "none", spec: "", wantNil: true},
		{name: "unknown unit", spec: "items=0-99", wantNil: true},
		{name: "multi-range ignored", spec: "bytes=0-99,200-299", wantNil: true},
		{name: "malformed no dash", spec: "bytes=0", wantNil: true},
		{name: "malformed both empty", spec: "bytes=-", wantNil: true},
		{name: "malformed end before start", spec: "bytes=200-100", wantNil: true},

		{name: "closed", spec: "bytes=100-199", start: 100, end: 199},
		{name: "open-ended", spec: "bytes=9900-", start: 9900, end: 9999},
		{name: "end clamped to size", spec: "bytes=9900-99999", start: 9900, end: 9999},
		{name: "suffix", spec: "bytes=-100", start: 9900, end: 9999},
		{name: "suffix exceeds size", spec: "bytes=-99999", start: 0, end: 9999},
		{name: "single byte", spec: "bytes=0-0", start: 0, end: 0},

		{name: "start past EOF", spec: "bytes=10000-", wantNil: true, err: server.ErrUnsatisfiableRange},
		{name: "start far past EOF", spec: "bytes=99999-100000", wantNil: true, err: server.ErrUnsatisfiableRange},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := server.ParseSingleRange(tc.spec, size)

			if !errors.Is(err, tc.err) {
				t.Fatalf("err = %v, want %v", err, tc.err)
			}

			if tc.wantNil {
				if got != nil {
					t.Fatalf("got %+v, want nil", got)
				}

				return
			}

			if got == nil || got.Start() != tc.start || got.End() != tc.end {
				t.Fatalf("got %+v, want [%d,%d]", got, tc.start, tc.end)
			}
		})
	}
}
