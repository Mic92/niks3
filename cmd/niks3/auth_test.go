package main

import (
	"bytes"
	"testing"
)

func TestAuthHelpAndValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		args  []string
		valid bool
	}{
		{[]string{"--help"}, true},
		{[]string{"login", "--help"}, true},
		{[]string{"status", "--help"}, true},
		{[]string{"logout", "--help"}, true},
		{nil, false},
		{[]string{"unknown"}, false},
		{[]string{"login"}, false},
		{[]string{"status", "unexpected"}, false},
		{[]string{"logout", "--device"}, false},
	} {
		var out, instructions bytes.Buffer
		err := runAuth(t.Context(), tc.args, &out, &instructions)
		if (err == nil) != tc.valid {
			t.Fatalf("auth %v: %v", tc.args, err)
		}
		if out.Len() != 0 {
			t.Fatal("help or validation wrote to stdout")
		}
	}
}
