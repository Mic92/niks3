package client_test

import (
	"reflect"
	"testing"

	"github.com/Mic92/niks3/client"
)

const (
	ab  = "a b"
	abc = "abc"
)

func TestShellSplit(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"   ", nil},
		{"a b c", []string{"a", "b", "c"}},
		{"a  b\tc\n", []string{"a", "b", "c"}},
		{"'a b' c", []string{ab, "c"}},
		{`"a b" c`, []string{ab, "c"}},
		{`a\ b c`, []string{ab, "c"}},
		{`a"b"'c'`, []string{abc}},
		{`""`, []string{""}},
		{`''`, []string{""}},
		// Single quotes are literal: no escape, no nesting.
		{`'a\nb'`, []string{`a\nb`}},
		// Double quotes allow backslash escapes.
		{`"a\"b"`, []string{`a"b`}},
		{`"a\\b"`, []string{`a\b`}},
		// No expansion of any kind.
		{`$HOME`, []string{"$HOME"}},
		{"`whoami`", []string{"`whoami`"}},
		// Realistic credential commands.
		{`vault token lookup -format=json`, []string{"vault", "token", "lookup", "-format=json"}},
		{`sh -c 'curl -s "$URL" | jq .token'`, []string{"sh", "-c", `curl -s "$URL" | jq .token`}},
	}

	for _, tc := range cases {
		got, err := client.ShellSplit(tc.in)
		if err != nil {
			t.Errorf("shellSplit(%q): unexpected error %v", tc.in, err)

			continue
		}

		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("shellSplit(%q) = %#v, want %#v", tc.in, got, tc.want)
		}
	}
}

func TestShellSplitErrors(t *testing.T) {
	t.Parallel()

	for _, in := range []string{`'open`, `"open`, `trail\`} {
		if _, err := client.ShellSplit(in); err == nil {
			t.Errorf("shellSplit(%q): expected error", in)
		}
	}
}
