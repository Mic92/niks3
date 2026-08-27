package server_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Mic92/niks3/server"
)

func TestResolveDBConnectionString(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	file := filepath.Join(dir, "uri")

	if err := os.WriteFile(file, []byte("postgres://from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	lookup := func(env map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) {
			v, ok := env[k]

			return v, ok
		}
	}

	tests := []struct {
		name    string
		flag    string
		file    string
		env     map[string]string
		want    string
		wantErr bool
	}{
		{name: "flag wins", flag: "postgres://flag", file: file, want: "postgres://flag"},
		{name: "file when flag empty", file: file, want: "postgres://from-file"},
		{name: "missing file is an error", file: filepath.Join(dir, "nope"), wantErr: true},
		{name: "PGHOST allows empty", env: map[string]string{"PGHOST": "db"}, want: ""},
		{name: "nothing configured", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := server.ResolveDBConnectionString(tc.flag, tc.file, lookup(tc.env))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}

			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
