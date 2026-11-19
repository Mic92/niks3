package server_test

import (
	"testing"

	"github.com/Mic92/niks3/server"
)

func TestParsePinReadRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   string
		want    server.PinReadRule
		wantErr bool
	}{
		{
			input: "CN=*=host-{cn}",
			want:  server.PinReadRule{Subject: "CN=*", Pin: "host-{cn}"},
		},
		{
			input: "CN=web-*,O=x=web-*",
			want:  server.PinReadRule{Subject: "CN=web-*,O=x", Pin: "web-*"},
		},
		{
			input:   "CN=*=",
			wantErr: true,
		},
		{
			input:   "=host",
			wantErr: true,
		},
		{
			input:   "CN=*=host/{cn}",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got, err := server.ParsePinReadRule(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePinReadRule(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)

				return
			}
			if err == nil && got != tt.want {
				t.Errorf("ParsePinReadRule(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestCommonName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input     string
		wantCN    string
		wantFound bool
	}{
		{
			input:     "CN=web-01,O=acme",
			wantCN:    "web-01",
			wantFound: true,
		},
		{
			input:     "O=acme",
			wantCN:    "",
			wantFound: false,
		},
		{
			input:     "O=x\\,CN=evil,CN=real",
			wantCN:    "real",
			wantFound: true,
		},
		{
			input:     "CN=a\\,b,O=x",
			wantCN:    "a\\,b",
			wantFound: true,
		},
		{
			input:     "CN=a\\\\,O=b",
			wantCN:    "a\\\\",
			wantFound: true,
		},
		{
			input:     "CN=a+O=b",
			wantCN:    "a+O=b",
			wantFound: true, // + is not a field separator, so CN=a+O=b is treated as one RDN with value a+O=b
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			cn, found := server.CommonName(tt.input)
			if cn != tt.wantCN || found != tt.wantFound {
				t.Errorf("CommonName(%q) = (%q, %v), want (%q, %v)", tt.input, cn, found, tt.wantCN, tt.wantFound)
			}
		})
	}
}
