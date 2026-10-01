package signing_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Mic92/niks3/server/signing"
)

const (
	externalPublicKey  = "test-key:O2onvM62pC1io6jQKm8Nc2UyFXcd4kOmOsBIoYtZ2ik="
	externalSignature  = "test-key:Q7JG4oOWwCyqpWakWIfE8hXGcD92NPKNAsFKrjhVgkHVHLUnI8FQiwusGsypMTw9a06ZGg3i/OAl8KsVI3XlAQ=="
	externalSignature2 = "test-key:YVnA37ruTaVFRiLRfqMrdfSM8yU9OStWcP2o8rNtaQ94BEEp0wij6wvmGUQdit4UTyAS/3QI9ATGpZ00NhJUBA=="
)

func TestExternalSigner(t *testing.T) {
	t.Parallel()

	program := writeSigningProgram(t, `test "$#" -eq 0
echo called >> "$0.calls"
cat > "$0.input"
printf '%s\n' '{"signatures":{"first.narinfo":"`+externalSignature+`","second.narinfo":"`+externalSignature2+`"},"extra":{"ignored":true}}'
`)
	signer, err := signing.NewExternalSigner(program, externalPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := signer.PublicKey()
	if err != nil || publicKey != externalPublicKey {
		t.Fatalf("PublicKey = %q, %v", publicKey, err)
	}

	infos := map[string]*signing.NarInfo{
		"first.narinfo": {
			StorePath:  "/nix/store/first",
			NarHash:    "sha256:0000000000000000000000000000000000000000000000000000",
			NarSize:    123,
			References: []string{"/nix/store/zzz", "/nix/store/aaa"},
		},
		"second.narinfo": {
			StorePath: "/nix/store/second",
			NarHash:   "sha256:0000000000000000000000000000000000000000000000000000",
			NarSize:   456,
		},
	}
	signatures, err := signer.Sign(t.Context(), infos)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"first.narinfo": externalSignature, "second.narinfo": externalSignature2}
	if !maps.Equal(signatures, want) {
		t.Fatalf("signatures = %v, want %v", signatures, want)
	}

	input, err := os.ReadFile(program + ".input")
	if err != nil {
		t.Fatal(err)
	}
	var request struct {
		Fingerprints map[string]string `json:"fingerprints"`
	}
	if err := json.Unmarshal(input, &request); err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{
		"first.narinfo":  "1;/nix/store/first;sha256:0000000000000000000000000000000000000000000000000000;123;/nix/store/aaa,/nix/store/zzz",
		"second.narinfo": "1;/nix/store/second;sha256:0000000000000000000000000000000000000000000000000000;456;",
	}
	if len(request.Fingerprints) != len(expected) {
		t.Fatalf("received %d fingerprints, want %d", len(request.Fingerprints), len(expected))
	}
	for key, fingerprint := range expected {
		decoded, err := base64.StdEncoding.DecodeString(request.Fingerprints[key])
		if err != nil || string(decoded) != fingerprint {
			t.Errorf("fingerprint %q = %q, %v; want %q", key, decoded, err, fingerprint)
		}
	}
	calls, err := os.ReadFile(program + ".calls")
	if err != nil || string(calls) != "called\n" {
		t.Fatalf("program invocations = %q, %v; want one", calls, err)
	}

	wrongKeySigner, err := signing.NewExternalSigner(program, "test-key:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		t.Fatal(err)
	}
	if signatures, err := wrongKeySigner.Sign(t.Context(), infos); err == nil || len(signatures) != 0 {
		t.Fatalf("wrong public key: got %v, %v; want no signatures and an error", signatures, err)
	}
	infos["first.narinfo"].NarSize++
	if signatures, err := signer.Sign(t.Context(), infos); err == nil || len(signatures) != 0 {
		t.Fatalf("modified narinfo: got %v, %v; want no signatures and an error", signatures, err)
	}
}

func TestExternalSignerErrors(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"nonzero exit":             "exit 42\n",
		"invalid JSON":             "printf 'not json'\n",
		"wrong shape":              "printf '[]'\n",
		"null":                     "printf 'null'\n",
		"missing signatures":       `printf '%s' '{}'`,
		"null signatures":          `printf '%s' '{"signatures":null}'`,
		"wrong signatures shape":   `printf '%s' '{"signatures":[]}'`,
		"missing signature name":   `printf '%s' '{"signatures":{"test.narinfo":"signature"}}'`,
		"wrong signature name":     `printf '%s' '{"signatures":{"test.narinfo":"other-key:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=="}}'`,
		"invalid signature base64": `printf '%s' '{"signatures":{"test.narinfo":"test-key:invalid!"}}'`,
		"wrong signature length":   `printf '%s' '{"signatures":{"test.narinfo":"test-key:YQ=="}}'`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			signer, err := signing.NewExternalSigner(writeSigningProgram(t, body), externalPublicKey)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := signer.Sign(t.Context(), nil); err == nil {
				t.Fatal("expected signing to fail")
			}
		})
	}
}

func TestExternalSignerRejectsInvalidPublicKey(t *testing.T) {
	t.Parallel()

	for _, publicKey := range []string{"", "no-colon", ":AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "test-key:invalid!", "test-key:YQ=="} {
		t.Run(publicKey, func(t *testing.T) {
			t.Parallel()

			if _, err := signing.NewExternalSigner("sh", publicKey); err == nil {
				t.Fatal("expected invalid public key to fail")
			}
		})
	}
}

func TestExternalSignerCancellation(t *testing.T) {
	t.Parallel()

	program := writeSigningProgram(t, `sleep 30 &
child=$!
printf '%s\n' "$child" > "$0.pid"
wait "$child"
`)
	signer, err := signing.NewExternalSigner(program, externalPublicKey)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := signer.Sign(ctx, nil)
		done <- err
	}()

	deadline := time.After(5 * time.Second)
	var pid int
	for pid == 0 {
		data, err := os.ReadFile(program + ".pid")
		if err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
		}
		select {
		case <-deadline:
			t.Fatal("signing program did not start its child")
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Sign returned %v, want context.Canceled", err)
		}
	case <-deadline:
		t.Fatal("cancellation did not stop the signing program and its child")
	}
}

func writeSigningProgram(t *testing.T, body string) string {
	t.Helper()

	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Fatal(err)
	}
	program := filepath.Join(t.TempDir(), "sign program")
	//nolint:gosec // test script must be executable
	if err := os.WriteFile(program, []byte("#!"+shell+"\nset -eu\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}

	return program
}
