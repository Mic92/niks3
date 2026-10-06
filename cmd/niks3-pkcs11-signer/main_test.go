package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/miekg/pkcs11"
)

const (
	tokenLabel = "niks3-test"
	keyLabel   = "niks3-signing"
	userPIN    = "1234"
	soPIN      = "5678"

	ckmECEdwardsKeyPairGen = 0x1055
)

// newSoftHSM creates a token with an Ed25519 key pair in a temporary directory.
// It needs PKCS11_MODULE to point at libsofthsm2.so.
func newSoftHSM(t *testing.T) string {
	t.Helper()

	module := os.Getenv("PKCS11_MODULE")
	if module == "" {
		t.Skip("PKCS11_MODULE not set")
	}

	dir := t.TempDir()
	tokens := filepath.Join(dir, "tokens")

	if err := os.Mkdir(tokens, 0o700); err != nil {
		t.Fatal(err)
	}

	conf := filepath.Join(dir, "softhsm2.conf")
	if err := os.WriteFile(conf, []byte("directories.tokendir = "+tokens+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("SOFTHSM2_CONF", conf)

	ctx := pkcs11.New(module)
	if ctx == nil {
		t.Fatalf("loading %s", module)
	}

	if err := ctx.Initialize(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		_ = ctx.Finalize()
		ctx.Destroy()
	}()

	slots, err := ctx.GetSlotList(false)
	if err != nil || len(slots) == 0 {
		t.Fatalf("listing slots: %v", err)
	}

	if err := ctx.InitToken(slots[0], soPIN, tokenLabel); err != nil {
		t.Fatal(err)
	}

	// SoftHSM assigns a new slot id to the initialized token.
	slot, err := findToken(ctx, tokenLabel)
	if err != nil {
		t.Fatal(err)
	}

	session, err := ctx.OpenSession(slot, pkcs11.CKF_SERIAL_SESSION|pkcs11.CKF_RW_SESSION)
	if err != nil {
		t.Fatal(err)
	}

	if err := ctx.Login(session, pkcs11.CKU_SO, soPIN); err != nil {
		t.Fatal(err)
	}

	if err := ctx.InitPIN(session, userPIN); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Logout(session); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Login(session, pkcs11.CKU_USER, userPIN); err != nil {
		t.Fatal(err)
	}

	_, _, err = ctx.GenerateKeyPair(session,
		[]*pkcs11.Mechanism{pkcs11.NewMechanism(ckmECEdwardsKeyPairGen, nil)},
		[]*pkcs11.Attribute{
			// DER encoding of the OID 1.3.101.112, which names Ed25519.
			pkcs11.NewAttribute(pkcs11.CKA_EC_PARAMS, []byte{0x06, 0x03, 0x2b, 0x65, 0x70}),
			pkcs11.NewAttribute(pkcs11.CKA_LABEL, keyLabel),
			pkcs11.NewAttribute(pkcs11.CKA_TOKEN, true),
			pkcs11.NewAttribute(pkcs11.CKA_VERIFY, true),
		},
		[]*pkcs11.Attribute{
			pkcs11.NewAttribute(pkcs11.CKA_LABEL, keyLabel),
			pkcs11.NewAttribute(pkcs11.CKA_TOKEN, true),
			pkcs11.NewAttribute(pkcs11.CKA_PRIVATE, true),
			pkcs11.NewAttribute(pkcs11.CKA_SENSITIVE, true),
			pkcs11.NewAttribute(pkcs11.CKA_SIGN, true),
		})
	if err != nil {
		t.Fatal(err)
	}

	return module
}

//nolint:paralleltest // SOFTHSM2_CONF is process-wide.
func TestSignWithSoftHSM(t *testing.T) {
	module := newSoftHSM(t)

	tok, err := openToken(module, tokenLabel, userPIN, keyLabel)
	if err != nil {
		t.Fatal(err)
	}

	defer tok.close()

	publicKey, err := tok.publicKey()
	if err != nil {
		t.Fatal(err)
	}

	in := strings.NewReader(`{"id":1,"fingerprints":{"a.narinfo":"` + base64.StdEncoding.EncodeToString([]byte("one")) + `","b.narinfo":"` + base64.StdEncoding.EncodeToString([]byte("two")) + `"}}
{"id":2,"fingerprints":{"a.narinfo":"` + base64.StdEncoding.EncodeToString([]byte("one")) + `"}}
`)

	var out bytes.Buffer
	if err := serve(in, &out, "test-key", tok.sign); err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(&out)
	messages := map[string]string{"a.narinfo": "one", "b.narinfo": "two"}

	for id := uint64(1); id <= 2; id++ {
		var resp response
		if err := dec.Decode(&resp); err != nil {
			t.Fatal(err)
		}

		if resp.ID != id {
			t.Fatalf("response id = %d, want %d", resp.ID, id)
		}

		for objectKey, signature := range resp.Signatures {
			name, encoded, _ := strings.Cut(signature, ":")
			raw, err := base64.StdEncoding.DecodeString(encoded)

			if name != "test-key" || err != nil || !ed25519.Verify(publicKey, []byte(messages[objectKey]), raw) {
				t.Errorf("request %d, %s: invalid signature %q (%v)", id, objectKey, signature, err)
			}
		}
	}
}

//nolint:paralleltest // SOFTHSM2_CONF is process-wide.
func TestOpenTokenErrors(t *testing.T) {
	module := newSoftHSM(t)

	if _, err := openToken(module, "nonexistent", userPIN, keyLabel); err == nil {
		t.Error("unknown token label succeeded")
	}

	if _, err := openToken(module, tokenLabel, "wrong", keyLabel); err == nil {
		t.Error("wrong PIN succeeded")
	}

	tok, err := openToken(module, tokenLabel, userPIN, "nonexistent")
	if err != nil {
		t.Fatal(err)
	}

	defer tok.close()

	if _, err := tok.sign([]byte("x")); err == nil {
		t.Error("signing with an unknown key succeeded")
	}
}
