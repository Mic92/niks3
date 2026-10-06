// niks3-pkcs11-signer is an example --sign-program that signs narinfo
// fingerprints with an Ed25519 key on a PKCS#11 token such as an HSM or
// SoftHSM. The private key never leaves the token.
//
// It reads one JSON request per line from stdin and writes one JSON response
// per line to stdout:
//
//	{"id":1,"fingerprints":{"object-key":"base64-fingerprint"}}
//	{"id":1,"signatures":{"object-key":"name:base64-signature"}}
//
// Any token failure ends the process. niks3 then starts a fresh one, which
// opens a new session.
//
// Run it with -print-public-key to get the value for niks3's public key option.
package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/miekg/pkcs11"
)

// Newer than the constants miekg/pkcs11 ships. Pure Ed25519 takes no parameters.
const ckmEDDSA = 0x1057

type request struct {
	ID           uint64            `json:"id"`
	Fingerprints map[string][]byte `json:"fingerprints"`
}

type response struct {
	ID         uint64            `json:"id"`
	Signatures map[string]string `json:"signatures"`
}

// token is a logged-in session with the signing key.
type token struct {
	ctx     *pkcs11.Ctx
	session pkcs11.SessionHandle
	label   string
}

func main() {
	if err := run(); err != nil {
		slog.Error("niks3-pkcs11-signer failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	module := flag.String("module", "", "path to the PKCS#11 module, for example libsofthsm2.so")
	tokenLabel := flag.String("token", "", "label of the token")
	pinFile := flag.String("pin-file", "", "file containing the user PIN")
	keyLabel := flag.String("key", "", "label of the Ed25519 key pair")
	name := flag.String("signature-name", "", "key name in signatures, must match the public key given to niks3")
	printKey := flag.Bool("print-public-key", false, "print the public key as name:base64 and exit")
	flag.Parse()

	if *module == "" || *tokenLabel == "" || *pinFile == "" || *keyLabel == "" || *name == "" {
		return errors.New("-module, -token, -pin-file, -key and -signature-name are required")
	}

	pin, err := os.ReadFile(*pinFile)
	if err != nil {
		return fmt.Errorf("reading PIN: %w", err)
	}

	t, err := openToken(*module, *tokenLabel, strings.TrimSpace(string(pin)), *keyLabel)
	if err != nil {
		return err
	}

	defer t.close()

	if *printKey {
		key, err := t.publicKey()
		if err != nil {
			return err
		}

		if _, err := fmt.Fprintf(os.Stdout, "%s:%s\n", *name, base64.StdEncoding.EncodeToString(key)); err != nil {
			return fmt.Errorf("printing public key: %w", err)
		}

		return nil
	}

	if err := serve(os.Stdin, os.Stdout, *name, t.sign); err != nil {
		return fmt.Errorf("serving signing requests: %w", err)
	}

	return nil
}

func findToken(ctx *pkcs11.Ctx, label string) (uint, error) {
	slots, err := ctx.GetSlotList(true)
	if err != nil {
		return 0, fmt.Errorf("listing slots: %w", err)
	}

	for _, slot := range slots {
		info, err := ctx.GetTokenInfo(slot)
		if err == nil && strings.TrimSpace(info.Label) == label {
			return slot, nil
		}
	}

	return 0, fmt.Errorf("no token labeled %q", label)
}

func openToken(module, label, pin, keyLabel string) (*token, error) {
	ctx := pkcs11.New(module)
	if ctx == nil {
		return nil, fmt.Errorf("loading PKCS#11 module %s", module)
	}

	if err := ctx.Initialize(); err != nil {
		return nil, fmt.Errorf("initializing module: %w", err)
	}

	t := &token{ctx: ctx, label: keyLabel}

	slot, err := findToken(ctx, label)
	if err != nil {
		t.close()

		return nil, err
	}

	t.session, err = ctx.OpenSession(slot, pkcs11.CKF_SERIAL_SESSION)
	if err != nil {
		t.close()

		return nil, fmt.Errorf("opening session: %w", err)
	}

	if err := ctx.Login(t.session, pkcs11.CKU_USER, pin); err != nil {
		t.close()

		return nil, fmt.Errorf("logging in: %w", err)
	}

	return t, nil
}

func (t *token) close() {
	_ = t.ctx.Finalize()
	t.ctx.Destroy()
}

func (t *token) findKey(class uint) (pkcs11.ObjectHandle, error) {
	template := []*pkcs11.Attribute{
		pkcs11.NewAttribute(pkcs11.CKA_CLASS, class),
		pkcs11.NewAttribute(pkcs11.CKA_LABEL, t.label),
	}
	if err := t.ctx.FindObjectsInit(t.session, template); err != nil {
		return 0, fmt.Errorf("searching for key: %w", err)
	}

	defer func() { _ = t.ctx.FindObjectsFinal(t.session) }()

	objects, _, err := t.ctx.FindObjects(t.session, 1)
	if err != nil {
		return 0, fmt.Errorf("searching for key: %w", err)
	}

	if len(objects) == 0 {
		return 0, fmt.Errorf("no key labeled %q", t.label)
	}

	return objects[0], nil
}

// publicKey returns the raw 32 byte Ed25519 public key.
func (t *token) publicKey() ([]byte, error) {
	object, err := t.findKey(pkcs11.CKO_PUBLIC_KEY)
	if err != nil {
		return nil, err
	}

	attrs, err := t.ctx.GetAttributeValue(t.session, object, []*pkcs11.Attribute{pkcs11.NewAttribute(pkcs11.CKA_EC_POINT, nil)})
	if err != nil {
		return nil, fmt.Errorf("reading public key: %w", err)
	}

	// The point is a DER OCTET STRING around the key. Some modules omit the wrapper.
	point := attrs[0].Value
	if len(point) == 34 && point[0] == 0x04 && point[1] == 0x20 {
		point = point[2:]
	}

	if len(point) != 32 {
		return nil, fmt.Errorf("unexpected public key length %d", len(point))
	}

	return point, nil
}

func (t *token) sign(fingerprint []byte) ([]byte, error) {
	object, err := t.findKey(pkcs11.CKO_PRIVATE_KEY)
	if err != nil {
		return nil, err
	}

	if err := t.ctx.SignInit(t.session, []*pkcs11.Mechanism{pkcs11.NewMechanism(ckmEDDSA, nil)}, object); err != nil {
		return nil, fmt.Errorf("starting signature: %w", err)
	}

	signature, err := t.ctx.Sign(t.session, fingerprint)
	if err != nil {
		return nil, fmt.Errorf("signing: %w", err)
	}

	return signature, nil
}

// serve answers requests until stdin closes.
func serve(in io.Reader, out io.Writer, keyName string, sign func(fingerprint []byte) ([]byte, error)) error {
	dec := json.NewDecoder(in)
	enc := json.NewEncoder(out)

	for {
		var req request
		if err := dec.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return fmt.Errorf("reading request: %w", err)
		}

		resp := response{ID: req.ID, Signatures: make(map[string]string, len(req.Fingerprints))}

		for objectKey, fingerprint := range req.Fingerprints {
			signature, err := sign(fingerprint)
			if err != nil {
				return fmt.Errorf("narinfo %q: %w", objectKey, err)
			}

			resp.Signatures[objectKey] = keyName + ":" + base64.StdEncoding.EncodeToString(signature)
		}

		if err := enc.Encode(resp); err != nil {
			return fmt.Errorf("writing response: %w", err)
		}
	}
}
