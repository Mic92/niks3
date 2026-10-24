package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Mic92/niks3/client/oidclogin"
	"github.com/Mic92/niks3/cmdutil"
)

const authLogin = "login"

func printAuthHelp(w io.Writer) {
	fmt.Fprintln(w, `Usage: niks3 auth <login|status|logout> [flags]

  --server-url string       Server URL (or NIKS3_SERVER_URL)

Login only:
  --device                  Explicitly authorize with a device code (required)
  --oidc-issuer string       OIDC issuer URL (required)
  --oidc-client-id string    Public OIDC client ID (required)
  --oidc-scopes string       Space-separated scopes (default: "openid profile offline_access")

Login saves tokens in Linux Secret Service or macOS Keychain and selects a
fallback profile for this server. Unlock your keyring in the desktop session first.
Status prints local metadata as JSON without refreshing or printing tokens.
Logout removes local credentials; it does not revoke the provider's grant.
For headless sessions without a keyring, use push --oidc-device instead.`)
}

func authCommand(args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return runAuth(ctx, args, os.Stdout, os.Stderr)
}

func runAuth(ctx context.Context, args []string, output, instructions io.Writer) error {
	if len(args) == 0 {
		printAuthHelp(instructions)

		return errors.New("missing auth subcommand (login, status or logout)")
	}
	if args[0] == "--help" || args[0] == "-h" {
		printAuthHelp(instructions)

		return nil
	}
	action := args[0]
	if action != authLogin && action != "status" && action != "logout" {
		return errors.New("unknown auth subcommand (use login, status or logout)")
	}
	fs := flag.NewFlagSet("auth "+action, flag.ContinueOnError)
	fs.SetOutput(instructions)
	fs.Usage = func() { printAuthHelp(instructions) }
	server := fs.String("server-url", os.Getenv("NIKS3_SERVER_URL"), "Server URL")
	var device bool
	var issuer, clientID, scopes string
	if action == authLogin {
		fs.BoolVar(&device, "device", false, "Explicit device authorization")
		fs.StringVar(&issuer, "oidc-issuer", "", "OIDC issuer URL")
		fs.StringVar(&clientID, "oidc-client-id", "", "Public OIDC client ID")
		fs.StringVar(&scopes, "oidc-scopes", "openid profile offline_access", "Space-separated OIDC scopes")
	}
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}

		return fmt.Errorf("parsing auth flags: %w", err)
	}
	if fs.NArg() != 0 {
		return errors.New("auth commands do not accept positional arguments")
	}
	if err := cmdutil.RequireServerURL(*server); err != nil {
		return err //nolint:wrapcheck // cmdutil errors are already user-facing
	}
	if action == authLogin && (!device || issuer == "" || clientID == "") {
		return errors.New("login requires --device, --oidc-issuer and --oidc-client-id")
	}
	manager, err := oidclogin.DefaultManager()
	if err != nil {
		return fmt.Errorf("managing OIDC login: %w", err)
	}
	switch action {
	case authLogin:
		if err := manager.Login(ctx, *server, oidclogin.Config{Issuer: issuer, ClientID: clientID, Scopes: strings.Fields(scopes)}, instructions); err != nil {
			return fmt.Errorf("managing OIDC login: %w", err)
		}
		_, err = fmt.Fprintln(output, "OIDC login saved for this server")
	case "status":
		status, statusErr := manager.Status(ctx, *server)
		if statusErr != nil {
			return fmt.Errorf("reading OIDC status: %w", statusErr)
		}
		err = json.NewEncoder(output).Encode(status)
	case "logout":
		if err := manager.Logout(ctx, *server); err != nil {
			return fmt.Errorf("managing OIDC login: %w", err)
		}
		_, err = fmt.Fprintln(output, "Local OIDC login removed")
	}
	if err != nil {
		return fmt.Errorf("writing auth result: %w", err)
	}

	return nil
}
