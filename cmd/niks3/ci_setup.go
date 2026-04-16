package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Mic92/niks3/client"
)

// githubIssuer is GitHub Actions' OIDC issuer URL — a well-known constant.
const githubIssuer = "https://token.actions.githubusercontent.com"

// setupConfig aggregates action inputs and server-fetched defaults.
type setupConfig struct {
	serverURL   string
	substituter string
	publicKeys  []string
	audience    string
	skipPush    bool
	workDir     string
	debug       bool
}

// runCISetup is the action's main step. It fetches cache config from the
// server, writes a nix.conf snippet, picks a push mode (daemon / storescan /
// none), and saves state for the post step.
func runCISetup(workDir string) error {
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return fmt.Errorf("creating work dir: %w", err)
	}

	cfg, err := resolveConfig(workDir)
	if err != nil {
		return err
	}

	if err := writeNixConf(cfg); err != nil {
		return err
	}

	mode := pickMode(cfg)
	slog.Info("Push mode selected", "mode", mode)

	switch mode {
	case modeDaemon:
		if err := startDaemon(cfg); err != nil {
			return err
		}

	case modeStorescan:
		snapshot := filepath.Join(cfg.workDir, "store-pre")
		if err := storescanSnapshot(snapshot); err != nil {
			return err
		}

		_ = ghaSetState(stateKeySnapshot, snapshot)
		_ = ghaSetState(stateKeyServerURL, cfg.serverURL)
		_ = ghaSetState(stateKeyAudience, cfg.audience)

	case modeNone:
		// Substituter configured, nothing to push. Common for fork PRs.
	}

	_ = ghaSetState(stateKeyMode, mode)

	return nil
}

// resolveConfig reads action inputs and fills gaps from /api/cache-config.
// Inputs always win over server-provided defaults.
func resolveConfig(workDir string) (setupConfig, error) {
	cfg := setupConfig{
		serverURL:   ghaGetInput("server-url"),
		substituter: ghaGetInput("substituter"),
		audience:    ghaGetInput("audience"),
		skipPush:    ghaGetInputBool("skip-push"),
		workDir:     workDir,
		debug:       ghaGetInputBool("debug"),
	}

	if pk := ghaGetInput("public-key"); pk != "" {
		cfg.publicKeys = strings.Fields(pk)
	}

	if cfg.serverURL == "" {
		return cfg, errors.New("server-url input is required")
	}

	// Only hit the server if something is missing.
	need := cfg.substituter == "" || len(cfg.publicKeys) == 0 || cfg.audience == ""
	if !need {
		return cfg, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fetched, err := client.FetchCacheConfig(ctx, cfg.serverURL, githubIssuer)
	if err != nil {
		// Not fatal if the user provided everything we actually need for
		// the chosen mode. Warn and continue; downstream validation will
		// catch truly missing pieces.
		ghaWarningf("could not fetch cache-config from server: %v", err)

		return cfg, nil
	}

	if cfg.substituter == "" {
		cfg.substituter = fetched.SubstituterURL
	}

	if len(cfg.publicKeys) == 0 {
		cfg.publicKeys = fetched.PublicKeys
	}

	if cfg.audience == "" {
		cfg.audience = fetched.OIDCAudience
	}

	return cfg, nil
}

// writeNixConf writes a nix.conf snippet and registers it via
// NIX_USER_CONF_FILES so every subsequent `nix` invocation picks it up
// without a daemon restart. The post-build-hook line is only added once
// the daemon mode is confirmed, so we write it in startDaemon instead.
func writeNixConf(cfg setupConfig) error {
	var b strings.Builder

	if cfg.substituter != "" {
		fmt.Fprintf(&b, "extra-substituters = %s\n", cfg.substituter)
	}

	if len(cfg.publicKeys) > 0 {
		fmt.Fprintf(&b, "extra-trusted-public-keys = %s\n", strings.Join(cfg.publicKeys, " "))
	}

	confPath := filepath.Join(cfg.workDir, "nix.conf")

	// nix.conf must be world-readable: the nix daemon reads it as a different
	// user than the runner.
	//nolint:gosec // G306: intentional 0644
	if err := os.WriteFile(confPath, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("writing nix.conf: %w", err)
	}

	// Prepend our conf to the existing list. Nix reads NIX_USER_CONF_FILES
	// colon-separated, in order. If unset it falls back to XDG paths — we
	// include those explicitly so we don't mask a ~/.config/nix/nix.conf.
	existing := os.Getenv("NIX_USER_CONF_FILES")
	if existing == "" {
		existing = defaultUserConfFiles()
	}

	return ghaSetEnv("NIX_USER_CONF_FILES", confPath+":"+existing)
}

// defaultUserConfFiles replicates Nix's default NIX_USER_CONF_FILES search
// path (XDG_CONFIG_HOME + XDG_CONFIG_DIRS, each /nix/nix.conf).
func defaultUserConfFiles() string {
	home := os.Getenv("XDG_CONFIG_HOME")
	if home == "" {
		if h, err := os.UserHomeDir(); err == nil {
			home = filepath.Join(h, ".config")
		}
	}

	dirs := os.Getenv("XDG_CONFIG_DIRS")
	if dirs == "" {
		dirs = "/etc/xdg"
	}

	parts := append([]string{home}, strings.Split(dirs, ":")...)
	for i, p := range parts {
		parts[i] = filepath.Join(p, "nix", "nix.conf")
	}

	return strings.Join(parts, ":")
}

// pickMode decides daemon vs storescan vs none.
//
//	none:      skip-push set, OR no OIDC available (fork PR)
//	storescan: OIDC available but runner user is not in trusted-users
//	daemon:    OIDC available and user is trusted (the happy path)
func pickMode(cfg setupConfig) string {
	if cfg.skipPush {
		slog.Info("Skip-push set; configuring substituter only")

		return modeNone
	}

	if os.Getenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN") == "" {
		slog.Info("No OIDC token available (fork PR or missing id-token:write); configuring substituter only")

		return modeNone
	}

	if cfg.audience == "" {
		ghaWarningf("no OIDC audience configured — set the 'audience' input or configure an OIDC provider with issuer %s on the server", githubIssuer)

		return modeNone
	}

	if !isTrustedUser() {
		ghaWarningf("runner user is not in Nix trusted-users; falling back to store-scan push (intermediate derivations won't be cached on build failure)")

		return modeStorescan
	}

	return modeDaemon
}

// isTrustedUser reports whether the current user can set post-build-hook.
// A trusted user (or a writable store, i.e. single-user Nix) is required
// for the hook to actually fire.
func isTrustedUser() bool {
	// Single-user install: the store is directly writable, no daemon, hooks
	// run unconditionally.
	if err := syscall.Access(storeDir, 2 /* W_OK */); err == nil {
		return true
	}

	u, err := user.Current()
	if err != nil {
		return false
	}

	groups, _ := u.GroupIds()
	groupNames := make(map[string]bool, len(groups))

	for _, gid := range groups {
		if g, err := user.LookupGroupId(gid); err == nil {
			groupNames[g.Name] = true
		}
	}

	for _, entry := range nixTrustedUsers() {
		if entry == u.Username || entry == "*" {
			return true
		}

		// @group syntax in nix.conf trusted-users.
		if strings.HasPrefix(entry, "@") && groupNames[entry[1:]] {
			return true
		}
	}

	return false
}

// nixTrustedUsers parses `nix show-config` output for the trusted-users line.
// Returns an empty slice if nix isn't available or the line is missing.
func nixTrustedUsers() []string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, "nix", "show-config").Output()
	if err != nil {
		return nil
	}

	for line := range strings.SplitSeq(string(out), "\n") {
		if after, ok := strings.CutPrefix(line, "trusted-users = "); ok {
			return strings.Fields(after)
		}
	}

	return nil
}

// startDaemon writes the post-build-hook shim, appends the hook to nix.conf,
// and forks the daemon process (detached, own process group).
func startDaemon(cfg setupConfig) error {
	socket := socketPath(cfg.workDir)

	// The hook runs with a stripped env (only DRV_PATH + OUT_PATHS per
	// `man nix.conf`), so the shim bakes in the absolute binary path and
	// socket path resolved NOW, not at hook-exec time.
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolving own executable: %w", err)
	}

	hookPath := filepath.Join(cfg.workDir, "post-build-hook")
	hook := fmt.Sprintf("#!/bin/sh\nexec %q ci push --socket %q\n", self, socket)

	// The hook must be executable; 0755 is the minimum for nix to exec it.
	//nolint:gosec // G306: intentional 0755
	if err := os.WriteFile(hookPath, []byte(hook), 0o755); err != nil {
		return fmt.Errorf("writing hook shim: %w", err)
	}

	// Append post-build-hook to the nix.conf we already wrote.
	confPath := filepath.Join(cfg.workDir, "nix.conf")

	f, err := os.OpenFile(confPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("opening nix.conf for append: %w", err)
	}

	if _, err := fmt.Fprintf(f, "post-build-hook = %s\n", hookPath); err != nil {
		_ = f.Close()

		return fmt.Errorf("writing post-build-hook to nix.conf: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing nix.conf: %w", err)
	}

	// Fork the daemon. Setpgid gives it its own process group so the
	// post-step's SIGKILL-on-timeout can kill the whole tree, and so the
	// runner's step-end cleanup doesn't accidentally take it down early.
	logPath := filepath.Join(cfg.workDir, "daemon.log")

	logFile, err := os.Create(logPath)
	if err != nil {
		return fmt.Errorf("creating daemon log: %w", err)
	}

	args := []string{"ci", "daemon", "--socket", socket, "--server-url", cfg.serverURL, "--audience", cfg.audience}
	if cfg.debug {
		args = append(args, "--debug")
	}

	// The daemon is a long-lived detached child; a context here would cancel
	// it when setup returns, which is the opposite of what we want.
	//nolint:noctx // detached daemon, lifetime not bound to this function
	cmd := exec.Command(self, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		_ = logFile.Close()

		return fmt.Errorf("starting daemon: %w", err)
	}

	// The parent doesn't wait — daemon runs until the post step stops it.
	// Closing the log file here is fine; the child holds its own fd.
	_ = logFile.Close()

	slog.Info("Daemon started", "pid", cmd.Process.Pid, "socket", socket, "log", logPath)

	_ = ghaSetState(stateKeySocket, socket)
	_ = ghaSetState(stateKeyDaemonPID, strconv.Itoa(cmd.Process.Pid))
	_ = ghaSetState(stateKeyDaemonLog, logPath)

	return nil
}

// socketPath picks a Unix socket path. Unix socket paths are limited to 104
// (darwin) / 108 (linux) bytes including the null terminator. $RUNNER_TEMP
// on GitHub-hosted runners is short enough, but self-hosted runners can
// have deep home directories. Fall back to os.TempDir() if too long.
func socketPath(workDir string) string {
	limit := 108
	if runtime.GOOS == "darwin" {
		limit = 104
	}

	candidate := filepath.Join(workDir, "daemon.sock")
	if len(candidate) < limit {
		return candidate
	}

	return filepath.Join(os.TempDir(), "niks3-daemon.sock")
}
