package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// ghaAppendFile appends a key=value entry to a GitHub Actions environment file
// ($GITHUB_ENV, $GITHUB_STATE, $GITHUB_OUTPUT). Multiline values use the
// heredoc delimiter syntax GitHub documents for these files.
//
// Outside of GitHub Actions (env file path unset), it silently does nothing —
// letting `niks3 ci` subcommands be exercised locally.
func ghaAppendFile(envVar, key, value string) error {
	path := os.Getenv(envVar)
	if path == "" {
		return nil
	}

	// Path is set by the GitHub Actions runtime; trusted like the rest of
	// the runner environment.
	//nolint:gosec // G703: trusted GHA runtime path, not user input
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("opening %s: %w", envVar, err)
	}

	defer func() { _ = f.Close() }()

	var line string

	if strings.ContainsRune(value, '\n') {
		// Random delimiter so the value can't close the heredoc early.
		delim := "EOF_" + randHex(8)
		line = fmt.Sprintf("%s<<%s\n%s\n%s\n", key, delim, value, delim)
	} else {
		line = fmt.Sprintf("%s=%s\n", key, value)
	}

	if _, err := f.WriteString(line); err != nil {
		return fmt.Errorf("writing %s: %w", envVar, err)
	}

	return nil
}

// ghaSetEnv exports an environment variable to subsequent steps in the job.
func ghaSetEnv(key, value string) error {
	return ghaAppendFile("GITHUB_ENV", key, value)
}

// ghaSetState saves a value readable in the post step as $STATE_<key>.
func ghaSetState(key, value string) error {
	return ghaAppendFile("GITHUB_STATE", key, value)
}

// ghaGetState reads a value saved by ghaSetState in the main step.
func ghaGetState(key string) string {
	return os.Getenv("STATE_" + key)
}

// ghaGetInput reads an action input. GitHub exposes inputs as INPUT_<NAME>
// with the name uppercased and dashes replaced by underscores.
func ghaGetInput(name string) string {
	key := "INPUT_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))

	return strings.TrimSpace(os.Getenv(key))
}

// ghaGetInputBool reads a boolean action input ("true" / "false").
func ghaGetInputBool(name string) bool {
	return ghaGetInput(name) == "true"
}

// Workflow commands must go to stdout for the runner to parse them.
func ghaCommand(cmd, data string) {
	_, _ = fmt.Fprintf(os.Stdout, "::%s::%s\n", cmd, escapeGHAData(data))
}

// ghaWarningf emits a ::warning:: workflow command. The message appears in the
// job summary and as an annotation.
func ghaWarningf(format string, args ...any) {
	ghaCommand("warning", fmt.Sprintf(format, args...))
}

// ghaNoticef emits a ::notice:: workflow command.
func ghaNoticef(format string, args ...any) {
	ghaCommand("notice", fmt.Sprintf(format, args...))
}

// ghaGroup wraps fn's output in a collapsible log group.
func ghaGroup(title string, fn func() error) error {
	ghaCommand("group", title)
	defer ghaCommand("endgroup", "")

	return fn()
}

// escapeGHAData escapes a workflow command's data field. % and \r and \n
// are the only characters that need escaping per the runner's parser.
func escapeGHAData(s string) string {
	s = strings.ReplaceAll(s, "%", "%25")
	s = strings.ReplaceAll(s, "\r", "%0D")
	s = strings.ReplaceAll(s, "\n", "%0A")

	return s
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}
