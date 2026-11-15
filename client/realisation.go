package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

// RealisationInfo maps a CA derivation output to its store path.
// Nix >= 2.35 keys it by DrvPath and OutputName, older Nix by ID.
// Store paths are base names.
type RealisationInfo struct {
	DrvPath               string
	OutputName            string
	ID                    string
	OutPath               string
	Signatures            []string // "name:base64-signature"
	DependentRealisations map[string]string
}

// Key returns the binary cache key Nix reads the realisation from.
func (r *RealisationInfo) Key() string {
	if r.ID != "" {
		return "realisations/" + r.ID + ".doi"
	}

	return "build-trace-v2/" + r.DrvPath + "/" + r.OutputName + ".doi"
}

type nixSignature struct {
	KeyName string `json:"keyName"` //nolint:tagliatelle // Nix's JSON format
	Sig     string `json:"sig"`
}

// FileContent returns the .doi file with the given signatures.
func (r *RealisationInfo) FileContent(signatures []string) ([]byte, error) {
	// Nix < 2.35 rejects null.
	sigs := append([]string{}, slices.Compact(slices.Sorted(slices.Values(signatures)))...)

	if r.ID != "" {
		deps := r.DependentRealisations
		if deps == nil {
			deps = map[string]string{}
		}

		return marshalNix(map[string]any{
			"id":                    r.ID,
			"outPath":               r.OutPath,
			"signatures":            sigs,
			"dependentRealisations": deps,
		})
	}

	objects := make([]nixSignature, 0, len(sigs))
	for _, sig := range sigs {
		name, value, found := strings.Cut(sig, ":")
		if !found {
			return nil, fmt.Errorf("invalid signature %q", sig)
		}
		objects = append(objects, nixSignature{KeyName: name, Sig: value})
	}

	return marshalNix(map[string]any{"outPath": r.OutPath, "signatures": objects})
}

// metadata returns what the server needs to sign the realisation.
func (r *RealisationInfo) metadata() RealisationMetadata {
	return RealisationMetadata{
		DrvPath:               r.DrvPath,
		OutputName:            r.OutputName,
		ID:                    r.ID,
		OutPath:               r.OutPath,
		DependentRealisations: r.DependentRealisations,
	}
}

func marshalNix(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("encoding realisation: %w", err)
	}

	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// parseSignatures accepts "name:base64" strings and {keyName, sig} objects.
func parseSignatures(raw []json.RawMessage) ([]string, error) {
	sigs := make([]string, 0, len(raw))
	for _, r := range raw {
		var s string
		if err := json.Unmarshal(r, &s); err == nil {
			sigs = append(sigs, s)

			continue
		}
		var o nixSignature
		if err := json.Unmarshal(r, &o); err != nil || o.KeyName == "" {
			return nil, fmt.Errorf("invalid signature %s", r)
		}
		if _, err := base64.StdEncoding.DecodeString(o.Sig); err != nil {
			return nil, fmt.Errorf("invalid signature %s: %w", r, err)
		}
		sigs = append(sigs, o.KeyName+":"+o.Sig)
	}

	return sigs, nil
}

// parseRealisationsJSON parses `nix realisation info --json`, skipping opaque paths.
func parseRealisationsJSON(output []byte) ([]RealisationInfo, error) {
	type entry struct {
		// Nix >= 2.35
		Key *struct {
			DrvPath    string `json:"drvPath"`    //nolint:tagliatelle // Nix's JSON format
			OutputName string `json:"outputName"` //nolint:tagliatelle // Nix's JSON format
		} `json:"key"`
		Value *struct {
			OutPath    string            `json:"outPath"` //nolint:tagliatelle // Nix's JSON format
			Signatures []json.RawMessage `json:"signatures"`
		} `json:"value"`

		// Nix < 2.35
		ID                    string            `json:"id"`
		OutPath               string            `json:"outPath"` //nolint:tagliatelle // Nix's JSON format
		Signatures            []json.RawMessage `json:"signatures"`
		DependentRealisations map[string]string `json:"dependentRealisations"` //nolint:tagliatelle // Nix's JSON format
	}

	var entries []entry
	if err := json.Unmarshal(output, &entries); err != nil {
		return nil, fmt.Errorf("parsing realisation info: %w", err)
	}

	var result []RealisationInfo
	for _, e := range entries {
		var r RealisationInfo
		var rawSigs []json.RawMessage
		switch {
		case e.Key != nil && e.Value != nil:
			r = RealisationInfo{DrvPath: e.Key.DrvPath, OutputName: e.Key.OutputName, OutPath: e.Value.OutPath}
			rawSigs = e.Value.Signatures
		case e.ID != "":
			r = RealisationInfo{ID: e.ID, OutPath: e.OutPath, DependentRealisations: e.DependentRealisations}
			rawSigs = e.Signatures
		default:
			continue
		}
		sigs, err := parseSignatures(rawSigs)
		if err != nil {
			return nil, fmt.Errorf("parsing realisation %s: %w", r.Key(), err)
		}
		r.Signatures = sigs
		result = append(result, r)
	}

	return result, nil
}

// storeName strips the store directory and hash from a store path.
func storeName(path string) string {
	_, name, _ := strings.Cut(filepath.Base(path), "-")

	return name
}

// realisationInstallable returns "<deriver>^<output>" for path, or "".
// Output "out" is named <name>, others <name>-<output>.
func realisationInstallable(path, deriver string) string {
	drvName, ok := strings.CutSuffix(storeName(deriver), ".drv")
	if !ok || drvName == "" {
		return ""
	}
	name := storeName(path)
	output := "out"
	if name != drvName {
		output, ok = strings.CutPrefix(name, drvName+"-")
		if !ok || output == "" {
			return ""
		}
	}

	return deriver + "^" + output
}

// isFloatingOutput reports whether output has no fixed path in the drv,
// i.e. ("name","",...) in its ATerm. Nix < 2.35 aborts when asked for
// fixed outputs. Unreadable drvs count as floating.
func isFloatingOutput(deriver, output string) bool {
	content, err := os.ReadFile(deriver)
	if err != nil {
		return true
	}
	outputs, _, _ := bytes.Cut(content, []byte("],"))

	return bytes.Contains(outputs, []byte(`("`+output+`","",`))
}

// QueryRealisations returns the realisations of floating CA outputs in
// pathInfos, keyed by cache key. Nix finds them by <deriver>^<output> only.
func QueryRealisations(ctx context.Context, pathInfos map[string]*PathInfo, nixEnv []string) (map[string]*RealisationInfo, error) {
	var installables []string
	for _, info := range pathInfos {
		if info.CA == nil || info.CA.String() == "" || info.Deriver == nil || *info.Deriver == "" {
			continue
		}
		installable := realisationInstallable(info.Path, *info.Deriver)
		if installable == "" {
			continue
		}
		drv, output, _ := strings.Cut(installable, "^")
		if isFloatingOutput(drv, output) {
			installables = append(installables, installable)
		}
	}
	slices.Sort(installables)

	result := make(map[string]*RealisationInfo)

	// Stay below ARG_MAX.
	const maxPerChunk = 1000
	for chunk := range slices.Chunk(installables, maxPerChunk) {
		realisations, err := queryRealisationInfo(ctx, chunk, nixEnv)
		if err != nil {
			return nil, err
		}
		for i := range realisations {
			result[realisations[i].Key()] = &realisations[i]
		}
	}

	return result, nil
}

// queryRealisationInfo queries installables. One missing realisation fails
// the call, so it bisects on failure and skips single failures.
func queryRealisationInfo(ctx context.Context, installables []string, nixEnv []string) ([]RealisationInfo, error) {
	args := append([]string{"--extra-experimental-features", "nix-command ca-derivations", "realisation", "info", "--json", "--"}, installables...)

	cmd := exec.CommandContext(ctx, "nix", args...)
	if len(nixEnv) > 0 {
		cmd.Env = nixEnv
	}

	output, err := cmd.Output()
	if err == nil {
		return parseRealisationsJSON(output)
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("querying realisations: %w", ctx.Err())
	}
	if len(installables) == 1 {
		var stderr []byte
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			stderr = exitErr.Stderr
		}
		slog.Debug("No realisation", "output", installables[0], "error", err, "stderr", string(stderr))

		return nil, nil
	}

	half := len(installables) / 2
	first, err := queryRealisationInfo(ctx, installables[:half], nixEnv)
	if err != nil {
		return nil, err
	}
	second, err := queryRealisationInfo(ctx, installables[half:], nixEnv)
	if err != nil {
		return nil, err
	}

	return append(first, second...), nil
}

// queryRealisations drops build traces for servers that reject them.
func (c *Client) queryRealisations(ctx context.Context, pathInfos map[string]*PathInfo) map[string]*RealisationInfo {
	realisations, err := QueryRealisations(ctx, pathInfos, c.NixEnv)
	if err != nil {
		slog.Warn("Failed to query realisations, pushing CA paths without them", "error", err)

		return nil
	}

	if cfg, err := c.GetCacheConfig(ctx); err != nil || !cfg.BuildTraces {
		for key, r := range realisations {
			if r.ID == "" {
				slog.Warn("Server does not accept build traces, not pushing realisation", "key", key)
				delete(realisations, key)
			}
		}
	}

	return realisations
}
