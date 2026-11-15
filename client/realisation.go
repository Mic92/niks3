package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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

// floatingOutputRe matches a floating CA output in a derivation's ATerm:
// ("name","","hashAlgo","").
var floatingOutputRe = regexp.MustCompile(`\("([^"]+)","","[^"]+",""\)`)

// floatingOutputs returns the floating CA outputs of the derivation drv and
// whether drv could be read.
func floatingOutputs(drv string) ([]string, bool) {
	content, err := os.ReadFile(drv)
	if err != nil {
		return nil, false
	}

	outputs, _, _ := bytes.Cut(content, []byte("],"))

	var names []string
	for _, m := range floatingOutputRe.FindAllSubmatch(outputs, -1) {
		names = append(names, string(m[1]))
	}

	return names, true
}

// floatingInstallable narrows <drv>^<outputs> to the floating CA outputs
// among them, or returns "" if there are none: input-addressed and
// fixed-output derivations have no realisation to push, and Nix < 2.35
// aborts when asked about a fixed output. An unreadable drv is kept as is.
func floatingInstallable(installable string) string {
	drv, spec, _ := strings.Cut(installable, "^")

	floating, readable := floatingOutputs(drv)
	if !readable {
		return installable
	}

	if spec != "*" {
		wanted := strings.Split(spec, ",")
		floating = slices.DeleteFunc(floating, func(o string) bool { return !slices.Contains(wanted, o) })
	}

	if len(floating) == 0 {
		return ""
	}

	return drv + "^" + strings.Join(floating, ",")
}

// fixedOutputRe matches an output with a path in a derivation's ATerm,
// input-addressed or fixed-output: ("name","/nix/store/...",...).
var fixedOutputRe = regexp.MustCompile(`\("([^"]+)","(/[^"]+)",`)

// withFixedOutputs adds to paths the outputs named by <drv>^<outputs> that
// the derivations fix, input-addressed or fixed-output. They have no
// realisation, so pushing the derivation means pushing them, as nix copy
// does. Outputs that are not in the store are skipped with a warning;
// unreadable derivations are skipped.
func withFixedOutputs(paths, installables []string) []string {
	for _, installable := range installables {
		drv, spec, _ := strings.Cut(installable, "^")

		content, err := os.ReadFile(drv)
		if err != nil {
			continue
		}

		outputs, _, _ := bytes.Cut(content, []byte("],"))
		wanted := strings.Split(spec, ",")

		for _, m := range fixedOutputRe.FindAllSubmatch(outputs, -1) {
			name, path := string(m[1]), string(m[2])
			if spec != "*" && !slices.Contains(wanted, name) {
				continue
			}

			if _, err := os.Lstat(path); err != nil { //nolint:gosec // G703: an output path of a .drv the caller named
				slog.Warn("Not pushing output: it is not in the store", "output", installable, "path", path)

				continue
			}

			if !slices.Contains(paths, path) {
				paths = append(paths, path)
			}
		}
	}

	return paths
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

// queryRealisations returns the realisations of the derivers of pathInfos
// and named, dropping build traces for servers that reject them.
func (c *Client) queryRealisations(
	ctx context.Context,
	pathInfos map[string]*PathInfo,
	named map[string]*RealisationInfo,
) map[string]*RealisationInfo {
	realisations, err := QueryRealisations(ctx, pathInfos, c.NixEnv)
	if err != nil {
		slog.Warn("Failed to query realisations, pushing CA paths without them", "error", err)

		realisations = make(map[string]*RealisationInfo, len(named))
	}

	maps.Copy(realisations, named)

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

// splitDerivationOutputs separates derivation outputs, written
// <drv>^<outputs> as Nix does, from store paths.
func splitDerivationOutputs(args []string) ([]string, []string) {
	var paths, outputs []string

	for _, arg := range args {
		if drv, _, found := strings.Cut(arg, "^"); found && strings.HasSuffix(drv, ".drv") {
			outputs = append(outputs, arg)
		} else {
			paths = append(paths, arg)
		}
	}

	return paths, outputs
}

// namedRealisations queries the realisations of derivation outputs named as
// <drv>^<outputs>. A store path records one deriver, the first derivation
// that built it; a changed derivation that builds the same output (early
// cutoff) has a realisation of its own, found only by its name.
//
// Only realisations of the named derivations are kept: for an unresolved
// derivation Nix resolves through the realisations of all its inputs and
// reports those too. The store records realisations of resolved derivations,
// so these are the ones to name, as a post-build hook's DRV_PATH does.
//
// It returns the realisations by cache key and the store paths of their
// outputs, leaving out outputs that are not in the store.
func (c *Client) namedRealisations(ctx context.Context, installables []string) (map[string]*RealisationInfo, []string, error) {
	if len(installables) == 0 {
		return nil, nil, nil
	}

	// Older Nix keys realisations by hash, so only the new format can be
	// matched to a name. The outputs are in the derivations' store.
	named := make(map[string]bool, len(installables))
	storeDir := c.storeDir

	floating := make([]string, 0, len(installables))

	for _, installable := range installables {
		drv, _, _ := strings.Cut(installable, "^")
		named[filepath.Base(drv)] = true
		storeDir = filepath.Dir(drv)

		if f := floatingInstallable(installable); f != "" {
			floating = append(floating, f)
		}
	}

	realisations := make(map[string]*RealisationInfo)

	var outPaths []string

	// Stay below ARG_MAX.
	const maxPerChunk = 1000
	for chunk := range slices.Chunk(floating, maxPerChunk) {
		found, err := queryRealisationInfo(ctx, chunk, c.NixEnv)
		if err != nil {
			return nil, nil, err
		}

		for i := range found {
			r := &found[i]
			if r.ID == "" && !named[r.DrvPath] {
				continue
			}

			outPath := filepath.Join(storeDir, r.OutPath)
			if _, err := os.Lstat(outPath); err != nil {
				slog.Warn("Not pushing realisation: its output is not in the store", "key", r.Key(), "output", outPath)

				continue
			}

			if _, dup := realisations[r.Key()]; !dup {
				outPaths = append(outPaths, outPath)
			}

			realisations[r.Key()] = r
		}
	}

	return realisations, outPaths, nil
}
