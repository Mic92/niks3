package signing

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// Realisation maps a CA derivation output to its store path.
// Nix >= 2.35 keys it by DrvPath and OutputName, older Nix by ID.
// Store paths are base names.
type Realisation struct {
	DrvPath               string
	OutputName            string
	ID                    string
	OutPath               string
	DependentRealisations map[string]string
}

// IsBuildTrace reports whether r uses the format of Nix >= 2.35.
func (r *Realisation) IsBuildTrace() bool {
	return r.ID == ""
}

// Key returns the binary cache key Nix reads r from.
func (r *Realisation) Key() string {
	if r.IsBuildTrace() {
		return "build-trace-v2/" + r.DrvPath + "/" + r.OutputName + ".doi"
	}

	return "realisations/" + r.ID + ".doi"
}

func (r *Realisation) validate() error {
	if r.OutPath == "" || filepath.Base(r.OutPath) != r.OutPath {
		return fmt.Errorf("realisation out path must be a store path base name: %q", r.OutPath)
	}
	if !r.IsBuildTrace() {
		if r.DrvPath != "" || r.OutputName != "" {
			return errors.New("realisation must have either an id or a derivation path, not both")
		}
		if !strings.Contains(r.ID, "!") {
			return fmt.Errorf("realisation id has no output name: %q", r.ID)
		}

		return nil
	}
	if !strings.HasSuffix(r.DrvPath, ".drv") || filepath.Base(r.DrvPath) != r.DrvPath {
		return fmt.Errorf("realisation derivation must be a store path base name: %q", r.DrvPath)
	}
	if r.OutputName == "" || strings.ContainsAny(r.OutputName, "/!^") {
		return fmt.Errorf("invalid realisation output name: %q", r.OutputName)
	}
	if len(r.DependentRealisations) > 0 {
		return errors.New("build trace entries have no dependent realisations")
	}

	return nil
}

// GenerateRealisationFingerprint returns what Nix signs: compact JSON
// without signatures, keys sorted (as encoding/json sorts maps).
func GenerateRealisationFingerprint(r *Realisation) ([]byte, error) {
	if r == nil {
		return nil, errors.New("realisation cannot be nil")
	}
	if err := r.validate(); err != nil {
		return nil, err
	}

	var value any
	if r.IsBuildTrace() {
		value = map[string]any{
			"key":   map[string]string{"drvPath": r.DrvPath, "outputName": r.OutputName},
			"value": map[string]string{"outPath": r.OutPath},
		}
	} else {
		deps := r.DependentRealisations
		if deps == nil {
			deps = map[string]string{}
		}
		value = map[string]any{
			"dependentRealisations": deps,
			"id":                    r.ID,
			"outPath":               r.OutPath,
		}
	}

	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return nil, fmt.Errorf("encoding realisation fingerprint: %w", err)
	}

	return []byte(strings.TrimSuffix(b.String(), "\n")), nil
}
