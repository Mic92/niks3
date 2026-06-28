package server

import (
	"bytes"
	_ "embed"
	"fmt"
	"html/template"
	"strings"
)

//go:embed niks3.svg
var logoSVG string

//go:embed landing_page.html
var landingPageTemplate string

type landingPageData struct {
	Logo             template.HTML
	PublicKeys       []string
	PublicKeysJoined string
	CacheURL         string
}

// GenerateLandingPage creates an HTML landing page with the cache's public keys and usage instructions.
func (s *Service) GenerateLandingPage(cacheURL string) (string, error) {
	publicKeys := make([]string, 0, len(s.SigningKeys))
	for _, key := range s.SigningKeys {
		pubKey, err := key.PublicKey()
		if err != nil {
			return "", fmt.Errorf("failed to get public key: %w", err)
		}

		publicKeys = append(publicKeys, pubKey)
	}

	data := landingPageData{
		Logo:             template.HTML(logoSVG), //nolint:gosec // logoSVG is a constant, not user input
		PublicKeys:       publicKeys,
		PublicKeysJoined: strings.Join(publicKeys, " "),
		CacheURL:         cacheURL,
	}

	tmpl, err := template.New("landing").Parse(landingPageTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return buf.String(), nil
}
