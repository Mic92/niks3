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

const landingPageTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nix Binary Cache</title>
    <style>
        :root {
            --primary: #5277C3;
            --bg: #ffffff;
            --surface: #f5f5f5;
            --text: #24292e;
            --text-secondary: #586069;
            --border: #e1e4e8;
            --code-bg: #f6f8fa;
        }

        @media (prefers-color-scheme: dark) {
            :root {
                --bg: #0d1117;
                --surface: #161b22;
                --text: #c9d1d9;
                --text-secondary: #8b949e;
                --border: #30363d;
                --code-bg: #161b22;
            }
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
            line-height: 1.6;
            color: var(--text);
            background: var(--bg);
            padding: 20px;
        }

        .container {
            max-width: 800px;
            margin: 0 auto;
        }

        header {
            text-align: center;
            padding: 40px 0;
            border-bottom: 1px solid var(--border);
        }

        .logo {
            max-width: 400px;
            margin: 0 auto 20px auto;
        }

        .logo svg {
            width: 100%;
            height: auto;
        }

        h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
            color: var(--text);
        }

        .subtitle {
            color: var(--text-secondary);
            font-size: 1.1rem;
        }

        section {
            margin: 40px 0;
            padding: 30px;
            background: var(--surface);
            border-radius: 8px;
            border: 1px solid var(--border);
        }

        h2 {
            font-size: 1.5rem;
            margin-bottom: 20px;
            color: var(--text);
        }

        h3 {
            font-size: 1.2rem;
            margin: 25px 0 15px 0;
            color: var(--text);
        }

        pre {
            background: var(--code-bg);
            padding: 16px;
            border-radius: 6px;
            overflow-x: auto;
            border: 1px solid var(--border);
            margin: 10px 0;
        }

        code {
            font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
            font-size: 0.9em;
            color: var(--text);
        }

        .key-list {
            margin: 15px 0;
        }

        .key-item {
            background: var(--code-bg);
            padding: 12px 16px;
            border-radius: 6px;
            margin: 10px 0;
            border: 1px solid var(--border);
            font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
            font-size: 0.85em;
            word-break: break-all;
        }

        .note {
            background: rgba(255, 200, 61, 0.1);
            border-left: 4px solid #ffc83d;
            padding: 12px 16px;
            margin: 15px 0;
            border-radius: 4px;
        }

        .note p {
            margin: 5px 0;
            color: var(--text-secondary);
        }

        footer {
            text-align: center;
            padding: 40px 0;
            color: var(--text-secondary);
            border-top: 1px solid var(--border);
            margin-top: 40px;
        }

        a {
            color: var(--primary);
            text-decoration: none;
        }

        a:hover {
            text-decoration: underline;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="logo">{{ .Logo }}</div>
            <p class="subtitle">Fast and reliable binary cache for Nix packages</p>
        </header>

        <section>
            <h2>Public Keys</h2>
            <p>Add these trusted public keys to your Nix configuration to verify packages from this cache:</p>

            <div class="key-list">
{{- range .PublicKeys }}
                <div class="key-item">{{ . }}</div>
{{- end }}
{{- if eq (len .PublicKeys) 0 }}
                <div class="note">
                    <p>No signing keys are currently configured for this cache.</p>
                </div>
{{- end }}
            </div>
        </section>

        <section>
            <h2>Usage Instructions</h2>

            <h3>Option 1: Command Line (Temporary)</h3>
            <p>Use this cache for a single Nix command:</p>
            <pre><code>nix build --substituters '{{ .CacheURL }}' \
  --trusted-public-keys '{{ .PublicKeysJoined }}' \
  nixpkgs#hello</code></pre>

            <h3>Option 2: Nix Configuration (Persistent)</h3>
            <p>Add to your <code>~/.config/nix/nix.conf</code> or <code>/etc/nix/nix.conf</code>:</p>
            <pre><code>extra-substituters = {{ .CacheURL }}
extra-trusted-public-keys = {{ .PublicKeysJoined }}</code></pre>

            <h3>Option 3: NixOS Configuration</h3>
            <p>Add to your <code>/etc/nixos/configuration.nix</code>:</p>
            <pre><code>nix.settings = {
  extra-substituters = [ "{{ .CacheURL }}" ];
  extra-trusted-public-keys = [
{{- range .PublicKeys }}
    "{{ . }}"
{{- end }}
  ];
};</code></pre>

            <h3>Option 4: Flake</h3>
            <p>Add to your <code>flake.nix</code>:</p>
            <pre><code>{
  nixConfig = {
    extra-substituters = [ "{{ .CacheURL }}" ];
    extra-trusted-public-keys = [
{{- range .PublicKeys }}
      "{{ . }}"
{{- end }}
    ];
  };

  # ... rest of your flake
}</code></pre>

            <div class="note">
                <p><strong>Note:</strong> Using <code>extra-*</code> settings will add this cache alongside the default cache.nixos.org cache.</p>
            </div>
        </section>

        <section>
            <h2>Cache Information</h2>
            <p>This binary cache is powered by <a href="https://github.com/Mic92/niks3" target="_blank">niks3</a>, a garbage-collected Nix binary cache backed by S3-compatible storage.</p>

            <h3>Features</h3>
            <ul style="margin-left: 20px; color: var(--text-secondary);">
                <li>Cryptographically signed packages</li>
                <li>Reference-tracking garbage collection</li>
                <li>High availability via S3</li>
                <li>Efficient storage with zstd compression</li>
            </ul>
        </section>

        <footer>
            <p>Powered by <a href="https://github.com/Mic92/niks3">niks3</a></p>
        </footer>
    </div>
</body>
</html>
`

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
