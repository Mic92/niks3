# Native OIDC login

Native device login is optional. It authenticates the niks3 CLI to an existing
OIDC-enabled server without an external token script or a localhost callback.
Server configuration and permissions continue to determine access.

## Provider configuration

Configure a **public client** in your identity provider with Device Authorization
Grant (RFC 8628) enabled. Its discovery document must advertise
`device_authorization_endpoint`, `token_endpoint`, and `jwks_uri`. The initial
response must include a signed OIDC **ID token**. Configure niks3's server-side
OIDC provider with this exact issuer and the public client ID as audience, and
configure the required claims and permissions as described in the
[OIDC guide](https://github.com/Mic92/niks3/wiki/OIDC).

The CLI uses ID tokens for compatibility with the existing JWT validator. An ID
token is an identity assertion for the client, not a general OAuth API access
token. This mode does **not** support opaque access tokens, token exchange, or a
separate resource-audience access-token contract. The CLI verifies signatures,
issuer, audience, authorized party and subject before sending credentials.

Issuer, discovered endpoints and niks3 server URLs must use HTTPS. Literal
loopback HTTP issuer endpoints are permitted for local mock-provider tests;
this does not allow HTTP niks3 server URLs or HTTPS-to-HTTP redirects. OIDC
endpoint redirects are rejected. Providers do not need to be on the same host
as the niks3 server.

## One command without persistence

```sh
niks3 push --server-url https://cache.example.com \
  --oidc-issuer https://auth.example.com/application/o/niks3-cli/ \
  --oidc-client-id niks3-cli --oidc-device ./result
```

Open the URL printed on stderr on any device and enter the displayed user code.
This works over SSH without a browser or keyring on the remote machine. Tokens
stay in memory until the command exits. Ctrl-C cancels login; device login is
bounded to fifteen minutes and individual HTTP requests to thirty seconds.
Only this explicit mode and `auth login --device` start device authorization.
Device mode cannot be combined with explicit token flags or `--oidc-profile`.

The default scopes are `openid profile offline_access`. Override them with
`--oidc-scopes 'openid profile'` if your provider does not support offline
access; `openid` is required. Refresh is available only when the provider issues
a refresh token and returns usable ID tokens on renewal.

## Save a login on Linux

The initial secure backend supports Linux Secret Service over the existing
session D-Bus. A provider such as GNOME Keyring or a compatible KWallet setup
must have an **unlocked default collection**. The CLI does not launch a session
bus, create or unlock a keyring, or display keyring prompts. Unlock it in your
desktop session before logging in. Missing, locked or unavailable storage is an
error, with no plaintext fallback. macOS and Windows can use ephemeral device
mode; durable login is currently Linux-only.

```sh
niks3 auth login --server-url https://cache.example.com \
  --oidc-issuer https://auth.example.com/application/o/niks3-cli/ \
  --oidc-client-id niks3-cli --device
niks3 auth status --server-url https://cache.example.com
niks3 push --server-url https://cache.example.com ./result
niks3 pins list --server-url https://cache.example.com
niks3 auth logout --server-url https://cache.example.com
```

`NIKS3_SERVER_URL` can supply the server URL for all these commands. A successful
login explicitly installs one fallback profile for this server. Public metadata
is saved under `$XDG_CONFIG_HOME/niks3/oidc` (normally `~/.config/niks3/oidc`);
ID tokens and refresh tokens are stored only in Secret Service. Credentials are
keyed by canonical server URL, exact issuer and public client ID. Server URL
canonicalization ignores host case, default port 443 and trailing slashes;
distinct paths and ports retain separate identities. Log out before changing a
server profile's issuer or client ID. Multiple simultaneous accounts per server
are not supported.

`status` outputs local metadata and cached token expiry as JSON. It does not
print tokens, contact the issuer, or attempt refresh. `refreshable` describes
cached refresh capability; it does not guarantee the provider still accepts the
grant. `logout` deletes local secrets and profile metadata, and does not revoke
the upstream grant or invalidate copies of previously issued JWTs.

## Authentication precedence

For `push`, `gc`, and `pins`, selection follows this order:

| Priority | Source |
| --- | --- |
| 1 | Explicit `--auth-token-script`, then `--auth-token-path`, then `--auth-token` |
| 2 | Saved server profile explicitly selected with `--oidc-profile` |
| 3 | `NIKS3_AUTH_TOKEN_FILE`, then an existing XDG default token file |
| 4 | Configured mTLS client certificate |
| 5 | Saved server profile installed by `auth login` |

Explicit `--oidc-device` selects a separate one-shot flow ahead of defaults.
Existing explicit token flags always win over a saved profile. An existing but
invalid token file remains an error; it does not trigger fallback to a different
identity. A selected stored login with unavailable keyring, invalid credentials
or failed refresh also fails without switching identities or launching login.

`niks3-hook` retains its existing token/file/script/mTLS resolution and does not
inspect saved OIDC profiles. Use `--auth-token-script` for unattended integrations
that manage their own credentials. Regular CLI operations using a saved profile
can run noninteractively and refresh it, but never prompt for device login.

## Refresh behavior and concurrency

Refresh timing is based on the verified ID token's lifetime, independently of
OAuth access-token expiry. Credentials are refreshed after approximately 75% of
that lifetime, and are never returned within thirty seconds of ID-token expiry.
No OAuth access token is substituted for an ID token.

OIDC Core allows refresh responses to omit `id_token`. In this case the CLI saves
any rotated refresh token and retains the previous verified ID token only while
it remains valid outside the expiry margin. It does not repeatedly refresh on
every request; once that token reaches the margin, run `auth login` again (or
repeat an ephemeral invocation). Providers that omit ID tokens cannot support
indefinite native sessions with this token contract.

Linux processes serialize refresh, login, logout and status with a per-server
file lock, reload secure state after acquiring it, and replace whole credential
records. Lock files contain no tokens and are intentionally retained after
logout. If a rotated token cannot be saved, the command fails; re-login may be
necessary because rotation cannot be rolled back at the provider. Refresh and
logout never initiate device or keyring prompts.

This authenticates niks3 CLI requests. It does not configure authenticated Nix
substituter reads or provide credential refresh to the Nix daemon.

## Manual verification

Use a disposable provider client and the server's intended permission claims:

1. With Authentik, enable the device-code grant for the public client and allow
   the requested scopes. With Keycloak, enable OAuth 2.0 Device Authorization
   Grant on the public OIDC client and configure equivalent scopes. Confirm
   discovery advertises a device authorization endpoint and the ID token's
   audience matches the server configuration.
1. From an SSH session, run the ephemeral push example and complete the code on
   another device. Confirm the upload succeeds and no saved profile appears.
1. In a desktop session with an unlocked keyring, run login, status, and push in
   separate processes. Confirm no additional device code is displayed by push.
1. After the provider's ID token reaches its refresh time, run another operation.
   Check both renewal and the documented re-login behavior for providers that
   omit a new ID token. Check concurrent operations against a disposable grant
   when refresh-token rotation is enabled.
1. Log out and verify `push --oidc-profile` requires login again. Check that
   existing token files, token scripts and mTLS still select the expected
   identity, including when a saved login exists.

The automated tests use local mock issuers and a private D-Bus service; CI never
needs a real identity provider or the user's desktop keyring. These manual steps
are a provider compatibility checklist, not a claim of live-provider testing.
