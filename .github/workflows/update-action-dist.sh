#!/usr/bin/env nix-shell
#!nix-shell -i bash -p nix -p nodejs -p coreutils -p gawk

# Regenerates action/package-lock.json, nix/packages/niks3-action-npm-deps-hash.txt,
# and dist/index.js. Run when action/package.json or the version in
# nix/packages/niks3.nix changes — NIKS3_VERSION is baked into dist/index.js
# at bundle time, so a stale dist means the action downloads the wrong binary.

set -exuo pipefail

cd "$(git rev-parse --show-toplevel)"

# Regenerate the lockfile. --registry forces the public npm registry regardless
# of ambient ~/.npmrc, so the lockfile is reproducible across environments.
(cd action && npm install --package-lock-only --registry https://registry.npmjs.org/)

# Trial build with an empty hash to extract the real one, same as
# update-vendor-hash.sh does for Go.
failed=$(nix build --log-format bar-with-logs --impure --expr \
  '(builtins.getFlake (toString ./.)).packages.${builtins.currentSystem}.niks3-action.overrideAttrs (_: { npmDepsHash = ""; })' \
  2>&1 || true)

echo "$failed"

hash=$(echo "$failed" | awk '/got:.*sha256/ { print $2 }')
if [ -z "$hash" ]; then
  echo "Error: could not extract npmDepsHash from build output"
  exit 1
fi

echo "$hash" > nix/packages/niks3-action-npm-deps-hash.txt

# Real build now succeeds; copy the bundle.
nix build .#niks3-action
cp -f result/dist/index.js dist/index.js
rm -f result
