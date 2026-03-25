#!/usr/bin/env nix-shell
#!nix-shell -i bash -p nix -p coreutils -p gnused -p gawk

set -exuo pipefail

cd "$(git rev-parse --show-toplevel)"

go mod tidy

# Each package has its own vendor hash because they include different
# subsets of the Go source tree via Nix filesets.
for pkg in niks3 niks3-server niks3-hook niks3-tests; do
  case "$pkg" in
    niks3)        hashfile="nix/packages/goVendorHash.txt" ;;
    niks3-server) hashfile="nix/packages/goVendorHash-server.txt" ;;
    niks3-hook)   hashfile="nix/packages/goVendorHash-hook.txt" ;;
    niks3-tests)  hashfile="nix/packages/goVendorHash-tests.txt" ;;
  esac

  failedbuild=$(nix build --log-format bar-with-logs --impure \
    --expr "(builtins.getFlake (toString ./.)).packages.\${builtins.currentSystem}.${pkg}.overrideAttrs (_:{ vendorHash = \"\"; })" \
    2>&1 || true)
  echo "$failedbuild"
  checksum=$(echo "$failedbuild" | awk '/got:.*sha256/ { print $2 }')

  if [ -z "$checksum" ]; then
    # Build succeeded — hash is already correct, read the current one.
    echo "Package $pkg: vendor hash already up to date"
    continue
  fi

  echo "$checksum" > "$hashfile"
  echo "Package $pkg: updated $hashfile to $checksum"
done
