#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
cd "$SCRIPT_DIR/.."

version="${1:-}"
if [[ -z $version ]]; then
  echo "USAGE: $0 version" >&2
  exit 1
fi

# Strip "v" prefix if provided
version="${version#v}"
if [[ ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]]; then
  echo "invalid version '${version}', expected e.g. 1.2.3" >&2
  exit 1
fi

if [[ "$(git symbolic-ref --short HEAD)" != "main" ]]; then
  echo "must be on main branch" >&2
  exit 1
fi

waitForPr() {
  local pr=$1
  while true; do
    if [[ "$(gh pr view "$pr" --json state --jq .state)" == MERGED ]]; then
      break
    fi
    echo "Waiting for PR to be merged..."
    sleep 5
  done
}

# ensure we are up-to-date
uncommitted_changes=$(git diff HEAD --compact-summary)
if [[ -n $uncommitted_changes ]]; then
  echo -e "There are uncommitted changes, exiting:\n${uncommitted_changes}" >&2
  exit 1
fi
git pull git@github.com:Mic92/niks3 main
unpushed_commits=$(git log --format=oneline origin/main..main)
if [[ $unpushed_commits != "" ]]; then
  echo -e "\nThere are unpushed changes, exiting:\n$unpushed_commits" >&2
  exit 1
fi
# make sure tag does not exist
if git rev-parse -q --verify "refs/tags/v${version}" >/dev/null ||
  git ls-remote --exit-code --tags git@github.com:Mic92/niks3 "refs/tags/v${version}" >/dev/null; then
  echo "Tag v${version} already exists, exiting" >&2
  exit 1
fi

# VERSION is read by the nix packages. The helm chart cannot import files, so
# keep it in sync here. The version-sync check catches drift.
chart=deploy/helm/niks3/Chart.yaml
echo "$version" >VERSION
sed -i -e "s!^version: .*!version: ${version}!" \
  -e "s!^appVersion: .*!appVersion: \"v${version}\"!" "$chart"

git add VERSION "$chart"
git branch -D "release-${version}" || true
git checkout -b "release-${version}"
git commit -m "bump version ${version}"
git push origin "release-${version}"
pr_url=$(gh pr create \
  --base main \
  --head "release-${version}" \
  --title "Release ${version}" \
  --body "Release ${version} of niks3")

# Extract PR number from URL
pr_number=$(echo "$pr_url" | grep -oE '[0-9]+$')

# Enable auto-merge with specific merge method and delete branch
gh pr merge "$pr_number" --auto --merge --delete-branch
git checkout main

waitForPr "$pr_number"
git pull git@github.com:Mic92/niks3 main
gh release create "v${version}" --draft --title "v${version}" --notes ""
