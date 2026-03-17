{
  lib,
  buildNpmPackage,
  esbuild,
  niks3,
}:

# Bundles action/src/index.ts into a single dist/index.js with @actions/core
# and @actions/tool-cache inlined. The output is copied to the repo's dist/
# and committed — GitHub Actions loads it directly from the checkout.
#
# NIKS3_VERSION is baked in via esbuild --define so the action downloads the
# matching release binary: `Mic92/niks3@v1.4.0` → `.../v1.4.0/niks3_*.tar.gz`.
buildNpmPackage {
  pname = "niks3-action";
  inherit (niks3) version;

  src = lib.fileset.toSource {
    fileset = lib.fileset.unions [
      ../../action/src
      ../../action/package.json
      ../../action/package-lock.json
      ../../action/tsconfig.json
    ];
    root = ../../action;
  };

  npmDepsHash = lib.fileContents ./niks3-action-npm-deps-hash.txt;

  # Skip npm build; we invoke esbuild directly so --define works without
  # threading an env var through package.json scripts.
  dontNpmBuild = true;

  nativeBuildInputs = [ esbuild ];

  buildPhase = ''
    runHook preBuild

    esbuild src/index.ts \
      --bundle \
      --platform=node \
      --target=node24 \
      --format=cjs \
      --define:NIKS3_VERSION='"v${niks3.version}"' \
      --outfile=dist/index.js

    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall

    mkdir -p $out
    cp -r dist $out/

    runHook postInstall
  '';

  meta.description = "Bundled JavaScript wrapper for the niks3 GitHub Action";
}
