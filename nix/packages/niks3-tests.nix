# Pre-compiled Go test binaries for CI.
# Needs the full source tree since it compiles tests for all packages.
{
  pkgs,
  lib,
  go,
}:

let
  common = import ./niks3-src.nix { inherit lib; };
in
pkgs.buildGoModule {
  pname = "niks3-tests";
  version = "1.4.0";
  vendorHash = common.vendorHashTests;

  src = lib.fileset.toSource {
    inherit (common) root;
    fileset = lib.fileset.unions [
      common.commonFiles
      common.srcs.api
      common.srcs.client
      common.srcs.cmdutil
      common.srcs.hook
      common.srcs.ratelimit
      common.srcs.server
      common.srcs.cmd-niks3
      common.srcs.cmd-niks3-server
      common.srcs.cmd-niks3-hook
    ];
  };

  # Don't build any binaries — we only want test binaries.
  subPackages = [ ];

  doCheck = false;

  buildPhase = ''
    runHook preBuild

    go test -c ./client -o client.test
    go test -c ./server -o server.test
    go test -c ./server/oidc -o server-oidc.test
    go test -c ./hook -o hook.test

    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall

    mkdir -p $out/bin
    install -D client.test $out/bin/niks3-client.test
    install -D server.test $out/bin/niks3-server.test
    install -D server-oidc.test $out/bin/niks3-server-oidc.test
    install -D hook.test $out/bin/niks3-hook.test

    # Remove Go compiler reference to reduce closure size
    if command -v remove-references-to >/dev/null; then
      for f in $out/bin/*.test; do
        remove-references-to -t ${go} "$f"
      done
    fi

    runHook postInstall
  '';
}
