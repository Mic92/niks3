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

    # Cap compiler/vet parallelism at the allotted build cores; go defaults to
    # all CPUs, which exhausts process limits on shared builders.
    export GOMAXPROCS=$NIX_BUILD_CORES

    # With the race detector: several of the defects these tests guard
    # against are data races that only -race reports.
    go test -c -race -p "$NIX_BUILD_CORES" ./client -o client.test
    go test -c -race -p "$NIX_BUILD_CORES" ./server -o server.test
    go test -c -race -p "$NIX_BUILD_CORES" ./server/oidc -o server-oidc.test
    go test -c -race -p "$NIX_BUILD_CORES" ./server/signing -o server-signing.test
    go test -c -race -p "$NIX_BUILD_CORES" ./hook -o hook.test
    go test -c -race -p "$NIX_BUILD_CORES" ./ratelimit -o ratelimit.test
    go test -c -race -p "$NIX_BUILD_CORES" ./cmd/niks3-hook -o cmd-niks3-hook.test

    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall

    mkdir -p $out/bin
    install -D client.test $out/bin/niks3-client.test
    install -D server.test $out/bin/niks3-server.test
    install -D server-oidc.test $out/bin/niks3-server-oidc.test
    install -D server-signing.test $out/bin/niks3-server-signing.test
    install -D hook.test $out/bin/niks3-hook.test
    install -D ratelimit.test $out/bin/niks3-ratelimit.test
    install -D cmd-niks3-hook.test $out/bin/niks3-cmd-niks3-hook.test

    # Remove Go compiler reference to reduce closure size
    if command -v remove-references-to >/dev/null; then
      for f in $out/bin/*.test; do
        remove-references-to -t ${go} "$f"
      done
    fi

    runHook postInstall
  '';
}
