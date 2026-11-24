{
  pkgs,
  selfPackages,
  selfDevShells ? { },
  treefmtCheck ? null,
}:
let
  lib = pkgs.lib;
  common = import ./common.nix { inherit pkgs; };
  system = pkgs.stdenv.hostPlatform.system;
  packages = lib.mapAttrs' (n: lib.nameValuePair "package-${n}") selfPackages;
  devShells = lib.mapAttrs' (n: lib.nameValuePair "devShell-${n}") selfDevShells;
in
packages
// devShells
// {
  treefmt = treefmtCheck;

  version-sync = pkgs.runCommand "niks3-version-sync" { } ''
    version=$(cat ${../../VERSION})
    chart=${../../deploy/helm/niks3/Chart.yaml}
    grep -qx "version: $version" "$chart" || { echo "Chart.yaml version != VERSION ($version)" >&2; exit 1; }
    grep -qx "appVersion: \"v$version\"" "$chart" || { echo "Chart.yaml appVersion != v$version" >&2; exit 1; }
    touch $out
  '';
  golangci-lint = selfPackages.niks3-tests.overrideAttrs (old: {
    # niks3-tests' fileset only carries go.mod, go.sum and the Go source
    # dirs, so without this golangci-lint silently runs with its default
    # config instead of .golangci.yml.
    src = lib.fileset.toSource {
      root = ../..;
      fileset = lib.fileset.unions [
        (lib.fileset.fromSource old.src)
        ../../.golangci.yml
      ];
    };
    nativeBuildInputs = old.nativeBuildInputs ++ [ pkgs.golangci-lint ];
    buildPhase = ''
      HOME=$TMPDIR
      # Cap parallelism at the allotted build cores; golangci-lint defaults to
      # all CPUs, which exhausts process limits on shared builders.
      export GOMAXPROCS=$NIX_BUILD_CORES
      golangci-lint run --concurrency "$NIX_BUILD_CORES"
    '';
    installPhase = ''
      touch $out
    '';
  });

  go-unit-tests =
    pkgs.runCommand "niks3-go-unit-tests"
      {
        nativeBuildInputs = [
          selfPackages.niks3-tests
          pkgs.rustfs
          pkgs.postgresql
          pkgs.nix
        ];
        __darwinAllowLocalNetworking = true;
      }
      ''
        export HOME=$TMPDIR
        # Cap runtime parallelism at the allotted build cores to avoid hitting
        # process limits on shared builders.
        export GOMAXPROCS=$NIX_BUILD_CORES
        # rustfs (via reqwest/rustls-native-certs) wants this
        export SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt

        echo "Running client tests..."
        niks3-client.test -test.v

        echo "Running server tests..."
        niks3-server.test -test.v

        echo "Running OIDC tests..."
        niks3-server-oidc.test -test.v

        echo "Running hook tests..."
        niks3-hook.test -test.v

        echo "Running signing tests..."
        niks3-server-signing.test -test.v

        echo "Running OpenBao signer tests..."
        niks3-openbao-signer.test -test.v

        echo "Running niks3-hook command tests..."
        niks3-cmd-niks3-hook.test -test.v

        echo "Running rate limiter tests..."
        niks3-ratelimit.test -test.v

        touch $out
      '';
}
// lib.optionalAttrs (lib.hasSuffix "linux" system) {
  nixos-test-niks3 = pkgs.callPackage ./nixos-test-niks3.nix {
    inherit common;
    mock-oidc-server = selfPackages.mock-oidc-server;
    niks3 = selfPackages.niks3;
    niks3-hook = selfPackages.niks3-hook;
    nix = pkgs.nixVersions.latest;
    ca-derivations-supported = true;
  };
  nixos-test-niks3-lix = pkgs.callPackage ./nixos-test-niks3.nix {
    inherit common;
    mock-oidc-server = selfPackages.mock-oidc-server;
    niks3 = selfPackages.niks3;
    niks3-hook = selfPackages.niks3-hook;
    nix = pkgs.lixPackageSets.latest.lix;
    ca-derivations-supported = false;
  };
  nixos-test-read-proxy = pkgs.callPackage ./nixos-test-read-proxy.nix {
    inherit common;
    niks3 = selfPackages.niks3;
  };
  nixos-test-external-signer = pkgs.callPackage ./nixos-test-external-signer.nix {
    inherit common;
    niks3 = selfPackages.niks3;
    niks3-openbao-signer = selfPackages.niks3-openbao-signer;
  };
  nixos-test-k3s = pkgs.callPackage ./nixos-test-k3s.nix {
    inherit common;
    inherit (selfPackages) niks3 niks3-docker;
  };
}
