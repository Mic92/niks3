{
  pkgs,
  selfPackages,
  selfDevShells ? { },
}:
let
  lib = pkgs.lib;
  system = pkgs.system;
  packages = lib.mapAttrs' (n: lib.nameValuePair "package-${n}") selfPackages;
  devShells = lib.mapAttrs' (n: lib.nameValuePair "devShell-${n}") selfDevShells;
in
packages
// devShells
// {
  golangci-lint = selfPackages.niks3-tests.overrideAttrs (old: {
    nativeBuildInputs = old.nativeBuildInputs ++ [ pkgs.golangci-lint ];
    buildPhase = ''
      HOME=$TMPDIR
      golangci-lint run
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
          selfPackages.rustfs
          pkgs.postgresql
          pkgs.nix
        ];
        __darwinAllowLocalNetworking = true;
      }
      ''
        export HOME=$TMPDIR

        echo "Running client tests..."
        niks3-client.test -test.v

        echo "Running server tests..."
        niks3-server.test -test.v

        echo "Running OIDC tests..."
        niks3-server-oidc.test -test.v

        echo "Running hook tests..."
        niks3-hook.test -test.v

        touch $out
      '';
}
// lib.optionalAttrs (lib.hasSuffix "linux" system) {
  nixos-test-niks3 = pkgs.callPackage ./nixos-test-niks3.nix {
    mock-oidc-server = selfPackages.mock-oidc-server;
    niks3 = selfPackages.niks3;
    niks3-hook = selfPackages.niks3-hook;
    rustfs = selfPackages.rustfs;
    nix = pkgs.nixVersions.latest;
    ca-derivations-supported = true;
  };
  nixos-test-niks3-lix = pkgs.callPackage ./nixos-test-niks3.nix {
    mock-oidc-server = selfPackages.mock-oidc-server;
    niks3 = selfPackages.niks3;
    niks3-hook = selfPackages.niks3-hook;
    rustfs = selfPackages.rustfs;
    nix = pkgs.lixPackageSets.latest.lix;
    ca-derivations-supported = false;
  };
  nixos-test-read-proxy = pkgs.callPackage ./nixos-test-read-proxy.nix {
    niks3 = selfPackages.niks3;
    rustfs = selfPackages.rustfs;
  };
}
