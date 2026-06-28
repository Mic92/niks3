{
  pkgs,
  lib ? pkgs.lib,
}:
let
  packages = rec {
    rustfs = pkgs.callPackage ./rustfs.nix { };
    niks3 = pkgs.callPackage ./niks3.nix { };
    niks3-server = pkgs.callPackage ./niks3-server.nix { };
    niks3-hook = pkgs.callPackage ./niks3-hook.nix { };
    niks3-tests = pkgs.callPackage ./niks3-tests.nix { inherit (pkgs) go; };
    mock-oidc-server = pkgs.callPackage ./mock-oidc-server.nix { };
    benchmark-closure = pkgs.callPackage ../benchmark/benchmark-closure.nix { };
    default = niks3;
  };
in
packages
# Skip the docker image on Darwin: https://github.com/NixOS/nixpkgs/pull/536230
// lib.optionalAttrs (!pkgs.stdenv.hostPlatform.isDarwin) {
  niks3-docker = pkgs.callPackage ./niks3-docker.nix { inherit (packages) niks3-server; };
}
