{
  pkgs,
}:
rec {
  rustfs = pkgs.callPackage ./rustfs.nix { };
  niks3 = pkgs.callPackage ./niks3.nix { };
  niks3-server = pkgs.callPackage ./niks3-server.nix { };
  niks3-hook = pkgs.callPackage ./niks3-hook.nix { };
  niks3-tests = pkgs.callPackage ./niks3-tests.nix { inherit (pkgs) go; };
  niks3-docker = pkgs.callPackage ./niks3-docker.nix { inherit niks3-server; };
  mock-oidc-server = pkgs.callPackage ./mock-oidc-server.nix { };
  benchmark-closure = pkgs.callPackage ../benchmark/benchmark-closure.nix { };
  default = niks3;
}
