{
  pkgs,
}:
rec {
  niks3 = pkgs.callPackage ./niks3.nix { };
  niks3-server = pkgs.callPackage ./niks3-server.nix { };
  niks3-hook = pkgs.callPackage ./niks3-hook.nix { };
  niks3-tests = pkgs.callPackage ./niks3-tests.nix { inherit (pkgs) go; };
  mock-oidc-server = pkgs.callPackage ./mock-oidc-server.nix { };
  benchmark-closure = pkgs.callPackage ../benchmark/benchmark-closure.nix { };
  benchmark-disk-image = pkgs.callPackage ../benchmark/benchmark-disk-image.nix { };
  niks3-docker = pkgs.callPackage ./niks3-docker.nix { inherit niks3-server; };
  default = niks3;
}
