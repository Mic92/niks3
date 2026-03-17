{ ... }:
{
  perSystem =
    {
      config,
      pkgs,
      ...
    }:
    {
      packages.rustfs = pkgs.callPackage ./rustfs.nix { };
      packages.niks3 = pkgs.callPackage ./niks3.nix { };
      packages.niks3-server = pkgs.callPackage ./niks3-server.nix { };
      packages.niks3-hook = pkgs.callPackage ./niks3-hook.nix { };
      packages.niks3-tests = pkgs.callPackage ./niks3-tests.nix {
        inherit (pkgs) go;
      };
      packages.niks3-docker = pkgs.callPackage ./niks3-docker.nix {
        inherit (config.packages) niks3-server;
      };
      packages.mock-oidc-server = pkgs.callPackage ./mock-oidc-server.nix { };
      packages.niks3-action = pkgs.callPackage ./niks3-action.nix {
        inherit (config.packages) niks3;
      };
      packages.default = config.packages.niks3;
    };
}
