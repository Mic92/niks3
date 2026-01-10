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
      packages.niks3 = pkgs.callPackage ./niks3.nix {
        inherit (pkgs) go;
        nix = pkgs.nixVersions.stable;
      };
      packages.niks3-docker = pkgs.callPackage ./niks3-docker.nix {
        inherit (config.packages) niks3;
      };
      packages.mock-oidc-server = pkgs.callPackage ./mock-oidc-server.nix { };
      packages.default = config.packages.niks3;
    };
}
