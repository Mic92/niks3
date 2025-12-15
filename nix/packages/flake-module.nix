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
      };
      packages.niks3-docker = pkgs.callPackage ./niks3-docker.nix { };
      packages.default = config.packages.niks3;
    };
}
