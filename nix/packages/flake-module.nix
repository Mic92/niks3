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
      packages.niks3 = pkgs.callPackage ./niks3.nix { rustfs = config.packages.rustfs; };
      packages.default = config.packages.niks3;
    };
}
