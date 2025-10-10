{ ... }:
{
  perSystem =
    {
      config,
      pkgs,
      ...
    }:
    {
      packages.niks3 = pkgs.callPackage ./niks3.nix { };
      packages.default = config.packages.niks3;
    };
}
