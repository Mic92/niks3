{
  perSystem =
    {
      config,
      pkgs,
      ...
    }:
    {
      packages.niks3-server = pkgs.callPackage ./niks3-server.nix { };
      packages.niks3 = pkgs.callPackage ./niks3.nix { };
      packages.default = config.packages.niks3;
    };
}
