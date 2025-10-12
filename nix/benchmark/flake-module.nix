{ ... }:
{
  perSystem =
    {
      pkgs,
      ...
    }:
    {
      packages.benchmark-closure = pkgs.callPackage ./benchmark-closure.nix { };
    };
}
