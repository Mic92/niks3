{ self, ... }:
{
  flake.nixosModules = {
    niks3 = ./niks3.nix;
    default = self.nixosModules.niks3;
  };
}
