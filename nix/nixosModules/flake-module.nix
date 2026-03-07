{ self, ... }:
{
  flake.nixosModules = {
    niks3 = ./niks3.nix;
    niks3-auto-upload = ./niks3-auto-upload.nix;
    default = self.nixosModules.niks3;
  };
}
