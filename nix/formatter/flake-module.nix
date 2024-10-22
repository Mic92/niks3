{ inputs, ... }:
{
  imports = [ inputs.treefmt-nix.flakeModule ];

  perSystem = {
    treefmt = {
      # Used to find the project root
      projectRootFile = ".git/config";

      programs.nixfmt.enable = true;
      programs.gofumpt.enable = true;
    };
  };
}
