{ inputs, ... }:
{
  imports = [ inputs.treefmt-nix.flakeModule ];

  perSystem = {
    treefmt = {
      # Used to find the project root
      projectRootFile = ".git/config";

      settings.global.excludes = [ ".envrc" ];

      programs.nixfmt.enable = true;
      programs.gofumpt.enable = true;
    };
  };
}
