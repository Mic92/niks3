{
  perSystem =
    { config, pkgs, ... }:
    {
      devShells.default = config.packages.default.overrideAttrs (oldAttrs: {
        buildInputs = oldAttrs.buildInputs ++ [
          pkgs.bashInteractive
          pkgs.delve
          pkgs.golangci-lint
          pkgs.gopls
        ];
      });
    };

}
