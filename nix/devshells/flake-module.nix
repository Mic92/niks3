{
  perSystem =
    { config, pkgs, ... }:
    {
      devShells.default = config.packages.default.overrideAttrs (oldAttrs: {
        nativeBuildInputs = (oldAttrs.nativeBuildInputs or []) ++ [
          pkgs.bashInteractive
          pkgs.delve
          pkgs.golangci-lint
          pkgs.gopls
        ];
      });
    };

}
