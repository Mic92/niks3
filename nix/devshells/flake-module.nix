{ inputs, ... }:
{
  imports = [
    inputs.process-compose.flakeModule
    ./postgres
  ];

  perSystem =
    { config, pkgs, ... }:
    {
      devShells.default = config.packages.default.overrideAttrs (oldAttrs: {
        GOROOT = "${oldAttrs.passthru.go}/share/go";
        nativeBuildInputs = (oldAttrs.nativeBuildInputs or [ ]) ++ [
          pkgs.bashInteractive
          pkgs.delve
          pkgs.gotools
          pkgs.golangci-lint
          pkgs.gopls
          pkgs.goose # db migrations
          pkgs.delve
          pkgs.postgresql_16
        ];

        shellHook = ''
          # this is only needed for hermetic builds
          unset GO_NO_VENDOR_CHECKS GOSUMDB GOPROXY GOFLAGS
        '';
      });
    };

}
