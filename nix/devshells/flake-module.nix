{ inputs, ... }:
{
  imports = [
    inputs.process-compose.flakeModule
    ./rustfs.nix
    ./postgres
    ./niks3.nix
  ];

  perSystem =
    { config, pkgs, ... }:
    {
      devShells.default = pkgs.mkShell {
        GOROOT = "${pkgs.go}/share/go";

        packages = [
          # Go development
          pkgs.bashInteractive
          pkgs.delve
          pkgs.gotools
          pkgs.golangci-lint
          pkgs.gopls
          pkgs.goose # db migrations
          pkgs.sqlc # type safe querying

          # Database and storage tools
          pkgs.postgresql_16
          pkgs.s5cmd # fast S3 client
          pkgs.awscli
          config.packages.rustfs

          # General tools
          pkgs.watchexec
        ];

        inputsFrom = [
          config.packages.niks3
        ];

        shellHook = ''
          # this is only needed for hermetic builds
          unset GO_NO_VENDOR_CHECKS GOSUMDB GOPROXY GOFLAGS
        '';
      };
    };

}
