{ inputs, ... }:
{
  imports = [
    inputs.process-compose.flakeModule
    ./minio.nix
    ./postgres
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

          # Rust development
          pkgs.rustfmt
          pkgs.clippy

          # Database and storage tools
          pkgs.postgresql_16
          pkgs.minio-client
          pkgs.awscli
          pkgs.minio

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
