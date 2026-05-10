{
  pkgs,
  selfPackages,
}:
{
  default = pkgs.mkShell {
    GOROOT = "${pkgs.go}/share/go";

    packages = [
      pkgs.bashInteractive
      pkgs.delve
      pkgs.gotools
      pkgs.golangci-lint
      pkgs.gopls
      pkgs.goose
      pkgs.sqlc
      pkgs.postgresql_16
      pkgs.s5cmd
      pkgs.awscli
      selfPackages.rustfs
      pkgs.watchexec
    ];

    inputsFrom = [
      selfPackages.niks3
    ];

    shellHook = ''
      # this is only needed for hermetic builds
      unset GO_NO_VENDOR_CHECKS GOSUMDB GOPROXY GOFLAGS
    '';
  };
}
