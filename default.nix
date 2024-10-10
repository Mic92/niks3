with import <nixpkgs> {};
mkShell {
  packages = [
    bashInteractive
    go
    golangci-lint
    gopls

    # for debugging
    delve

    # for testing
    postgresql
    minio-client
  ];
  # delve fails otherwise
  hardeningDisable = [ "all" ];
}
