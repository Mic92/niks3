with import <nixpkgs> {};
mkShell {
  packages = [
    bashInteractive
    go
    golangci-lint
    gopls
  ];
}
