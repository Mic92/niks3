{ pkgs }:
pkgs.buildGoModule {
  pname = "mock-oidc-server";
  version = "0.1.0";
  src = ./mock-oidc-server;

  vendorHash = "sha256-o3/brRHdf929SRwvFyR3DMBLBhbVjlr8w6h5onCb6yI=";

  doCheck = false;

  meta = {
    description = "Mock OIDC server for testing";
    mainProgram = "mock-oidc-server";
  };
}
