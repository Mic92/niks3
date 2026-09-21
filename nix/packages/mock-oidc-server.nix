{ pkgs }:
pkgs.buildGoModule {
  pname = "mock-oidc-server";
  version = "0.1.0";
  src = ./mock-oidc-server;

  vendorHash = "sha256-nP/1rHixcx8xCN2VkRISU21oYuVMvoK727dxx/vVQA8=";

  doCheck = false;

  meta = {
    description = "Mock OIDC server for testing";
    mainProgram = "mock-oidc-server";
  };
}
