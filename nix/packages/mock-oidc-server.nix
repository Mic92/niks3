# Mock OIDC provider used by the NixOS integration tests.
{
  pkgs,
  lib,
}:

let
  common = import ./niks3-src.nix { inherit lib; };
in
pkgs.buildGoModule {
  pname = "mock-oidc-server";
  version = "0.1.0";
  vendorHash = common.vendorHashMockOIDC;

  src = lib.fileset.toSource {
    inherit (common) root;
    fileset = lib.fileset.unions [
      common.commonFiles
      common.srcsNoTests.oidcmock
      common.srcsNoTests.cmd-mock-oidc-server
    ];
  };

  subPackages = [ "cmd/mock-oidc-server" ];

  doCheck = false;

  meta = {
    description = "Mock OIDC server for testing";
    mainProgram = "mock-oidc-server";
  };
}
