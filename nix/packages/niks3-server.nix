# niks3-server binary
{
  pkgs,
  lib,
}:

let
  common = import ./niks3-src.nix { inherit lib; };
in
pkgs.buildGoModule {
  pname = "niks3-server";
  version = "1.4.0";
  vendorHash = common.vendorHashServer;

  src = lib.fileset.toSource {
    inherit (common) root;
    fileset = lib.fileset.unions [
      common.commonFiles
      common.srcsNoTests.api
      common.srcsNoTests.server
      common.srcsNoTests.ratelimit
      common.srcsNoTests.cmd-niks3-server
    ];
  };

  subPackages = [ "cmd/niks3-server" ];

  doCheck = false;
}
