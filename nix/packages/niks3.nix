# niks3 CLI (push, gc subcommands)
{
  pkgs,
  lib,
}:

let
  common = import ./niks3-src.nix { inherit lib; };
in
pkgs.buildGoModule {
  pname = "niks3";
  version = "1.5.1";
  inherit (common) vendorHash;

  src = lib.fileset.toSource {
    inherit (common) root;
    fileset = lib.fileset.unions [
      common.commonFiles
      common.srcsNoTests.api
      common.srcsNoTests.client
      common.srcsNoTests.cmdutil
      common.srcsNoTests.ratelimit
      common.srcsNoTests.cmd-niks3
    ];
  };

  subPackages = [ "cmd/niks3" ];

  doCheck = false;
}
