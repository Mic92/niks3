# niks3-hook binary (send + serve subcommands for post-build-hook)
{
  pkgs,
  lib,
}:

let
  common = import ./niks3-src.nix { inherit lib; };
in
pkgs.buildGoModule {
  pname = "niks3-hook";
  version = "1.4.0";
  vendorHash = common.vendorHashHook;

  src = lib.fileset.toSource {
    inherit (common) root;
    fileset = lib.fileset.unions [
      common.commonFiles
      common.srcsNoTests.api
      common.srcsNoTests.client
      common.srcsNoTests.cmdutil
      common.srcsNoTests.hook
      common.srcsNoTests.ratelimit
      common.srcsNoTests.cmd-niks3-hook
    ];
  };

  subPackages = [ "cmd/niks3-hook" ];

  doCheck = false;
}
