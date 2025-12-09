{
  pkgs,
  lib,
  rustfs,
}:

let
  vendorHash = lib.fileContents ./goVendorHash.txt;
in
pkgs.buildGoModule {
  name = "niks3";
  src = lib.fileset.toSource {
    fileset = lib.fileset.unions [
      ../../api
      ../../cmd
      ../../client
      ../../server
      ../../go.mod
      ../../go.sum
    ];
    root = ../..;
  };

  inherit vendorHash;

  subPackages = [
    "cmd/niks3"
    "cmd/niks3-server"
  ];

  doCheck = true;
  nativeCheckInputs = with pkgs; [
    nix
    postgresql
    minio-client # mc client works with any S3-compatible storage
    rustfs
  ];
}
