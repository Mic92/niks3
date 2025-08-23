{
  pkgs,
  lib,
}:

pkgs.rustPlatform.buildRustPackage {
  name = "niks3";
  src = lib.fileset.toSource {
    fileset = lib.fileset.unions [
      ../../client/Cargo.toml
      ../../client/Cargo.lock
      ../../client/src
    ];
    root = ../..;
  };

  sourceRoot = "source/client";
  cargoLock = {
    lockFile = ../../client/Cargo.lock;
  };
}
