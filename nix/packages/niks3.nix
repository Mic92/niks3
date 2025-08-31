{
  pkgs,
  lib,
  crane,
}:
let
  craneLib = crane.mkLib pkgs;

  # Filter source to include only rust-related files for the client
  src = lib.fileset.toSource {
    root = ../../client;
    fileset = lib.fileset.unions [
      ../../client/Cargo.toml
      ../../client/Cargo.lock
      ../../client/src
    ];
  };

  commonArgs = {
    inherit src;
    pname = "niks3";
    version = "0.1.0";
    strictDeps = true;

    buildInputs = [ pkgs.openssl ];
    nativeBuildInputs = [ pkgs.pkg-config ];
  };

  # Build *just* the cargo dependencies
  cargoArtifacts = craneLib.buildDepsOnly commonArgs;

  # Build the actual crate
  niks3 = craneLib.buildPackage (
    commonArgs
    // {
      inherit cargoArtifacts;
      doCheck = true;

      # Add nix to checkInputs so nix-store is available during tests
      nativeCheckInputs = [ pkgs.nix ];

      passthru.clippy = craneLib.cargoClippy (
        commonArgs
        // {
          inherit cargoArtifacts;
          cargoClippyExtraArgs = "--all-targets --all-features -- -D warnings";
        }
      );
    }
  );

in
niks3
