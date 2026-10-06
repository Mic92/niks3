# Example signing program backed by OpenBao's transit engine.
# It is a single file that imports only the standard library, so it is built
# on its own instead of as part of the Go module and needs no vendor hash.
{
  pkgs,
  lib,
}:

pkgs.stdenv.mkDerivation {
  pname = "niks3-openbao-signer";
  version = "0.1.0";

  src = lib.fileset.toSource {
    root = ../../cmd/niks3-openbao-signer;
    fileset = ../../cmd/niks3-openbao-signer/main.go;
  };

  nativeBuildInputs = [ pkgs.go ];

  buildPhase = ''
    runHook preBuild
    export HOME=$TMPDIR GOCACHE=$TMPDIR/go-cache CGO_ENABLED=0
    go build -trimpath -o niks3-openbao-signer main.go
    runHook postBuild
  '';

  installPhase = ''
    runHook preInstall
    install -D niks3-openbao-signer $out/bin/niks3-openbao-signer
    runHook postInstall
  '';

  meta = {
    description = "niks3 signing program using an OpenBao transit key";
    mainProgram = "niks3-openbao-signer";
  };
}
