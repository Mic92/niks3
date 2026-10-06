# Example signing program for PKCS#11 tokens such as HSMs and SoftHSM.
# It is a separate Go module so that cgo and miekg/pkcs11 stay out of the
# main module's dependencies.
{
  pkgs,
  lib,
}:

pkgs.buildGoModule {
  pname = "niks3-pkcs11-signer";
  version = "0.1.0";
  vendorHash = lib.fileContents ./goVendorHash-niks3-pkcs11-signer.txt;

  src = lib.fileset.toSource {
    root = ../../cmd/niks3-pkcs11-signer;
    fileset = lib.fileset.unions [
      ../../cmd/niks3-pkcs11-signer/go.mod
      ../../cmd/niks3-pkcs11-signer/go.sum
      ../../cmd/niks3-pkcs11-signer/main.go
      ../../cmd/niks3-pkcs11-signer/main_test.go
    ];
  };

  nativeCheckInputs = [ pkgs.softhsm ];
  preCheck = ''
    export PKCS11_MODULE=${pkgs.softhsm}/lib/softhsm/libsofthsm2.so
  '';
  checkFlags = [ "-v" ];

  meta = {
    description = "niks3 signing program using an Ed25519 key on a PKCS#11 token";
    mainProgram = "niks3-pkcs11-signer";
  };
}
