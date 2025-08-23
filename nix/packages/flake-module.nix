{
  perSystem =
    {
      config,
      pkgs,
      lib,
      ...
    }:
    {
      packages.niks3-server = pkgs.buildGoModule {
        name = "niks3-server";
        src = lib.fileset.toSource {
          fileset = lib.fileset.unions [
            ../../cmd
            ../../server
            ../../go.mod
            ../../go.sum
          ];
          root = ../..;
        };

        vendorHash = "sha256-Vqll61QhSmpN6GdL7L2ghUHtzpT9mhxfhyRgTNFVQyo=";

        subPackages = [ "cmd/niks3-server" ];

        doCheck = true;
        nativeCheckInputs = [
          pkgs.postgresql
          pkgs.minio-client
          pkgs.minio
        ];
      };

      packages.niks3 = pkgs.rustPlatform.buildRustPackage {
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
      };

      packages.default = config.packages.niks3;
    };
}
