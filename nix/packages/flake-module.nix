{
  perSystem =
    {
      config,
      pkgs,
      lib,
      ...
    }:
    {
      packages.niks3 = pkgs.buildGoModule {
        name = "niks3";
        src = lib.fileset.toSource {
          fileset = lib.fileset.unions [
            ../../server
            ../../go.mod
            ../../go.sum
          ];
          root = ../..;
        };

        vendorHash = "sha256-Vqll61QhSmpN6GdL7L2ghUHtzpT9mhxfhyRgTNFVQyo=";

        doCheck = true;
        nativeCheckInputs = [
          pkgs.postgresql
          pkgs.minio-client
          pkgs.minio
        ];
      };
      packages.default = config.packages.niks3;
    };
}
