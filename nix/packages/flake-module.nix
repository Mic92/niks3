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
      packages.default = config.packages.niks3;
    };
}
