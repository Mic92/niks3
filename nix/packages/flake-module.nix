{
  perSystem =
    { config, pkgs, ... }:
    {
      packages.niks3 = pkgs.buildGoModule {
        name = "niks3";
        src = ../..;

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
