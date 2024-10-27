{
  perSystem =
    { config, pkgs, ... }:
    {
      packages.niks3 = pkgs.buildGoModule {
        name = "niks3";
        src = ../..;

        vendorHash = "sha256-PX0MYvoyZYYHYV7sMMXVbzDl+TpQjIJpAr4RBFxSmuQ=";

        doCheck = true;
        nativeCheckInputs = [
          pkgs.postgresql
          pkgs.minio-client
        ];
      };
      packages.default = config.packages.niks3;
    };
}
