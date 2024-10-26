{
  perSystem =
    { config, pkgs, ... }:
    {
      packages.niks3 = pkgs.buildGoModule {
        name = "niks3";
        src = ../..;

        vendorHash = "sha256-X2gMvKQeAY7pVJYAP4O0Nq+seiSuGvPub26seCk0c80=";

        doCheck = true;
        nativeCheckInputs = [
          pkgs.postgresql
          pkgs.minio-client
        ];
      };
      packages.default = config.packages.niks3;
    };
}
