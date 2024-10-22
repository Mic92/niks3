{
  perSystem =
    { config, pkgs, ... }:
    {
      packages.niks3 = pkgs.buildGoModule {
        name = "niks3";
        src = ../..;

        vendorHash = "sha256-FSDLGB+oN/sZhlBwugOiG2I3m+tcHpkaznY3rX6825c=";

        # TODO: fix sandbox test
        doCheck = false;
        nativeCheckInputs = [
          pkgs.postgresql
          pkgs.minio-client
        ];
      };
      packages.default = config.packages.niks3;
    };
}
