{
  perSystem =
    { pkgs, ... }:
    {
      config.process-compose.dev = {
        cli.preHook = ''
          # create default bucket
          mkdir -p "$MINIO_DATA/binary-cache"
        '';
        settings = {
          processes.minio = {
            command = "${pkgs.minio}/bin/minio server .";
            working_dir = "$MINIO_DATA";
          };
        };
      };
    };
}
