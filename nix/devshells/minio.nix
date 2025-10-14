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
            command = "cd \"$MINIO_DATA\" && ${pkgs.minio}/bin/minio server .";
            readiness_probe = {
              http_get = {
                host = "127.0.0.1";
                port = 9000;
                path = "/minio/health/ready";
              };
              initial_delay_seconds = 2;
              period_seconds = 1;
              timeout_seconds = 2;
            };
          };
        };
      };
    };
}
