{
  perSystem =
    { config, ... }:
    {
      config.process-compose.dev = {
        cli.preHook = ''
          # create default bucket
          mkdir -p "$RUSTFS_DATA/binary-cache"
        '';
        settings = {
          processes.rustfs = {
            command = "${config.packages.rustfs}/bin/rustfs --address 127.0.0.1:9000 --access-key niks3 --secret-key '!Pa55w0rd' \"$RUSTFS_DATA\"";
            readiness_probe = {
              http_get = {
                host = "127.0.0.1";
                port = 9000;
                path = "/health";
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
