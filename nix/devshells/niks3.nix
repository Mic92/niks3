{
  perSystem =
    { pkgs, ... }:
    {
      config.process-compose.dev = {
        cli.preHook = ''
          # Generate signing key if it doesn't exist
          if [ ! -f "$PRJ_DATA_DIR/signing-key.sec" ]; then
            echo "Generating Nix signing key..."
            nix-store --generate-binary-cache-key "niks3-dev" "$PRJ_DATA_DIR/signing-key.sec" "$PRJ_DATA_DIR/signing-key.pub"
            echo "Signing key generated at $PRJ_DATA_DIR/signing-key.sec"
            echo "Public key saved at $PRJ_DATA_DIR/signing-key.pub"
          fi
        '';
        settings = {
          processes.niks3 = {
            command = "${pkgs.watchexec}/bin/watchexec --restart --watch cmd --watch server --watch client --exts go -- go run ./cmd/niks3-server";
            depends_on = {
              postgres.condition = "process_healthy";
              minio.condition = "process_healthy";
            };
            readiness_probe = {
              http_get = {
                host = "127.0.0.1";
                port = 5751;
                path = "/health";
              };
              initial_delay_seconds = 5;
              period_seconds = 2;
              timeout_seconds = 2;
            };
          };
        };
      };
    };
}
