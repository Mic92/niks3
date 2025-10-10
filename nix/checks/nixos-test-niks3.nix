{
  nixosTest,
  writeText,
  minio-client,
  getent,
  niks3,
  pkgs,
  ...
}:

nixosTest {
  name = "nixos-test-niks3";

  nodes = {
    server = {
      imports = [ ../nixosModules/niks3.nix ];

      nix.settings.experimental-features = [
        "nix-command"
        "flakes"
      ];

      services.niks3 = {
        enable = true;
        httpAddr = "0.0.0.0:5751";

        s3 = {
          endpoint = "localhost:9000";
          bucket = "niks3-test";
          useSSL = false;
          accessKeyFile = writeText "s3-access-key" "minioadmin";
          secretKeyFile = writeText "s3-secret-key" "minioadmin";
        };

        apiTokenFile = writeText "api-token" "test-token-that-is-at-least-36-characters-long";
      };

      # Run MinIO for S3 storage
      services.minio = {
        enable = true;
        rootCredentialsFile = writeText "minio-credentials" ''
          MINIO_ROOT_USER=minioadmin
          MINIO_ROOT_PASSWORD=minioadmin
        '';
        listenAddress = ":9000";
      };

      systemd.services.minio-setup = {
        description = "Setup MinIO bucket";
        after = [ "minio.service" ];
        requires = [ "minio.service" ];
        before = [ "niks3.service" ];
        wantedBy = [ "multi-user.target" ];

        path = [
          minio-client
          getent
        ];

        script = ''
          set -e

          # Wait for MinIO to be ready
          for i in {1..60}; do
            if mc alias set local http://localhost:9000 minioadmin minioadmin; then
              echo "MinIO is ready!"
              break
            fi
            echo "Waiting for MinIO to start... ($i/60)"
            sleep 2
          done

          # Create the bucket if it doesn't exist
          mc mb local/niks3-test || true

          echo "MinIO bucket setup complete"
        '';

        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
        };
      };

      # Ensure niks3 starts after minio-setup
      systemd.services.niks3 = {
        after = [ "minio-setup.service" ];
        requires = [ "minio-setup.service" ];
      };

      # Add niks3 client and hello to the server
      environment.systemPackages = [
        niks3
        pkgs.hello
      ];

      networking.firewall.allowedTCPPorts = [
        5751
        9000
      ];
    };
  };

  testScript = ''
    start_all()

    # Wait for services to be ready
    server.wait_for_unit("postgresql.service")
    server.wait_for_unit("minio.service")
    server.wait_for_unit("minio-setup.service")
    server.wait_for_unit("niks3.service")
    server.wait_for_open_port(5751)
    server.wait_for_open_port(9000)

    # Use hello package to get a real closure with dependencies
    test_path = "${pkgs.hello}"
    print(f"Hello store path: {test_path}")

    # Test pushing the closure using the niks3 client
    print("Pushing closure to cache...")
    server.succeed(f"""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN=test-token-that-is-at-least-36-characters-long \
      ${niks3}/bin/niks3 push {test_path}
    """)

    # Test with invalid auth token (should fail)
    server.fail(f"""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN=invalid-token \
      ${niks3}/bin/niks3 push {test_path}
    """)

    # Test pulling from the binary cache using S3 protocol
    server.succeed("mkdir -p /tmp/test-store")

    # Configure S3 binary cache URL
    binary_cache_url = "s3://niks3-test?endpoint=http://localhost:9000&region=us-east-1"

    # Pull from the S3 cache to a local store
    server.succeed(f"""
      export AWS_ACCESS_KEY_ID=minioadmin
      export AWS_SECRET_ACCESS_KEY=minioadmin
      nix copy --from '{binary_cache_url}' \
                --to /tmp/test-store \
                --no-check-sigs \
                {test_path}
    """)
  '';
}
