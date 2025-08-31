{
  nixosTest,
  writeText,
  minio-client,
  getent,
  niks3,
  ...
}:

nixosTest {
  name = "nixos-test-niks3";

  nodes = {
    server =
      { pkgs, ... }:
      {
        imports = [ ../nixosModules/niks3.nix ];

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

        networking.firewall.allowedTCPPorts = [
          5751
          9000
        ];
      };

    client =
      { pkgs, ... }:
      {
        environment.systemPackages = [
          niks3
        ];
      };

      # Ensure niks3 starts after minio-setup
      systemd.services.niks3 = {
        after = [ "minio-setup.service" ];
        requires = [ "minio-setup.service" ];
      };

      # Add niks3 client to the server
      environment.systemPackages = [
        niks3
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

    # Create a test derivation to upload
    print("Creating test store path...")
    client.succeed("echo 'test content' > /tmp/test-file")
    test_path = client.succeed("nix-store --add /tmp/test-file").strip()
    print(f"Test store path: {test_path}")

    # Test pushing a store path using the niks3 client
    print("Testing niks3 push command...")
    client.succeed(f"""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN=test-token-that-is-at-least-36-characters-long \
      ${niks3}/bin/niks3 push {test_path}
    """)

    print("Successfully pushed store path!")

    # Test with invalid auth token (should fail)
    print("Testing unauthorized access...")
    client.fail(f"""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN=invalid-token \
      ${niks3}/bin/niks3 push {test_path}
    """)

    print("All tests passed!")
  '';
}
