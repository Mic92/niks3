{ pkgs, ... }:

pkgs.nixosTest {
  name = "niks3-integration-test";

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
            accessKeyFile = pkgs.writeText "s3-access-key" "minioadmin";
            secretKeyFile = pkgs.writeText "s3-secret-key" "minioadmin";
          };

          apiTokenFile = pkgs.writeText "api-token" "test-token-that-is-at-least-36-characters-long";
        };

        # Run MinIO for S3 storage
        services.minio = {
          enable = true;
          rootCredentialsFile = pkgs.writeText "minio-credentials" ''
            MINIO_ROOT_USER=minioadmin
            MINIO_ROOT_PASSWORD=minioadmin
          '';
        };

        systemd.services.minio-setup = {
          description = "Setup MinIO bucket";
          after = [ "minio.service" ];
          requires = [ "minio.service" ];
          before = [ "niks3.service" ];
          wantedBy = [ "multi-user.target" ];

          path = [ pkgs.minio-client ];

          script = ''
            set -e

            # Wait for MinIO to be ready
            for i in {1..30}; do
              if mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null; then
                break
              fi
              echo "Waiting for MinIO to start... ($i/30)"
              sleep 1
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

        networking.firewall.allowedTCPPorts = [
          5751
          9000
        ];
      };

    client =
      { pkgs, ... }:
      {
        environment.systemPackages = [
          pkgs.curl
          pkgs.jq
        ];
      };
  };

  testScript = ''
    import json

    start_all()

    # Wait for services to be ready
    server.wait_for_unit("postgresql.service")
    server.wait_for_unit("minio.service")
    server.wait_for_unit("minio-setup.service")
    server.wait_for_unit("niks3.service")
    server.wait_for_open_port(5751)
    server.wait_for_open_port(9000)

    # Test health endpoint (no auth required)
    client.succeed("curl -f http://server:5751/health")

    # Test API endpoints with authentication
    auth_header = "Authorization: Bearer test-token-that-is-at-least-36-characters-long"

    # Test creating a pending closure
    print("Testing pending closure creation...")
    response = client.succeed(f"""
      curl -f -X POST http://server:5751/api/pending_closures \
        -H "{auth_header}" \
        -H "Content-Type: application/json" \
        -d '{{"key": "test-closure-key", "paths": ["/nix/store/test-path"]}}'
    """)

    # Parse the response to get the pending closure ID
    pending_closure = json.loads(response)
    pending_id = pending_closure.get("id")
    print(f"Created pending closure with ID: {pending_id}")

    # Test completing the pending closure
    if pending_id:
      print(f"Completing pending closure {pending_id}...")
      client.succeed(f"""
        curl -f -X POST http://server:5751/api/pending_closures/{pending_id}/complete \
          -H "{auth_header}"
      """)
      print("Successfully completed pending closure")

    # Test getting a closure
    print("Testing closure retrieval...")
    client.succeed(f"""
      curl -f http://server:5751/api/closures/test-closure-key \
        -H "{auth_header}"
    """)

    # Test unauthorized access
    print("Testing unauthorized access...")
    client.fail("curl -f -X POST http://server:5751/api/pending_closures")
    client.fail('curl -f -X POST http://server:5751/api/pending_closures -H "Authorization: Bearer wrong-token"')

    # Test cleanup endpoints
    print("Testing cleanup endpoints...")
    client.succeed(f"""
      curl -f -X DELETE http://server:5751/api/pending_closures \
        -H "{auth_header}"
    """)

    client.succeed(f"""
      curl -f -X DELETE "http://server:5751/api/closures?older_than=1h" \
        -H "{auth_header}"
    """)

    print("All tests passed!")
  '';
}
