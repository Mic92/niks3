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
    server =
      let
        # Test signing key pair (generated with nix key generate-secret / convert-secret-to-public)
        signingSecretKey = writeText "niks3-signing-key" "niks3-test-1:0knWkx/F+6IJmI4dkvNs14SCaewg9ZWSAQUNg9juRxh/8x+rzUJx9SWdyGOVl21IbJlQemUKG40qW2TTyrE++w==";
        signingPublicKey = "niks3-test-1:f/Mfq81CcfUlnchjlZdtSGyZUHplChuNKltk08qxPvs=";
      in
      {
        imports = [ ../nixosModules/niks3.nix ];

        nix.settings.experimental-features = [
          "nix-command"
          "flakes"
          "ca-derivations"
        ];
        nix.settings.substituters = [ ];
        # Trust the signing key
        nix.settings.trusted-public-keys = [ signingPublicKey ];

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
          signKeyFiles = [ signingSecretKey ];

          gc = {
            enable = true;
            olderThan = "720h"; # Default 30 days for production use
            # Note: Test uses CLI with --older-than 10s for faster testing
          };
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
            ready=0
            for i in {1..60}; do
              if mc alias set local http://localhost:9000 minioadmin minioadmin; then
                ready=1
                break
              fi
              echo "Waiting for MinIO to start... ($i/60)"
              sleep 2
            done

            if [ "$ready" -eq 0 ]; then
              echo "ERROR: MinIO did not become ready after 60 attempts" >&2
              exit 1
            fi

            # Create the bucket if it doesn't exist
            if ! mc mb --ignore-existing local/niks3-test; then
              echo "ERROR: Failed to create bucket 'niks3-test'" >&2
              exit 1
            fi
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
          pkgs.zstd
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

    # Create auth token file for testing
    server.succeed("mkdir -p /tmp/test-config")
    server.succeed("echo -n 'test-token-that-is-at-least-36-characters-long' > /tmp/test-config/auth-token")

    # Use hello package to get a real closure with dependencies
    test_path = "${pkgs.hello}"
    print(f"Hello store path: {test_path}")

    # Test pushing the closure using the niks3 client with file-based auth token
    server.succeed(f"""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/auth-token \
      ${niks3}/bin/niks3 push {test_path}
    """)

    # Test with invalid auth token file (should fail)
    server.succeed("echo -n 'invalid-token' > /tmp/test-config/invalid-token")
    server.fail(f"""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/invalid-token \
      ${niks3}/bin/niks3 push {test_path}
    """)

    # Test pulling from the binary cache using S3 protocol
    server.succeed("mkdir -p /tmp/test-store")

    # Configure S3 binary cache URL
    binary_cache_url = "s3://niks3-test?endpoint=http://localhost:9000&region=us-east-1"

    # Test that signatures are verified by default (without --no-check-sigs)
    # This will fail if narinfos aren't properly signed
    server.succeed(f"""
      export AWS_ACCESS_KEY_ID=minioadmin
      export AWS_SECRET_ACCESS_KEY=minioadmin
      nix copy --from '{binary_cache_url}' \
                --to /tmp/test-store \
                {test_path}
    """)

    # Create a simple derivation that produces build log output
    server.succeed("""
    cat > /tmp/test-drv.nix << 'EOF'
    derivation {
      name = "test-build-log";
      system = builtins.currentSystem;
      builder = "/bin/sh";
      args = [ "-c" "echo 'test build log output'; echo 'hello world' > $out" ];
    }
    EOF
    """)

    test_output = server.succeed("nix-build --log-format bar-with-logs /tmp/test-drv.nix").strip()
    print(f"Test output path: {test_output}")

    server.succeed(f"""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/auth-token \
      ${niks3}/bin/niks3 push {test_output}
    """)

    log_output = server.succeed(f"""
      export AWS_ACCESS_KEY_ID=minioadmin
      export AWS_SECRET_ACCESS_KEY=minioadmin
      nix log --store '{binary_cache_url}' {test_output}
    """)
    assert "test build log output" in log_output, "Build log missing expected output"

    # Test CA (content-addressed) derivations with signature verification
    server.succeed("""
    cat > /tmp/ca-test.nix << 'EOF'
    derivation {
      name = "ca-test";
      system = builtins.currentSystem;
      builder = "/bin/sh";
      args = [ "-c" "echo 'Hello from CA derivation' > $out" ];
      __contentAddressed = true;
      outputHashMode = "recursive";
      outputHashAlgo = "sha256";
    }
    EOF
    """)

    # Build the CA derivation
    ca_output = server.succeed("nix-build --log-format bar-with-logs /tmp/ca-test.nix --no-out-link").strip()
    print(f"CA derivation output path: {ca_output}")

    # Push CA derivation to cache
    server.succeed(f"""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/auth-token \
      ${niks3}/bin/niks3 push {ca_output}
    """)

    # Use a chroot store to test retrieval from cache WITH signature verification
    server.succeed("mkdir -p /tmp/chroot-store")

    # Verify nix can retrieve the CA derivation from the cache with signature verification
    server.succeed(f"""
      export AWS_ACCESS_KEY_ID=minioadmin
      export AWS_SECRET_ACCESS_KEY=minioadmin
      nix copy --from '{binary_cache_url}' \
                --to /tmp/chroot-store \
                {ca_output}
    """)

    # Verify the content is correct in the chroot store
    content = server.succeed(f"nix --store /tmp/chroot-store store cat {ca_output}").strip()
    assert "Hello from CA derivation" in content, "CA derivation content incorrect"

    # Verify realisation info is available in the chroot store
    realisation_info = server.succeed(f"nix --store /tmp/chroot-store realisation info {ca_output}")
    print(f"Realisation info in chroot store: {realisation_info}")

    # Test that GC systemd service runs successfully
    server.succeed("systemctl start niks3-gc.service")
  '';
}
