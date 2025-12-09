{
  testers,
  writeText,
  s5cmd,
  niks3,
  rustfs,
  pkgs,
  ...
}:

testers.nixosTest {
  name = "nixos-test-niks3";

  nodes = {
    server =
      let
        # Test signing key pair (generated with nix key generate-secret / convert-secret-to-public)
        signingSecretKey = writeText "niks3-signing-key" "niks3-test-1:0knWkx/F+6IJmI4dkvNs14SCaewg9ZWSAQUNg9juRxh/8x+rzUJx9SWdyGOVl21IbJlQemUKG40qW2TTyrE++w==";
        signingPublicKey = "niks3-test-1:f/Mfq81CcfUlnchjlZdtSGyZUHplChuNKltk08qxPvs=";

        # Create a symlink wrapper for testing issue #59
        symlinkWrapper = pkgs.runCommand "symlink-wrapper" { } ''
          ln -s ${
            pkgs.runCommand "base-package" { } ''
              mkdir -p $out/bin
              echo "#!/bin/sh" > $out/bin/test-program
              echo "echo 'Hello from subdirectory'" >> $out/bin/test-program
              chmod +x $out/bin/test-program
            ''
          }/bin/test-program $out
        '';
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
            accessKeyFile = writeText "s3-access-key" "rustfsadmin";
            secretKeyFile = writeText "s3-secret-key" "rustfsadmin";
          };

          apiTokenFile = writeText "api-token" "test-token-that-is-at-least-36-characters-long";
          signKeyFiles = [ signingSecretKey ];

          gc = {
            enable = true;
            olderThan = "720h"; # Default 30 days for production use
            # Note: Test uses CLI with --older-than 10s for faster testing
          };
        };

        # Run RustFS for S3 storage
        systemd.services.rustfs = {
          description = "RustFS S3-compatible object storage";
          after = [ "network.target" ];
          wantedBy = [ "multi-user.target" ];

          serviceConfig = {
            ExecStart = "${rustfs}/bin/rustfs --address 0.0.0.0:9000 --access-key rustfsadmin --secret-key rustfsadmin /var/lib/rustfs";
            StateDirectory = "rustfs";
            DynamicUser = true;
            Restart = "on-failure";
          };
        };

        systemd.services.rustfs-setup = {
          description = "Setup RustFS bucket";
          after = [ "rustfs.service" ];
          requires = [ "rustfs.service" ];
          before = [ "niks3.service" ];
          wantedBy = [ "multi-user.target" ];

          environment = {
            S3_ENDPOINT_URL = "http://localhost:9000";
            AWS_ACCESS_KEY_ID = "rustfsadmin";
            AWS_SECRET_ACCESS_KEY = "rustfsadmin";
          };

          path = [ s5cmd ];

          script = ''
            set -e

            # Wait for RustFS to be ready
            ready=0
            for i in {1..60}; do
              if s5cmd ls 2>/dev/null; then
                ready=1
                break
              fi
              echo "Waiting for RustFS to start... ($i/60)"
              sleep 2
            done

            if [ "$ready" -eq 0 ]; then
              echo "ERROR: RustFS did not become ready after 60 attempts" >&2
              exit 1
            fi

            # Create the bucket if it doesn't exist
            s5cmd mb s3://niks3-test || true
          '';

          serviceConfig = {
            Type = "oneshot";
            RemainAfterExit = true;
          };
        };

        # Ensure niks3 starts after rustfs-setup
        systemd.services.niks3 = {
          after = [ "rustfs-setup.service" ];
          requires = [ "rustfs-setup.service" ];
        };

        # Add niks3 client and hello to the server
        environment.systemPackages = [
          niks3
          pkgs.hello
          pkgs.zstd
        ];

        # Add symlink wrapper for testing issue #59
        environment.etc."niks3-test/symlink-wrapper".source = symlinkWrapper;

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
    server.wait_for_unit("rustfs.service")
    server.wait_for_unit("rustfs-setup.service")
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
      export AWS_ACCESS_KEY_ID=rustfsadmin
      export AWS_SECRET_ACCESS_KEY=rustfsadmin
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
      export AWS_ACCESS_KEY_ID=rustfsadmin
      export AWS_SECRET_ACCESS_KEY=rustfsadmin
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
      export AWS_ACCESS_KEY_ID=rustfsadmin
      export AWS_SECRET_ACCESS_KEY=rustfsadmin
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

    # Test uploading a store path that is a symlink to a subdirectory
    # This tests the fix for issue #59: a store path that's a symlink pointing
    # to a file in another store path's subdirectory

    # Follow symlinks to get to the actual store path
    # /etc/niks3-test/symlink-wrapper -> /etc/static/... -> /nix/store/.../symlink-wrapper
    symlink_step1 = server.succeed("readlink /etc/niks3-test/symlink-wrapper").strip()
    symlink_wrapper = server.succeed(f"readlink {symlink_step1}").strip()
    print(f"Symlink wrapper store path: {symlink_wrapper}")

    # Verify that the wrapper is indeed a symlink to a subdirectory
    server.succeed(f"test -L {symlink_wrapper}")
    symlink_target = server.succeed(f"readlink {symlink_wrapper}").strip()
    print(f"Symlink wrapper points to: {symlink_target}")
    # The symlink-wrapper itself is a store path that points to a subdirectory
    assert symlink_target.endswith("/bin/test-program"), f"Symlink should point to a subdirectory path: {symlink_target}"

    # Push the symlink wrapper (this would fail before the fix)
    server.succeed(f"""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/auth-token \
      ${niks3}/bin/niks3 push {symlink_wrapper}
    """)

    # Verify we can retrieve it from the cache
    server.succeed(f"""
      export AWS_ACCESS_KEY_ID=rustfsadmin
      export AWS_SECRET_ACCESS_KEY=rustfsadmin
      nix copy --from '{binary_cache_url}' \
                --to /tmp/test-store \
                {symlink_wrapper}
    """)

    # Test that GC systemd service runs successfully
    server.succeed("systemctl start niks3-gc.service")
  '';
}
