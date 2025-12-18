{
  lib,
  testers,
  writeText,
  s5cmd,
  niks3,
  niks3-hook,
  rustfs,
  mock-oidc-server,
  nix,
  pkgs,
  ca-derivations-supported,
  ...
}:

let
  apiToken = "test-token-that-is-at-least-36-characters-long";
  apiTokenFile = writeText "api-token" apiToken;
  s3AccessKey = "rustfsadmin";
  s3SecretKey = "rustfsadmin";
  serverUrl = "http://server:5751";
in
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

        nix.package = nix;
        nix.settings.experimental-features = [
          "nix-command"
          "flakes"
        ]
        ++ lib.optional ca-derivations-supported "ca-derivations";
        nix.settings.substituters = [ ];
        # Trust the signing key
        nix.settings.trusted-public-keys = [ signingPublicKey ];

        services.niks3 = {
          enable = true;
          httpAddr = "0.0.0.0:5751";

          s3 = {
            endpoint = "server:9000";
            bucket = "niks3-test";
            useSSL = false;
            accessKeyFile = writeText "s3-access-key" s3AccessKey;
            secretKeyFile = writeText "s3-secret-key" s3SecretKey;
          };

          inherit apiTokenFile;
          signKeyFiles = [ signingSecretKey ];

          # OIDC configuration for testing
          oidc = {
            allowInsecure = true; # Allow HTTP for mock server
            providers = {
              test = {
                issuer = "http://127.0.0.1:8080/oidc";
                audience = serverUrl;
                boundClaims = {
                  repository_owner = [ "myorg" ];
                };
              };
            };
          };

          gc = {
            enable = true;
            olderThan = "720h"; # Default 30 days for production use
            # Note: Test uses CLI with --older-than 10s for faster testing
          };
        };

        # mTLS reverse proxy. Trusted, so requests with a verified client
        # cert skip bearer-token auth. Certs are generated at test runtime
        # (see testScript) so they never go stale.
        services.niks3.nginx = {
          enable = true;
          domain = "server";
          enableACME = false;
          mtls = {
            enable = true;
            clientCAFile = "/etc/niks3-test-certs/ca.pem";
            boundSubjects = [ "CN=niks3 test client" ];
          };
        };

        services.nginx.virtualHosts."server" = {
          sslCertificate = "/etc/niks3-test-certs/server.pem";
          sslCertificateKey = "/etc/niks3-test-certs/server.key";
        };

        # Certs are written by the testScript before nginx needs them.
        systemd.services.nginx = {
          wants = [ "niks3-test-certs.service" ];
          after = [ "niks3-test-certs.service" ];
        };

        systemd.services.niks3-test-certs = {
          description = "Generate test mTLS certs";
          before = [ "nginx.service" ];
          wantedBy = [ "multi-user.target" ];
          path = [ pkgs.openssl ];
          serviceConfig = {
            Type = "oneshot";
            RemainAfterExit = true;
          };
          script = ''
            set -euo pipefail
            mkdir -p /etc/niks3-test-certs
            cd /etc/niks3-test-certs
            openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 1 \
              -keyout ca.key -out ca.pem -subj "/CN=niks3 test CA"
            openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
              -keyout server.key -out server.csr -subj "/CN=server" -addext "subjectAltName=DNS:server"
            openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
              -days 1 -copy_extensions copy -out server.pem
            openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
              -keyout client.key -out client.csr -subj "/CN=niks3 test client"
            openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
              -days 1 -out client.pem
            chmod 644 *.pem *.key
          '';
        };

        # Mock OIDC server for testing
        systemd.services.mock-oidc = {
          description = "Mock OIDC server for testing";
          after = [ "network.target" ];
          wantedBy = [ "multi-user.target" ];

          serviceConfig = {
            ExecStart = "${mock-oidc-server}/bin/mock-oidc-server -addr 127.0.0.1:8080";
            DynamicUser = true;
            Restart = "on-failure";
          };
        };

        # Run RustFS for S3 storage
        systemd.services.rustfs = {
          description = "RustFS S3-compatible object storage";
          after = [ "network.target" ];
          wantedBy = [ "multi-user.target" ];

          serviceConfig = {
            ExecStart = "${rustfs}/bin/rustfs --address 0.0.0.0:9000 --access-key ${s3AccessKey} --secret-key ${s3SecretKey} /var/lib/rustfs";
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
            S3_ENDPOINT_URL = "http://server:9000";
            AWS_ACCESS_KEY_ID = s3AccessKey;
            AWS_SECRET_ACCESS_KEY = s3SecretKey;
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
          pkgs.curl
        ];

        # Add symlink wrapper for testing issue #59
        environment.etc."niks3-test/symlink-wrapper".source = symlinkWrapper;

        networking.firewall.allowedTCPPorts = [
          443
          5751
          8080
          8081
          9000
        ];
      };

    builder = {
      imports = [ ../nixosModules/niks3-auto-upload.nix ];
      nix.package = nix;
      nix.settings = {
        experimental-features = [
          "nix-command"
          "flakes"
        ];
        substituters = [ ];
      };
      services.niks3-auto-upload = {
        enable = true;
        package = niks3-hook;
        inherit serverUrl;
        authTokenFile = toString apiTokenFile;
        batchSize = 5;
        idleExitTimeout = 5;
      };
    };
  };

  testScript = ''
    start_all()

    # Common test variables
    niks3_cmd = "${niks3}/bin/niks3"
    server_url = "${serverUrl}"
    niks3_push_env = f"NIKS3_SERVER_URL={server_url} NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/auth-token"
    s3_env = "export AWS_ACCESS_KEY_ID=${s3AccessKey}\nexport AWS_SECRET_ACCESS_KEY=${s3SecretKey}"
    binary_cache_url = "s3://niks3-test?endpoint=http://server:9000&region=us-east-1"

    # Wait for services to be ready
    server.wait_for_unit("postgresql.service")
    server.wait_for_unit("rustfs.service")
    server.wait_for_unit("rustfs-setup.service")
    server.wait_for_unit("mock-oidc.service")
    server.wait_for_unit("niks3.service")
    server.wait_for_open_port(5751)
    server.wait_for_open_port(8080)
    server.wait_for_open_port(9000)

    # Create auth token file for testing
    server.succeed("mkdir -p /tmp/test-config")
    server.succeed("echo -n '${apiToken}' > /tmp/test-config/auth-token")

    # Use hello package to get a real closure with dependencies
    test_path = "${pkgs.hello}"
    print(f"Hello store path: {test_path}")

    # Test pushing the closure using the niks3 client with file-based auth token
    server.succeed(f"{niks3_push_env} {niks3_cmd} push {test_path}")

    # Test with invalid auth token file (should fail)
    server.succeed("echo -n 'invalid-token' > /tmp/test-config/invalid-token")
    server.fail(f"NIKS3_SERVER_URL={server_url} NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/invalid-token {niks3_cmd} push {test_path}")

    # ---- mTLS via nginx vhost ----
    server.wait_for_unit("nginx.service")
    server.wait_for_open_port(443)

    https_url = "https://server"
    certs = "/etc/niks3-test-certs"
    mtls_args = f"--ca-cert {certs}/ca.pem --client-cert {certs}/client.pem --client-key {certs}/client.key"

    # With a verified client cert, no bearer token required.
    server.succeed(f"{niks3_cmd} push --server-url {https_url} {mtls_args} {test_path}")

    # Without a client cert, anonymous TLS still gets 401 from niks3.
    server.fail(f"{niks3_cmd} push --server-url {https_url} --ca-cert {certs}/ca.pem {test_path}")

    # Without a cert but with a valid bearer token, still works (mTLS is optional).
    server.succeed(f"NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/auth-token {niks3_cmd} push --server-url {https_url} --ca-cert {certs}/ca.pem {test_path}")

    # Cert with a non-allowed subject is rejected by --mtls-bound-subject.
    openssl = "${pkgs.openssl}/bin/openssl"
    server.succeed(f"cd {certs} && {openssl} req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout other.key -out other.csr -subj '/CN=other client'")
    server.succeed(f"cd {certs} && {openssl} x509 -req -in other.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 -out other.pem")
    server.fail(f"{niks3_cmd} push --server-url {https_url} --ca-cert {certs}/ca.pem --client-cert {certs}/other.pem --client-key {certs}/other.key {test_path}")

    # Test pulling from the binary cache using S3 protocol
    server.succeed("mkdir -p /tmp/test-store")

    # Test that signatures are verified by default (without --no-check-sigs)
    # This will fail if narinfos aren't properly signed
    server.succeed(f"""
      {s3_env}
      nix copy --from '{binary_cache_url}' --to /tmp/test-store {test_path}
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

    server.succeed(f"{niks3_push_env} {niks3_cmd} push {test_output}")

    log_output = server.succeed(f"""
      {s3_env}
      nix log --store '{binary_cache_url}' {test_output}
    """)
    assert "test build log output" in log_output, "Build log missing expected output"
  ''
  + (lib.optionalString ca-derivations-supported ''
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
    server.succeed(f"{niks3_push_env} {niks3_cmd} push {ca_output}")

    # Use a chroot store to test retrieval from cache WITH signature verification
    server.succeed("mkdir -p /tmp/chroot-store")

    # Verify nix can retrieve the CA derivation from the cache with signature verification
    server.succeed(f"""
      {s3_env}
      nix copy --from '{binary_cache_url}' --to /tmp/chroot-store {ca_output}
    """)

    # Verify the content is correct in the chroot store
    content = server.succeed(f"nix --store /tmp/chroot-store store cat {ca_output}").strip()
    assert "Hello from CA derivation" in content, "CA derivation content incorrect"

    # Verify realisation info is available in the chroot store
    realisation_info = server.succeed(f"nix --store /tmp/chroot-store realisation info {ca_output}")
    print(f"Realisation info in chroot store: {realisation_info}")
  '')
  + ''
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
    server.succeed(f"{niks3_push_env} {niks3_cmd} push {symlink_wrapper}")

    # Verify we can retrieve it from the cache
    server.succeed(f"""
      {s3_env}
      nix copy --from '{binary_cache_url}' --to /tmp/test-store {symlink_wrapper}
    """)

    # ============================================
    # OIDC Authentication Tests
    # ============================================

    # Build a simple test derivation for OIDC tests
    server.succeed("""
    cat > /tmp/oidc-test.nix << 'EOF'
    derivation {
      name = "oidc-test";
      system = builtins.currentSystem;
      builder = "/bin/sh";
      args = [ "-c" "echo 'OIDC test derivation' > $out" ];
    }
    EOF
    """)

    oidc_test_path = server.succeed("nix-build --log-format bar-with-logs /tmp/oidc-test.nix --no-out-link").strip()
    print(f"OIDC test store path: {oidc_test_path}")

    # Test 1: Push with valid OIDC token (matching repository_owner claim)
    valid_oidc_token = server.succeed(
      f"curl -s 'http://127.0.0.1:8081/issue?sub=repo:myorg/myrepo:ref:refs/heads/main&aud={server_url}&repository_owner=myorg'"
    ).strip()
    print(f"Valid OIDC token obtained (length={len(valid_oidc_token)})")

    server.succeed(f"NIKS3_SERVER_URL={server_url} {niks3_cmd} push --auth-token '{valid_oidc_token}' {oidc_test_path}")
    print("OIDC push with valid token: SUCCESS")

    # Build another derivation for the failure tests
    server.succeed("""
    cat > /tmp/oidc-test2.nix << 'EOF'
    derivation {
      name = "oidc-test2";
      system = builtins.currentSystem;
      builder = "/bin/sh";
      args = [ "-c" "echo 'OIDC test 2' > $out" ];
    }
    EOF
    """)

    oidc_test_path2 = server.succeed("nix-build --log-format bar-with-logs /tmp/oidc-test2.nix --no-out-link").strip()

    # Test 2: Push with invalid OIDC token (wrong repository_owner claim)
    invalid_oidc_token = server.succeed(
      f"curl -s 'http://127.0.0.1:8081/issue?sub=repo:otherorg/repo:ref:refs/heads/main&aud={server_url}&repository_owner=otherorg'"
    ).strip()
    print("Invalid OIDC token obtained (wrong org)")

    server.fail(f"NIKS3_SERVER_URL={server_url} {niks3_cmd} push --auth-token '{invalid_oidc_token}' {oidc_test_path2}")
    print("OIDC push with wrong org: correctly rejected")

    # Test 3: Push with invalid OIDC token (wrong audience)
    wrong_aud_token = server.succeed(
      "curl -s 'http://127.0.0.1:8081/issue?sub=repo:myorg/myrepo:ref:refs/heads/main&aud=http://wrong:5751&repository_owner=myorg'"
    ).strip()
    print("Wrong audience OIDC token obtained")

    server.fail(f"NIKS3_SERVER_URL={server_url} {niks3_cmd} push --auth-token '{wrong_aud_token}' {oidc_test_path2}")
    print("OIDC push with wrong audience: correctly rejected")

    # Test 4: Push with malformed token (not a valid JWT)
    server.fail(f"NIKS3_SERVER_URL={server_url} {niks3_cmd} push --auth-token 'not-a-valid-jwt-token' {oidc_test_path2}")
    print("OIDC push with malformed token: correctly rejected")

    print("All OIDC tests passed!")

    # ============================================
    # Pin Command Tests
    # ============================================

    # Test 1: Create a pin for an existing store path using 'pins create'
    server.succeed(f"""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/auth-token \
      ${niks3}/bin/niks3 pins create hello-pin {test_path}
    """)

    # Test 2: List pins and verify our pin exists
    pins_output = server.succeed("""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/auth-token \
      ${niks3}/bin/niks3 pins list
    """)
    assert "hello-pin" in pins_output, f"Pin 'hello-pin' not found in list: {pins_output}"

    # Test 3: List pins with --names-only for scripting
    pins_names = server.succeed("""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/auth-token \
      ${niks3}/bin/niks3 pins list --names-only
    """).strip()
    assert "hello-pin" in pins_names, f"Pin 'hello-pin' not found in names-only list: {pins_names}"

    # Test 4: Verify pin is accessible via S3 (using s5cmd with credentials)
    pin_content = server.succeed("""
      export S3_ENDPOINT_URL=http://localhost:9000
      export AWS_ACCESS_KEY_ID=rustfsadmin
      export AWS_SECRET_ACCESS_KEY=rustfsadmin
      ${s5cmd}/bin/s5cmd cat s3://niks3-test/pins/hello-pin
    """).strip()
    assert pin_content == test_path, f"Pin content mismatch: expected {test_path}, got {pin_content}"

    # Test 5: Create a pin during push using --pin flag
    server.succeed(f"""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/auth-token \
      ${niks3}/bin/niks3 push --pin ca-pin {ca_output}
    """)

    # Verify the new pin exists
    pins_after_push = server.succeed("""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/auth-token \
      ${niks3}/bin/niks3 pins list --names-only
    """).strip()
    assert "ca-pin" in pins_after_push, f"Pin 'ca-pin' not found after push: {pins_after_push}"

    # Test 6: Delete a pin
    server.succeed("""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/auth-token \
      ${niks3}/bin/niks3 pins delete hello-pin
    """)

    # Verify pin is gone
    pins_after_delete = server.succeed("""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/auth-token \
      ${niks3}/bin/niks3 pins list --names-only
    """).strip()
    assert "hello-pin" not in pins_after_delete, f"Pin 'hello-pin' still exists after delete: {pins_after_delete}"
    assert "ca-pin" in pins_after_delete, f"Pin 'ca-pin' should still exist: {pins_after_delete}"

    # Test 7: Try to create pin for non-existent store path (should fail)
    server.fail("""
      NIKS3_SERVER_URL=http://server:5751 \
      NIKS3_AUTH_TOKEN_FILE=/tmp/test-config/auth-token \
      ${niks3}/bin/niks3 pins create bad-pin /nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-nonexistent
    """)

    print("All pin tests passed!")

    # Test that GC systemd service runs successfully
    server.succeed("systemctl start niks3-gc.service")

    # ============================================
    # Post-build-hook test: builder → socket → listener → server → S3
    # ============================================

    builder.wait_for_unit("niks3-auto-upload.socket")
    builder.succeed("test -S /run/niks3/upload-to-cache.sock")
    builder.succeed("grep post-build-hook /etc/nix/nix.conf")

    # Build a derivation on the builder — this triggers the post-build-hook
    builder.succeed("""
    cat > /tmp/test-drv.nix << 'EOF'
    derivation {
      name = "post-build-hook-test";
      system = builtins.currentSystem;
      builder = "/bin/sh";
      args = [ "-c" "echo 'hello from post-build-hook test' > $out" ];
    }
    EOF
    """)

    test_output = builder.succeed("nix-build --log-format bar-with-logs /tmp/test-drv.nix --no-out-link").strip()
    print(f"Built store path: {test_output}")

    # The post-build-hook fires asynchronously via socket activation.
    # Wait for the service to activate, upload, then exit after the idle timeout.
    builder.wait_for_unit("niks3-auto-upload.service")
    builder.wait_until_succeeds(
        "test $(systemctl is-active niks3-auto-upload.service) = inactive",
        timeout=60,
    )

    # Verify the path can be fetched from S3 with signature verification
    server.succeed(f"""
      {s3_env}
      nix copy --from '{binary_cache_url}' --to /tmp/hook-test-store {test_output}
    """)

    content = server.succeed(f"nix --store /tmp/hook-test-store store cat {test_output}").strip()
    assert "hello from post-build-hook test" in content, f"Content mismatch: {content}"

    print("Post-build-hook pipeline test passed!")
  '';
}
