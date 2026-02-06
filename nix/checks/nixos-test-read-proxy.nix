{
  testers,
  writeText,
  s5cmd,
  niks3,
  rustfs,
  pkgs,
  ...
}:

let
  signingSecretKey = writeText "niks3-signing-key" "niks3-test-1:0knWkx/F+6IJmI4dkvNs14SCaewg9ZWSAQUNg9juRxh/8x+rzUJx9SWdyGOVl21IbJlQemUKG40qW2TTyrE++w==";
  signingPublicKey = "niks3-test-1:f/Mfq81CcfUlnchjlZdtSGyZUHplChuNKltk08qxPvs=";
  apiToken = "test-token-that-is-at-least-36-characters-long";
in
testers.nixosTest {
  name = "nixos-test-read-proxy";

  nodes.server = {
    imports = [ ../nixosModules/niks3.nix ];

    nix.settings = {
      experimental-features = [
        "nix-command"
        "flakes"
      ];
      substituters = [ ];
      trusted-public-keys = [ signingPublicKey ];
    };

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
      apiTokenFile = writeText "api-token" apiToken;
      signKeyFiles = [ signingSecretKey ];
      readProxy.enable = true;
    };

    systemd.services.rustfs = {
      after = [ "network.target" ];
      wantedBy = [ "multi-user.target" ];
      serviceConfig = {
        ExecStart = "${rustfs}/bin/rustfs --address 0.0.0.0:9000 --access-key rustfsadmin --secret-key rustfsadmin /var/lib/rustfs";
        StateDirectory = "rustfs";
        DynamicUser = true;
      };
    };

    systemd.services.rustfs-setup = {
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
        for i in $(seq 60); do s5cmd ls 2>/dev/null && break; sleep 2; done
        s5cmd mb s3://niks3-test || true
      '';
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
      };
    };

    systemd.services.niks3 = {
      after = [ "rustfs-setup.service" ];
      requires = [ "rustfs-setup.service" ];
    };

    environment.systemPackages = [
      niks3
      pkgs.curl
    ];
  };

  testScript = ''
    server.wait_for_unit("niks3.service")
    server.wait_for_open_port(5751)

    # Smoke: nix-cache-info and health served through proxy
    server.succeed("curl -sf http://localhost:5751/nix-cache-info | grep StoreDir")
    server.succeed("curl -sf http://localhost:5751/health | grep OK")

    # Push a derivation via the write path, retrieve via the HTTP read proxy
    server.succeed("echo -n '${apiToken}' > /tmp/auth-token")
    server.succeed("nix-build -E 'derivation { name=\"proxy-test\"; system=builtins.currentSystem; builder=\"/bin/sh\"; args=[\"-c\" \"echo hello-proxy > $out\"]; }' --no-out-link > /tmp/proxy-path")
    server.succeed("NIKS3_SERVER_URL=http://localhost:5751 NIKS3_AUTH_TOKEN_FILE=/tmp/auth-token ${niks3}/bin/niks3 push $(cat /tmp/proxy-path)")

    # nix copy from the HTTP proxy with signature verification
    server.succeed("nix copy --from http://localhost:5751 --to /tmp/proxy-store $(cat /tmp/proxy-path)")
    server.succeed("nix --store /tmp/proxy-store store cat $(cat /tmp/proxy-path) | grep hello-proxy")

    # Invalid paths must 404
    server.succeed("test $(curl -so /dev/null -w '%{http_code}' http://localhost:5751/nonexistent) = 404")
  '';
}
