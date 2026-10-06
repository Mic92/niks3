{
  testers,
  niks3,
  pkgs,
  common,
  ...
}:

let
  inherit (common) apiToken;
in
testers.nixosTest {
  name = "nixos-test-read-proxy";

  nodes.server = {
    imports = [
      ../nixosModules/niks3.nix
      (common.rustfsModule { })
    ];

    nix.settings = common.nixSettings;

    services.niks3 = {
      enable = true;
      httpAddr = "0.0.0.0:5751";
      inherit (common) s3;
      apiTokenFile = common.apiTokenFile;
      signKeyFiles = [ common.signing.secretKeyFile ];
      readProxy.enable = true;
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
