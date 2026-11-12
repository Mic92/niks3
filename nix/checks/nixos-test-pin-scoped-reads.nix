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
  name = "nixos-test-pin-scoped-reads";

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
      nginx = {
        enable = true;
        domain = "server";
        enableACME = false;
        mtls = {
          enable = true;
          clientCAFile = "/etc/niks3-test-certs/ca.pem";
          boundSubjects = [ "CN=ci" ];
          readPins = [ "CN=*=host-{cn}" ];
        };
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
      description = "Generate test mTLS certs for pin-scoped reads";
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
          -keyout ci.key -out ci.csr -subj "/CN=ci"
        openssl x509 -req -in ci.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
          -days 1 -out ci.pem
        openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
          -keyout web-01.key -out web-01.csr -subj "/CN=web-01"
        openssl x509 -req -in web-01.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
          -days 1 -out web-01.pem
        openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
          -keyout db-01.key -out db-01.csr -subj "/CN=db-01"
        openssl x509 -req -in db-01.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
          -days 1 -out db-01.pem
        chmod 644 *.pem *.key
      '';
    };

    environment.systemPackages = [
      niks3
      pkgs.curl
    ];
  };

  testScript = ''
    server.wait_for_unit("niks3.service")
    server.wait_for_open_port(5751)
    server.wait_for_unit("nginx.service")
    server.wait_for_open_port(443)

    # Create auth token file for pushing
    server.succeed("echo -n '${apiToken}' > /tmp/auth-token")

    # Build two different store paths with different content
    server.succeed("nix-build -E 'derivation { name=\"web-test\"; system=builtins.currentSystem; builder=\"/bin/sh\"; args=[\"-c\" \"echo web-content > $out\"]; }' --no-out-link > /tmp/web-path")
    server.succeed("nix-build -E 'derivation { name=\"db-test\"; system=builtins.currentSystem; builder=\"/bin/sh\"; args=[\"-c\" \"echo db-content > $out\"]; }' --no-out-link > /tmp/db-path")

    web_path = server.succeed("cat /tmp/web-path").strip()
    db_path = server.succeed("cat /tmp/db-path").strip()
    print(f"Web path: {web_path}")
    print(f"DB path: {db_path}")

    # Extract the store path hash (32-char hash before the first dash)
    web_hash = web_path.split("/")[-1].split("-")[0]
    db_hash = db_path.split("/")[-1].split("-")[0]
    print(f"Web hash: {web_hash}")
    print(f"DB hash: {db_hash}")

    certs = "/etc/niks3-test-certs"
    niks3_cmd = "${niks3}/bin/niks3"

    # Push both paths as CN=ci through nginx
    server.succeed(f"NIKS3_AUTH_TOKEN_FILE=/tmp/auth-token {niks3_cmd} push --server-url https://server --ca-cert {certs}/ca.pem --client-cert {certs}/ci.pem --client-key {certs}/ci.key {web_path}")
    server.succeed(f"NIKS3_AUTH_TOKEN_FILE=/tmp/auth-token {niks3_cmd} push --server-url https://server --ca-cert {certs}/ca.pem --client-cert {certs}/ci.pem --client-key {certs}/ci.key {db_path}")

    # Pin them as CN=ci
    server.succeed(f"curl -s -X POST --cacert {certs}/ca.pem --cert {certs}/ci.pem --key {certs}/ci.key -d '{{\"store_path\": \"{web_path}\"}}' -H 'Content-Type: application/json' https://server/api/pins/host-web-01")
    server.succeed(f"curl -s -X POST --cacert {certs}/ca.pem --cert {certs}/ci.pem --key {certs}/ci.key -d '{{\"store_path\": \"{db_path}\"}}' -H 'Content-Type: application/json' https://server/api/pins/host-db-01")

    # Helper function for HTTP status checks
    def status(cert, path, method="GET", data=None):
        if method == "POST" and data:
            cmd = f"curl -s -o /dev/null -w '%{{http_code}}' -X POST --cacert {certs}/ca.pem --cert {certs}/{cert}.pem --key {certs}/{cert}.key -d '{data}' -H 'Content-Type: application/json' https://server{path}"
        else:
            cmd = f"curl -s -o /dev/null -w '%{{http_code}}' --cacert {certs}/ca.pem --cert {certs}/{cert}.pem --key {certs}/{cert}.key https://server{path}"
        return server.succeed(cmd).strip()

    # Test CN=web-01 can read its own narinfo
    code = status("web-01", f"/{web_hash}.narinfo")
    assert code == "200", f"web-01 should get 200 for its own narinfo, got {code}"

    # Test CN=web-01 can read its own pins
    code = status("web-01", "/pins/host-web-01")
    assert code == "200", f"web-01 should get 200 for its own pins, got {code}"

    # Test CN=web-01 cannot read db narinfo
    code = status("web-01", f"/{db_hash}.narinfo")
    assert code == "404", f"web-01 should get 404 for db narinfo, got {code}"

    # Test CN=web-01 cannot read db pins
    code = status("web-01", "/pins/host-db-01")
    assert code == "404", f"web-01 should get 404 for db pins, got {code}"

    # Test CN=web-01 cannot write pins
    code = status("web-01", "/api/pins/test", "POST", f'{{"store_path": "{web_path}"}}')
    assert code == "403", f"web-01 should get 403 for write, got {code}"

    # Test symmetry: CN=db-01
    code = status("db-01", f"/{db_hash}.narinfo")
    assert code == "200", f"db-01 should get 200 for its own narinfo, got {code}"

    code = status("db-01", f"/{web_hash}.narinfo")
    assert code == "404", f"db-01 should get 404 for web narinfo, got {code}"

    # Test nix-cache-info is readable
    code = status("web-01", "/nix-cache-info")
    assert code == "200", f"web-01 should get 200 for nix-cache-info, got {code}"
  '';
}
