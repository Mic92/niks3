{
  lib,
  testers,
  writeShellApplication,
  niks3,
  niks3-openbao-signer,
  openbao,
  pkgs,
  common,
  ...
}:

let
  inherit (common) apiToken;
  baoAddr = "http://127.0.0.1:8200";
  baoTokenFile = "/run/openbao-signer/token";

  # niks3 passes no arguments, so a wrapper supplies the configuration.
  signer = writeShellApplication {
    name = "niks3-openbao-signer";
    text = ''
      exec ${lib.getExe niks3-openbao-signer} \
        -addr ${baoAddr} -token-file ${baoTokenFile} \
        -key niks3 -signature-name ${common.signing.name}
    '';
  };
in
testers.nixosTest {
  name = "nixos-test-external-signer";

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
      signProgram = signer;
      signPublicKey = common.signing.publicKey;
      readProxy.enable = true;
    };

    # In-memory dev server: unsealed, with a fixed root token.
    systemd.services.openbao-dev = {
      wantedBy = [ "multi-user.target" ];
      # Dev mode expands "~" and fails without HOME, which DynamicUser units lack.
      environment.HOME = "/var/lib/openbao-dev";
      serviceConfig = {
        ExecStart = "${openbao}/bin/bao server -dev -dev-root-token-id=root -dev-listen-address=127.0.0.1:8200";
        DynamicUser = true;
        StateDirectory = "openbao-dev";
        RuntimeDirectory = "openbao-signer";
      };
    };

    systemd.services.openbao-setup = {
      after = [ "openbao-dev.service" ];
      # Not "requires", so that stopping OpenBao leaves niks3 running.
      wants = [ "openbao-dev.service" ];
      before = [ "niks3.service" ];
      wantedBy = [ "multi-user.target" ];
      environment = {
        BAO_ADDR = baoAddr;
        BAO_TOKEN = "root";
      };
      path = [
        openbao
        pkgs.coreutils
      ];
      script = ''
        for _ in $(seq 60); do bao status >/dev/null 2>&1 && break; sleep 1; done
        bao secrets enable transit
        # Import the shared test key. PKCS#8 wraps the 32 byte seed with a fixed 16 byte prefix.
        { printf '\x30\x2e\x02\x01\x00\x30\x05\x06\x03\x2b\x65\x70\x04\x22\x04\x20'
          echo ${common.signing.secretKeyBase64} | base64 -d | head -c 32; } | base64 -w0 > "$RUNTIME_DIRECTORY/key.p8"
        bao transit import transit/keys/niks3 "@$RUNTIME_DIRECTORY/key.p8" type=ed25519
        rm "$RUNTIME_DIRECTORY/key.p8"
        install -D -m 0644 <(echo -n root) ${baoTokenFile}
      '';
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        RuntimeDirectory = "openbao-signer-setup";
      };
    };

    systemd.services.niks3 = {
      after = [ "openbao-setup.service" ];
      requires = [ "openbao-setup.service" ];
    };

    environment.systemPackages = [
      niks3
      pkgs.curl
    ];
  };

  testScript = ''
    server.wait_for_unit("niks3.service")
    server.wait_for_open_port(5751)

    server.succeed("echo -n '${apiToken}' > /tmp/auth-token")
    push = "NIKS3_SERVER_URL=http://localhost:5751 NIKS3_AUTH_TOKEN_FILE=/tmp/auth-token ${niks3}/bin/niks3 push "

    def build(name):
        return server.succeed(
            f"nix-build -E 'derivation {{ name=\"{name}\"; system=builtins.currentSystem; builder=\"/bin/sh\"; args=[\"-c\" \"echo {name} > $out\"]; }}' --no-out-link"
        ).strip()

    def push_and_verify(path, store):
        server.succeed(push + path)
        # Signature verification is on by default, so this fails unless OpenBao signed the narinfo.
        server.succeed(f"nix copy --from http://localhost:5751 --to {store} {path}")
        server.succeed(f"nix --store {store} store cat {path} | grep -F signer-")

    with subtest("narinfos signed through OpenBao verify against the public key"):
        push_and_verify(build("signer-1"), "/tmp/store-1")

    with subtest("the signing process is reused across pushes"):
        first = server.succeed("pgrep -x niks3-openbao-s | head -n1").strip()
        push_and_verify(build("signer-2"), "/tmp/store-2")
        t.assertEqual(first, server.succeed("pgrep -x niks3-openbao-s | head -n1").strip())

    with subtest("a killed signing process is restarted"):
        server.succeed("pkill -KILL -x niks3-openbao-s")
        push_and_verify(build("signer-3"), "/tmp/store-3")

    with subtest("OpenBao errors fail the push without crashing the server"):
        server.succeed("systemctl stop openbao-dev.service")
        server.fail(push + build("signer-4"))
        server.succeed("systemctl restart openbao-setup.service")
        push_and_verify(build("signer-5"), "/tmp/store-5")
  '';
}
