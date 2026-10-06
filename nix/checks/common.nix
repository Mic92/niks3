# Building blocks shared by the NixOS integration tests.
{ pkgs }:
let
  inherit (pkgs) lib writeText;
in
rec {
  s3AccessKey = "rustfsadmin";
  s3SecretKey = "rustfsadmin";

  apiToken = "test-token-that-is-at-least-36-characters-long";
  apiTokenFile = writeText "api-token" apiToken;

  # Key pair generated with nix key generate-secret / convert-secret-to-public.
  signing = {
    name = "niks3-test-1";
    secretKeyBase64 = "0knWkx/F+6IJmI4dkvNs14SCaewg9ZWSAQUNg9juRxh/8x+rzUJx9SWdyGOVl21IbJlQemUKG40qW2TTyrE++w==";
    publicKey = "niks3-test-1:f/Mfq81CcfUlnchjlZdtSGyZUHplChuNKltk08qxPvs=";
    secretKeyFile = writeText "niks3-signing-key" "niks3-test-1:${signing.secretKeyBase64}";
  };

  # services.niks3.s3 pointing at the rustfs from rustfsModule on the same host.
  s3 = {
    endpoint = "localhost:9000";
    bucket = "niks3-test";
    useSSL = false;
    accessKeyFile = writeText "s3-access-key" s3AccessKey;
    secretKeyFile = writeText "s3-secret-key" s3SecretKey;
  };

  # RustFS plus a oneshot that creates the bucket. With orderNiks3, niks3.service
  # starts only after the bucket exists.
  rustfsModule =
    {
      bucket ? s3.bucket,
      orderNiks3 ? true,
    }:
    { ... }:
    {
      systemd.services.rustfs = {
        description = "RustFS S3-compatible object storage";
        after = [ "network.target" ];
        wantedBy = [ "multi-user.target" ];
        serviceConfig = {
          ExecStart = "${pkgs.rustfs}/bin/rustfs --address 0.0.0.0:9000 --access-key ${s3AccessKey} --secret-key ${s3SecretKey} /var/lib/rustfs";
          StateDirectory = "rustfs";
          DynamicUser = true;
          Restart = "on-failure";
        };
      };

      systemd.services.rustfs-setup = {
        description = "Setup RustFS bucket";
        after = [ "rustfs.service" ];
        requires = [ "rustfs.service" ];
        before = lib.optional orderNiks3 "niks3.service";
        wantedBy = [ "multi-user.target" ];
        environment = {
          S3_ENDPOINT_URL = "http://localhost:9000";
          AWS_ACCESS_KEY_ID = s3AccessKey;
          AWS_SECRET_ACCESS_KEY = s3SecretKey;
        };
        path = [ pkgs.s5cmd ];
        script = ''
          for i in $(seq 60); do
            s5cmd ls 2>/dev/null && break
            echo "Waiting for RustFS to start... ($i/60)"
            sleep 2
          done
          s5cmd ls >/dev/null
          s5cmd mb s3://${bucket} || true
        '';
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
        };
      };

      systemd.services.niks3 = lib.mkIf orderNiks3 {
        after = [ "rustfs-setup.service" ];
        requires = [ "rustfs-setup.service" ];
      };
    };

  # Trust the test signing key and enable flakes.
  nixSettings = {
    experimental-features = [
      "nix-command"
      "flakes"
    ];
    substituters = lib.mkForce [ ];
    trusted-public-keys = [ signing.publicKey ];
  };
}
