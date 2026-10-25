{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.niks3-auto-upload;
  hookPackage = cfg.package.override { postBuildHookSocketPath = cfg.socketPath; };
  # Nix post-build-hook must be a single executable; wrap the subcommand invocation.
  postBuildHookScript = pkgs.writeShellScript "niks3-post-build-hook" ''
    exec ${lib.getExe' hookPackage "niks3-hook"} send "$@"
  '';
in
{
  options.services.niks3-auto-upload = {
    enable = lib.mkEnableOption "niks3 automatic upload via post-build-hook";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.callPackage ../packages/niks3-hook.nix { };
      defaultText = lib.literalExpression "pkgs.callPackage ./niks3-hook.nix { }";
      description = "The niks3-hook package to use.";
    };

    serverUrl = lib.mkOption {
      type = lib.types.str;
      description = "URL of the niks3 server.";
      example = "http://127.0.0.1:5751";
    };

    authTokenFile = lib.mkOption {
      type = lib.types.str;
      description = ''
        Path to a file containing the auth token.
        The file should contain only the token without trailing newlines.
        Use a runtime path (e.g. from sops-nix or agenix), not a Nix store path.
      '';
      example = "/run/secrets/niks3-auth-token";
    };

    socketPath = lib.mkOption {
      type = lib.types.str;
      default = "/run/niks3/upload-to-cache.sock";
      description = "Path to the unix stream socket.";
    };

    batchSize = lib.mkOption {
      type = lib.types.int;
      default = 50;
      description = "Number of store paths to collect before pushing a batch.";
    };

    idleExitTimeout = lib.mkOption {
      type = lib.types.int;
      default = 60;
      description = "Seconds of idle time before the daemon exits. Set to 0 to disable.";
    };

    maxConcurrentUploads = lib.mkOption {
      type = lib.types.int;
      default = 30;
      description = "Maximum number of concurrent uploads.";
    };

    verifyS3Integrity = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Verify that objects in database actually exist in S3 before skipping upload.";
    };

    debug = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Enable debug logging.";
    };

    mtls = {
      enable = lib.mkEnableOption "mTLS client authentication against the niks3 server";

      clientCert = lib.mkOption {
        type = lib.types.str;
        description = ''
          Path to the client certificate file.
          Use a runtime path (e.g. from sops-nix or agenix), not a Nix store path.
        '';
        example = "/run/secrets/niks3/client.crt";
      };

      clientKey = lib.mkOption {
        type = lib.types.str;
        description = ''
          Path to the client private key file.
          Use a runtime path (e.g. from sops-nix or agenix), not a Nix store path.
        '';
        example = "/run/secrets/niks3/client.key";
      };

      caCert = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "Path to a CA certificate to verify the server against (optional).";
        example = "/run/secrets/niks3/ca.crt";
      };
    };
  };

  config = lib.mkIf cfg.enable {
    nix.settings.post-build-hook = toString postBuildHookScript;

    systemd.sockets.niks3-auto-upload = {
      description = "niks3 auto-upload socket";
      wantedBy = [ "sockets.target" ];

      socketConfig = {
        ListenStream = cfg.socketPath;
        SocketUser = "root";
        SocketGroup = "nixbld";
        SocketMode = "0660";
        RemoveOnStop = true;
      };
    };

    systemd.services.niks3-auto-upload = {
      description = "niks3 auto-upload daemon";
      after = [ "network.target" ];
      requires = [ "niks3-auto-upload.socket" ];
      path = [ config.nix.package ];
      # DynamicUser has HOME=/, which nix cannot write its eval cache to.
      environment.XDG_CACHE_HOME = "%C/niks3-hook";

      serviceConfig = {
        Type = "simple";
        ExecStart =
          let
            idleStr = if cfg.idleExitTimeout == 0 then "0" else "${toString cfg.idleExitTimeout}s";
          in
          lib.concatStringsSep " " (
            [
              (lib.getExe' cfg.package "niks3-hook")
              "serve"
              "--server-url"
              (lib.escapeShellArg cfg.serverUrl)
              "--auth-token-path"
              "%d/auth-token"
              "--socket"
              (lib.escapeShellArg cfg.socketPath)
              "--batch-size"
              (toString cfg.batchSize)
              "--idle-exit-timeout"
              idleStr
              "--max-concurrent-uploads"
              (toString cfg.maxConcurrentUploads)
              "--db-path"
              "/var/lib/niks3-hook/upload-queue.db"
            ]
            ++ lib.optional cfg.verifyS3Integrity "--verify-s3-integrity"
            ++ lib.optional cfg.debug "--debug"
            ++ lib.optionals cfg.mtls.enable [
              "--client-cert"
              "%d/client-cert"
              "--client-key"
              "%d/client-key"
            ]
            ++ lib.optionals (cfg.mtls.enable && cfg.mtls.caCert != null) [
              "--ca-cert"
              "%d/ca-cert"
            ]
          );
        Restart = "on-failure";
        RestartSec = "5s";
        # Uploading only needs world-readable store paths and build logs plus
        # nix-daemon access, so there is no reason to hold root.
        DynamicUser = true;
        StateDirectory = "niks3-hook";
        CacheDirectory = "niks3-hook";
        # Secrets are typically root-only (sops-nix, agenix). systemd hands
        # the dynamic user a private copy.
        LoadCredential = [
          "auth-token:${cfg.authTokenFile}"
        ]
        ++ lib.optionals cfg.mtls.enable [
          "client-cert:${cfg.mtls.clientCert}"
          "client-key:${cfg.mtls.clientKey}"
        ]
        ++ lib.optional (cfg.mtls.enable && cfg.mtls.caCert != null) "ca-cert:${cfg.mtls.caCert}";

        # Hardening
        NoNewPrivileges = true;
        PrivateTmp = true;
        PrivateDevices = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        ProtectControlGroups = true;
        ProtectKernelModules = true;
        ProtectKernelTunables = true;
        RestrictAddressFamilies = [
          "AF_UNIX"
          "AF_INET"
          "AF_INET6"
        ];
        RestrictNamespaces = true;
        RestrictRealtime = true;
        RestrictSUIDSGID = true;
        RemoveIPC = true;
        LockPersonality = true;
        SystemCallFilter = [
          "@system-service"
          "~@privileged"
        ];
        SystemCallArchitectures = "native";
      };
    };
  };
}
