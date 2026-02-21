{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.niks3-auto-upload;
  hookPackage = cfg.package.override { postBuildHookSocketPath = cfg.socketPath; };
in
{
  options.services.niks3-auto-upload = {
    enable = lib.mkEnableOption "niks3 automatic upload via post-build-hook";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.callPackage ../packages/niks3.nix { };
      defaultText = lib.literalExpression "pkgs.callPackage ./niks3.nix { }";
      description = "The niks3 package to use.";
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
      description = "Path to the unix datagram socket.";
    };

    batchSize = lib.mkOption {
      type = lib.types.int;
      default = 50;
      description = "Number of store paths to collect before pushing a batch.";
    };

    batchTimeout = lib.mkOption {
      type = lib.types.int;
      default = 10;
      description = "Seconds to wait before pushing a partial batch.";
    };

    idleExitTimeout = lib.mkOption {
      type = lib.types.int;
      default = 60;
      description = "Seconds of idle time before the listener exits. Set to 0 to disable.";
    };

    maxErrors = lib.mkOption {
      type = lib.types.int;
      default = 5;
      description = "Exit after this many consecutive push errors.";
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
  };

  config = lib.mkIf cfg.enable {
    nix.settings.post-build-hook = lib.getExe' hookPackage "niks3-post-build-hook";

    systemd.sockets.niks3-auto-upload = {
      description = "niks3 auto-upload socket";
      wantedBy = [ "sockets.target" ];

      socketConfig = {
        ListenDatagram = cfg.socketPath;
        SocketUser = "root";
        SocketGroup = "nixbld";
        SocketMode = "0660";
        ReceiveBuffer = "50M";
        RemoveOnStop = true;
      };
    };

    systemd.services.niks3-auto-upload = {
      description = "niks3 auto-upload listener";
      after = [ "network.target" ];
      requires = [ "niks3-auto-upload.socket" ];
      path = [ config.nix.package ];

      serviceConfig = {
        Type = "simple";
        ExecStart =
          let
            idleStr = if cfg.idleExitTimeout == 0 then "0" else "${toString cfg.idleExitTimeout}s";
          in
          lib.concatStringsSep " " (
            [
              (lib.getExe' cfg.package "niks3")
              "listen"
              "--server-url"
              (lib.escapeShellArg cfg.serverUrl)
              "--auth-token-path"
              (lib.escapeShellArg cfg.authTokenFile)
              "--socket"
              (lib.escapeShellArg cfg.socketPath)
              "--batch-size"
              (toString cfg.batchSize)
              "--batch-timeout"
              "${toString cfg.batchTimeout}s"
              "--idle-exit-timeout"
              idleStr
              "--max-errors"
              (toString cfg.maxErrors)
              "--max-concurrent-uploads"
              (toString cfg.maxConcurrentUploads)
            ]
            ++ lib.optional cfg.verifyS3Integrity "--verify-s3-integrity"
            ++ lib.optional cfg.debug "--debug"
          );
        Restart = "on-failure";
        RestartSec = "5s";

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
