{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.niks3-auto-upload;
  # Pass --socket so the client dials cfg.socketPath, not the binary's baked default.
  postBuildHookScript = pkgs.writeShellScript "niks3-post-build-hook" ''
    exec ${lib.getExe' cfg.package "niks3-hook"} send --socket ${lib.escapeShellArg cfg.socketPath} "$@"
  '';

  stateDir = "/var/lib/niks3-hook";
  dbPath = "${stateDir}/upload-queue.db";
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
        Must be readable by root (the launchd daemon runs as root).
      '';
      example = "/run/secrets/niks3-auth-token";
    };

    socketPath = lib.mkOption {
      type = lib.types.str;
      default = "${stateDir}/upload-to-cache.sock";
      defaultText = lib.literalExpression ''"''${stateDir}/upload-to-cache.sock"'';
      description = "Path to the unix stream socket, created by the daemon itself.";
    };

    batchSize = lib.mkOption {
      type = lib.types.int;
      default = 50;
      description = "Number of store paths to collect before pushing a batch.";
    };

    idleExitTimeout = lib.mkOption {
      type = lib.types.int;
      default = 0;
      description = ''
        Seconds of idle time before the daemon exits. Set to 0 to disable.
        Defaults to 0 because launchd (KeepAlive) would respawn the daemon anyway.
      '';
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

    launchd.daemons.niks3-auto-upload = {
      serviceConfig = {
        # Daemon shells out to `nix path-info`; launchd provides no nix in PATH.
        EnvironmentVariables.PATH = "${config.nix.package}/bin:/usr/bin:/bin";

        ProgramArguments =
          let
            idleStr = if cfg.idleExitTimeout == 0 then "0" else "${toString cfg.idleExitTimeout}s";
          in
          [
            (lib.getExe' cfg.package "niks3-hook")
            "serve"
            "--server-url"
            cfg.serverUrl
            "--auth-token-path"
            cfg.authTokenFile
            "--socket"
            cfg.socketPath
            "--batch-size"
            (toString cfg.batchSize)
            "--idle-exit-timeout"
            idleStr
            "--max-concurrent-uploads"
            (toString cfg.maxConcurrentUploads)
            "--db-path"
            dbPath
          ]
          ++ lib.optional cfg.verifyS3Integrity "--verify-s3-integrity"
          ++ lib.optional cfg.debug "--debug"
          ++ lib.optionals cfg.mtls.enable [
            "--client-cert"
            cfg.mtls.clientCert
            "--client-key"
            cfg.mtls.clientKey
          ]
          ++ lib.optionals (cfg.mtls.enable && cfg.mtls.caCert != null) [
            "--ca-cert"
            cfg.mtls.caCert
          ];

        RunAtLoad = true;
        KeepAlive = true;
        StandardOutPath = "/var/log/niks3-auto-upload.log";
        StandardErrorPath = "/var/log/niks3-auto-upload.err";
      };
    };
  };
}
