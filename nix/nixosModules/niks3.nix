{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.niks3;
in
{
  options.services.niks3 = {
    enable = lib.mkEnableOption "niks3 server";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.callPackage ../packages/niks3.nix { };
      defaultText = lib.literalExpression "pkgs.callPackage ./niks3.nix { }";
      description = "The niks3 package to use.";
    };

    httpAddr = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1:5751";
      description = "HTTP address to listen on.";
    };

    database = {
      connectionString = lib.mkOption {
        type = lib.types.str;
        default = "postgres:///niks3?user=niks3";
        defaultText = lib.literalExpression ''"postgres:///niks3?user=niks3"'';
        example = "postgres://niks3:password@localhost/niks3?sslmode=disable";
        description = ''
          Postgres connection string.
          When createLocally is true, the default uses Unix socket authentication.
          See https://pkg.go.dev/github.com/lib/pq#hdr-Connection_String_Parameters
        '';
      };

      createLocally = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = ''
          Whether to create the database locally.
          This will create a PostgreSQL database named 'niks3' with a user 'niks3'.
        '';
      };
    };

    s3 = {
      endpoint = lib.mkOption {
        type = lib.types.str;
        example = "s3.amazonaws.com";
        description = "S3 endpoint URL.";
      };

      bucket = lib.mkOption {
        type = lib.types.str;
        example = "niks3-bucket";
        description = "S3 bucket name.";
      };

      useSSL = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Whether to use SSL for S3 connections.";
      };

      accessKeyFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = ''
          Path to file containing S3 access key.
          The file should contain only the access key without any newlines.
        '';
      };

      secretKeyFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = ''
          Path to file containing S3 secret key.
          The file should contain only the secret key without any newlines.
        '';
      };
    };

    apiTokenFile = lib.mkOption {
      type = lib.types.path;
      description = ''
        Path to file containing the API token for authentication.
        The token must be at least 36 characters long.
        The file should contain only the token without any newlines.
      '';
    };

    signKeyFiles = lib.mkOption {
      type = lib.types.listOf lib.types.path;
      default = [ ];
      description = ''
        List of paths to signing key files for signing narinfo files.
        Each file should contain an Ed25519 key in the format "name:base64-key".
        Multiple keys can be provided for key rotation.
      '';
      example = lib.literalExpression ''[ /run/secrets/niks3-sign-key ]'';
    };

    cacheUrl = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "https://cache.example.com";
      description = ''
        Public URL where the binary cache is accessible.
        This is used to generate a landing page (index.html) with usage instructions
        and public keys, which is uploaded to the S3 bucket.
        Also used for redirecting requests to the niks3 server root to the public cache.
      '';
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "niks3";
      description = "User under which the niks3 server runs.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "niks3";
      description = "Group under which the niks3 server runs.";
    };

    gc = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = ''
          Whether to enable automatic garbage collection.
          When enabled, a systemd timer will periodically clean up old closures and orphan objects.
        '';
      };

      olderThan = lib.mkOption {
        type = lib.types.str;
        default = "720h"; # 30 days
        example = "168h"; # 7 days
        description = ''
          Duration string for how old closures must be before being garbage collected.
          Accepts Go duration format (e.g., "720h" for 30 days, "168h" for 7 days).
        '';
      };

      failedUploadsOlderThan = lib.mkOption {
        type = lib.types.str;
        default = "6h";
        example = "12h";
        description = ''
          Duration string for how old failed uploads must be before being cleaned up.
          Accepts Go duration format (e.g., "6h" for 6 hours, "12h" for 12 hours).
          Failed uploads are incomplete or stale upload attempts.
        '';
      };

      schedule = lib.mkOption {
        type = lib.types.str;
        default = "daily";
        example = "*-*-* 02:00:00";
        description = ''
          When to run garbage collection. Uses systemd timer calendar format.
          Default is "daily" (runs once per day at midnight).
          See systemd.time(7) for more details.
        '';
      };

      randomizedDelaySec = lib.mkOption {
        type = lib.types.int;
        default = 1800; # 30 minutes
        description = ''
          Add a randomized delay (in seconds) before starting garbage collection.
          This helps distribute load when multiple instances are running.
        '';
      };
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.s3.accessKeyFile != null;
        message = "services.niks3.s3.accessKeyFile must be set";
      }
      {
        assertion = cfg.s3.secretKeyFile != null;
        message = "services.niks3.s3.secretKeyFile must be set";
      }
    ];

    users.users = lib.mkIf (cfg.user == "niks3") {
      niks3 = {
        group = cfg.group;
        isSystemUser = true;
      };
    };

    users.groups = lib.mkIf (cfg.group == "niks3") {
      niks3 = { };
    };

    services.postgresql = lib.mkIf cfg.database.createLocally {
      enable = true;
      ensureDatabases = [ "niks3" ];
      ensureUsers = [
        {
          name = "niks3";
          ensureDBOwnership = true;
        }
      ];
      # Allow local Unix socket connections
      authentication = ''
        local all niks3 peer
      '';
    };

    systemd.services.niks3 = {
      description = "niks3 server";
      wantedBy = [ "multi-user.target" ];
      after = [ "network.target" ] ++ lib.optional cfg.database.createLocally "postgresql.service";
      requires = lib.optional cfg.database.createLocally "postgresql.service";

      serviceConfig = {
        Type = "simple";
        ExecStart = ''
          ${cfg.package}/bin/niks3-server \
            --db "${cfg.database.connectionString}" \
            --http-addr "${cfg.httpAddr}" \
            --s3-endpoint "${cfg.s3.endpoint}" \
            --s3-bucket "${cfg.s3.bucket}" \
            --s3-use-ssl="${if cfg.s3.useSSL then "true" else "false"}" \
            --s3-access-key-path "${cfg.s3.accessKeyFile}" \
            --s3-secret-key-path "${cfg.s3.secretKeyFile}" \
            --api-token-path "${cfg.apiTokenFile}"${
              lib.optionalString (cfg.cacheUrl != null) ''
                \
                            --cache-url "${cfg.cacheUrl}"''
            }${
              lib.optionalString (cfg.signKeyFiles != [ ]) ''
                \
                           ${lib.concatMapStringsSep " \\\n            " (
                             file: "--sign-key-path \"${file}\""
                           ) cfg.signKeyFiles}''
            }
        '';
        User = cfg.user;
        Group = cfg.group;
        Restart = "always";
        RestartSec = "10s";

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

        # State directory
        StateDirectory = "niks3";
        StateDirectoryMode = "0700";
      };
    };

    systemd.services.niks3-gc = lib.mkIf cfg.gc.enable {
      description = "niks3 garbage collection";
      after = [
        "network.target"
        "niks3.service"
      ];
      requires = [ "niks3.service" ];

      serviceConfig = {
        Type = "oneshot";
        ExecStart = ''
          ${cfg.package}/bin/niks3 gc \
            --server-url "http://${cfg.httpAddr}" \
            --auth-token-path "${cfg.apiTokenFile}" \
            --older-than "${cfg.gc.olderThan}" \
            --failed-uploads-older-than "${cfg.gc.failedUploadsOlderThan}"
        '';
        User = cfg.user;
        Group = cfg.group;

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

    systemd.timers.niks3-gc = lib.mkIf cfg.gc.enable {
      description = "niks3 garbage collection timer";
      wantedBy = [ "timers.target" ];

      timerConfig = {
        OnCalendar = cfg.gc.schedule;
        RandomizedDelaySec = cfg.gc.randomizedDelaySec;
        Persistent = true;
      };
    };
  };
}
