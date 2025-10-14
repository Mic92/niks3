{
  perSystem =
    {
      lib,
      ...
    }:
    let
      initdb = {
        args = [
          "--locale=en_US.UTF-8"
          "--encoding=UTF8"
        ];
        scripts = [
          # create some databases on startup
          "${./init.sql}"
        ];
      };
    in
    {
      # add to dev

      config.process-compose = {

        dev = {
          cli.preHook = ''
            if [ ! -d "$PGDATA" ]; then

              mkdir -p "$PGDATA"

              eval 'initdb --username="$PGUSER" --pwfile=<(printf "%s\n" "$PGPASS") ${lib.concatStringsSep " " initdb.args}'

              cat >> "$PGDATA/postgresql.conf" <<EOF
                port = $PGPORT
                listen_addresses = '$PGLISTEN'
                unix_socket_directories = '$PGHOST'
            EOF

              echo "CREATE DATABASE ''${PGUSER:-$(id -nu)};" | postgres --single -E postgres

              # execute init scripts
              ${lib.concatStringsSep "\n" (
                map (script: "postgres --single -E postgres < ${script}") initdb.scripts
              )}
            fi
          '';
          settings = {
            processes = {
              postgres = {
                command = "cd \"$PGDATA\" && postgres";
                readiness_probe = {
                  exec.command = "pg_isready -h $PGHOST -p $PGPORT -U $PGUSER";
                  initial_delay_seconds = 2;
                  period_seconds = 1;
                  timeout_seconds = 2;
                };
              };
            };
          };
        };
      };
    };
}
