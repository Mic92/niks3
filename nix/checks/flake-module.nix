{ ... }:
{
  perSystem =
    {
      self',
      lib,
      pkgs,
      config,
      system,
      ...
    }:
    {
      checks =
        let
          packages = lib.mapAttrs' (n: lib.nameValuePair "package-${n}") self'.packages;
          devShells = lib.mapAttrs' (n: lib.nameValuePair "devShell-${n}") self'.devShells;
        in
        packages
        // devShells
        // {
          golangci-lint = config.packages.niks3-tests.overrideAttrs (old: {
            nativeBuildInputs = old.nativeBuildInputs ++ [ pkgs.golangci-lint ];
            buildPhase = ''
              HOME=$TMPDIR
              golangci-lint run
            '';
            installPhase = ''
              touch $out
            '';
          });

          # Run pre-compiled Go test binaries with rustfs and postgres.
          # Test binaries are built by niks3-tests and can be substituted
          # from cache, avoiding rebuilding rustfs locally.
          go-unit-tests =
            pkgs.runCommand "niks3-go-unit-tests"
              {
                nativeBuildInputs = [
                  config.packages.niks3-tests
                  config.packages.rustfs
                  pkgs.postgresql
                  pkgs.nix
                ];
                # Allow network access for integration tests on macOS
                __darwinAllowLocalNetworking = true;
              }
              ''
                export HOME=$TMPDIR

                echo "Running client tests..."
                niks3-client.test -test.v

                echo "Running server tests..."
                niks3-server.test -test.v

                echo "Running OIDC tests..."
                niks3-server-oidc.test -test.v

                echo "Running hook tests..."
                niks3-hook.test -test.v

                echo "Running cmd tests..."
                niks3-cmd.test -test.v

                touch $out
              '';

          # Fails if the committed dist/index.js doesn't match a fresh
          # build. Catches forgotten regeneration after version bumps or
          # action/src changes.
          action-dist-fresh =
            pkgs.runCommand "niks3-action-dist-fresh"
              {
                fresh = config.packages.niks3-action;
                committed = ../../dist/index.js;
              }
              ''
                if ! diff -u "$committed" "$fresh/dist/index.js"; then
                  echo
                  echo "dist/index.js is stale. Regenerate with:"
                  echo "  nix build .#niks3-action && cp result/dist/index.js dist/"
                  exit 1
                fi
                touch $out
              '';
        }
        // lib.optionalAttrs (lib.hasSuffix "linux" system) {
          nixos-test-niks3 = pkgs.callPackage ./nixos-test-niks3.nix {
            mock-oidc-server = config.packages.mock-oidc-server;
            niks3 = config.packages.niks3;
            niks3-hook = config.packages.niks3-hook;
            rustfs = config.packages.rustfs;
            nix = pkgs.nixVersions.latest;
            ca-derivations-supported = true;
          };
          nixos-test-niks3-lix = pkgs.callPackage ./nixos-test-niks3.nix {
            mock-oidc-server = config.packages.mock-oidc-server;
            niks3 = config.packages.niks3;
            niks3-hook = config.packages.niks3-hook;
            rustfs = config.packages.rustfs;
            nix = pkgs.lixPackageSets.latest.lix;
            ca-derivations-supported = false;
          };
          nixos-test-read-proxy = pkgs.callPackage ./nixos-test-read-proxy.nix {
            niks3 = config.packages.niks3;
            rustfs = config.packages.rustfs;
          };
        };
    };
}
