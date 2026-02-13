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
          golangci-lint = config.packages.niks3.overrideAttrs (old: {
            nativeBuildInputs = old.nativeBuildInputs ++ [ pkgs.golangci-lint ];
            outputs = [ "out" ];
            buildPhase = ''
              HOME=$TMPDIR
              golangci-lint run
            '';
            installPhase = ''
              touch $out
            '';
          });

          # Run pre-compiled Go test binaries with rustfs and postgres
          # The test binaries are built as part of niks3 package (unittest output)
          # and can be substituted from cache, avoiding rebuilding rustfs locally
          go-unit-tests =
            pkgs.runCommand "niks3-go-unit-tests"
              {
                nativeBuildInputs = [
                  config.packages.niks3.unittest
                  config.packages.rustfs
                  pkgs.postgresql
                  pkgs.nix
                ];
                # Allow network access for integration tests on macOS
                __darwinAllowLocalNetworking = true;
              }
              ''
                export HOME=$TMPDIR

                # Run the pre-compiled test binaries
                echo "Running client tests..."
                niks3-client.test -test.v

                echo "Running server tests..."
                niks3-server.test -test.v

                echo "Running OIDC tests..."
                niks3-server-oidc.test -test.v

                touch $out
              '';
        }
        // lib.optionalAttrs (lib.hasSuffix "linux" system) {
          nixos-test-niks3 = pkgs.callPackage ./nixos-test-niks3.nix {
            mock-oidc-server = config.packages.mock-oidc-server;
            niks3 = config.packages.niks3;
            rustfs = config.packages.rustfs;
            nix = pkgs.nixVersions.latest;
            ca-derivations-supported = true;
          };
          nixos-test-niks3-lix = pkgs.callPackage ./nixos-test-niks3.nix {
            mock-oidc-server = config.packages.mock-oidc-server;
            niks3 = config.packages.niks3;
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
