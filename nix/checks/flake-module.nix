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
            buildPhase = ''
              HOME=$TMPDIR
              golangci-lint run
            '';
            installPhase = ''
              touch $out
            '';
          });

          go-unit-tests = config.packages.niks3.overrideAttrs (_old: {
            # Don't restrict to subPackages for tests
            subPackages = [ ];
            # Allow network access for integration tests on macOS
            __darwinAllowLocalNetworking = true;
            # Don't build binaries
            buildPhase = ''
              runHook preBuild
              runHook postBuild
            '';
            # Run all tests in checkPhase
            checkPhase = ''
              runHook preCheck
              # Set up test environment with absolute paths
              export HOME=$TMPDIR
              export TEST_ROOT=$(mktemp -d)
              # Resolve symlinks (important on macOS where /tmp -> /private/tmp)
              export TEST_ROOT=$(cd "$TEST_ROOT" && pwd -P)
              export NIX_STORE_DIR=$TEST_ROOT/store
              export NIX_DATA_DIR=$TEST_ROOT/share
              export NIX_LOG_DIR=$TEST_ROOT/var/log/nix
              export NIX_STATE_DIR=$TEST_ROOT/state
              export NIX_CONF_DIR=$TEST_ROOT/etc
              export XDG_CACHE_HOME=$TEST_ROOT/cache
              export NIX_CONFIG="substituters =
              connect-timeout = 0
              sandbox = false"
              export _NIX_TEST_NO_SANDBOX=1
              export NIX_REMOTE=""

              # Create required directories
              mkdir -p $NIX_STORE_DIR \
                       $NIX_DATA_DIR \
                       $NIX_LOG_DIR/drvs \
                       $NIX_STATE_DIR/nix/profiles \
                       $NIX_CONF_DIR \
                       $XDG_CACHE_HOME

              # Run all tests with verbose output
              go test -v ./client/... ./server/...

              # Cleanup
              rm -rf "$TEST_ROOT"
              runHook postCheck
            '';
            installPhase = ''
              touch $out
            '';
          });
        }
        // lib.optionalAttrs (lib.hasSuffix "linux" system) {
          nixos-test-niks3 = pkgs.callPackage ./nixos-test-niks3.nix {
            niks3 = config.packages.niks3;
          };
        };
    };
}
