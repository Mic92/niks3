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
              export HOME=$TMPDIR

              # Run all tests with verbose output
              go test -v ./client/... ./server/...

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
