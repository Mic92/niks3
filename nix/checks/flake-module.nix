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
        }
        // lib.optionalAttrs (lib.hasSuffix "linux" system) {
          nixos-test-niks3 = pkgs.callPackage ./nixos-test-niks3.nix {
            niks3 = config.packages.niks3;
          };
        };
    };
}
