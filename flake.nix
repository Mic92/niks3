{
  description = "Development environment for this project";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable-small";

    treefmt-nix.url = "github:numtide/treefmt-nix";
    treefmt-nix.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    inputs@{ nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f system nixpkgs.legacyPackages.${system});
    in
    {
      nixosModules = {
        niks3 = ./nix/nixosModules/niks3.nix;
        niks3-auto-upload = ./nix/nixosModules/niks3-auto-upload.nix;
        default = ./nix/nixosModules/niks3.nix;
      };

      packages = forAllSystems (_: pkgs: import ./nix/packages { inherit pkgs; });

      checks = forAllSystems (
        system: pkgs:
        import ./nix/checks {
          inherit pkgs;
          selfPackages = inputs.self.packages.${system};
          selfDevShells = inputs.self.devShells.${system} or { };
          treefmtCheck =
            (import ./nix/formatter {
              inherit pkgs;
              inherit (inputs) treefmt-nix;
            }).check
              inputs.self;
        }
      );

      devShells = forAllSystems (
        system: pkgs:
        import ./nix/devshells {
          inherit pkgs;
          selfPackages = inputs.self.packages.${system};
        }
      );

      formatter = forAllSystems (
        _: pkgs:
        (import ./nix/formatter {
          inherit pkgs;
          inherit (inputs) treefmt-nix;
        }).wrapper
      );
    };
}
