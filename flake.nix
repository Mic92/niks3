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

      lib = nixpkgs.lib;

      forAllSystems = f: lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      nixosModules = {
        niks3 = ./nix/nixosModules/niks3.nix;
        niks3-auto-upload = ./nix/nixosModules/niks3-auto-upload.nix;
        default = ./nix/nixosModules/niks3.nix;
      };

      packages = forAllSystems (pkgs: import ./nix/packages { inherit pkgs; });

      checks = forAllSystems (
        pkgs:
        import ./nix/checks {
          inherit pkgs;
          selfPackages = inputs.self.packages.${pkgs.system};
          selfDevShells = inputs.self.devShells.${pkgs.system} or { };
          treefmtCheck = (import ./nix/formatter {
            inherit pkgs;
            inherit (inputs) treefmt-nix;
          }).check inputs.self;
        }
      );

      devShells = forAllSystems (
        pkgs:
        import ./nix/devshells {
          inherit pkgs;
          selfPackages = inputs.self.packages.${pkgs.system};
        }
      );

      formatter = forAllSystems (
        pkgs:
        (import ./nix/formatter {
          inherit pkgs;
          inherit (inputs) treefmt-nix;
        }).wrapper
      );
    };
}
