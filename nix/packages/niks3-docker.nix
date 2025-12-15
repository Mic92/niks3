{
  lib,
  pkgs,
  imageName ? "niks3:latest",
}:
let
  supportedPlatforms = [
    "x86_64-linux"
    "aarch64-linux"
  ];
  platforms = lib.genAttrs supportedPlatforms (
    crossSystem:
    let
      inherit (pkgs.stdenv.hostPlatform) system;
      crossPkgs =
        if system == crossSystem then pkgs else (import pkgs.path { inherit system crossSystem; });
      niks3 = crossPkgs.callPackage ./niks3.nix { };
    in
    crossPkgs.dockerTools.buildLayeredImage {
      name = niks3.pname;
      tag = "${niks3.version}-${crossSystem}";
      contents = [
        niks3
      ]
      ++ (with crossPkgs; [
        busybox
        busybox-sandbox-shell
      ]);
      config = {
        Entrypoint = [ "/bin/niks3-server" ];
        ExposedPorts = {
          "5751" = { };
        };
      };
    }
  );
in
pkgs.stdenvNoCC.mkDerivation {
  name = "niks3-docker";
  phases = [ "installPhase" ];
  src = pkgs.linkFarm "images" (lib.mapAttrsToList (name: path: { inherit name path; }) platforms);
  nativeBuildInputs = [ pkgs.regctl ];
  installPhase = ''
    set -xve
    image_refs=()
    for platform in $src/*; do
      ref_url="ocidir://images:$(basename $platform)"
      image_refs+=("--ref" "$ref_url")
      regctl image import "$ref_url" "$platform"
    done
    regctl index create "ocidir://images:latest" "''${image_refs[@]}"
    regctl image export "ocidir://images:latest" --name "${imageName}" > $out
  '';
}
