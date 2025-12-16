{
  lib,
  pkgs,
  niks3,
  imageName ? "${niks3.pname}:latest",
}:
let
  supportedPlatforms = {
    "x86_64-linux" = {
      GOOS = "linux";
      GOARCH = "amd64";
    };
    "aarch64-linux" = {
      GOOS = "linux";
      GOARCH = "arm64";
    };
  };
  platforms = lib.mapAttrs (
    crossSystem:
    { GOOS, GOARCH }:
    let
      inherit (pkgs.stdenv.hostPlatform) system;
      crossPkgs =
        if system == crossSystem then pkgs else (import pkgs.path { inherit system crossSystem; });
    in
    crossPkgs.dockerTools.buildLayeredImage {
      name = niks3.pname;
      tag = "${niks3.version}-${crossSystem}";
      contents = [
        (niks3.overrideAttrs (old: {
          env = old.env // {
            inherit GOOS GOARCH;
            CGO_ENABLED = 0;
          };
          postInstall = (old.postInstall or "") + ''
            if [ -d $out/bin/${GOOS}_${GOARCH} ]; then
              mv $out/bin/${GOOS}_${GOARCH}/* $out/bin/
              rmdir $out/bin/${GOOS}_${GOARCH}
            fi
          '';
        }))
      ]
      ++ (with crossPkgs.pkgsStatic; [
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
  ) supportedPlatforms;
in
pkgs.stdenvNoCC.mkDerivation {
  name = "${niks3.pname}-docker";
  inherit (niks3) version;
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
