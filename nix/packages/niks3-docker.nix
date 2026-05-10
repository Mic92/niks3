{
  lib,
  pkgs,
  niks3-server,
  imageName ? "${niks3-server.pname}:latest",
}:
let
  allPlatforms = {
    "x86_64-linux" = {
      GOOS = "linux";
      GOARCH = "amd64";
    };
    "aarch64-linux" = {
      GOOS = "linux";
      GOARCH = "arm64";
    };
  };
  # On macOS, building the cross-arch Linux image segfaults the Python
  # layer streamer intermittently. Restrict darwin builds to the native
  # arch so CI on Apple Silicon still smoke-tests the package; the publish
  # workflow runs on x86_64-linux and produces the full multi-arch index.
  supportedPlatforms =
    if pkgs.stdenv.hostPlatform.isDarwin then
      lib.filterAttrs (n: _: lib.hasPrefix "${pkgs.stdenv.hostPlatform.parsed.cpu.name}-" n) allPlatforms
    else
      allPlatforms;
  platforms = lib.mapAttrs (
    crossSystem:
    { GOOS, GOARCH }:
    let
      inherit (pkgs.stdenv.hostPlatform) system;
      crossPkgs =
        if system == crossSystem then pkgs else (import pkgs.path { inherit system crossSystem; });
    in
    crossPkgs.dockerTools.buildLayeredImage {
      name = niks3-server.pname;
      tag = "${niks3-server.version}-${crossSystem}";
      # The per-arch tarball is an intermediate that regctl decompresses
      # immediately. Skip compression: pigz with -p$NIX_BUILD_CORES has
      # been observed to segfault (likely OOM) on busy CI runners, and
      # compressing here only to decompress in the next step is wasted CPU.
      # The final multi-arch image is compressed by regctl on export.
      compressor = "none";
      contents = [
        (niks3-server.overrideAttrs (old: {
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
        (import ./niks3.nix { inherit pkgs lib; })
      ]
      ++ (with crossPkgs.pkgsStatic; [
        busybox
        busybox-sandbox-shell
        cacert
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
  name = "${niks3-server.pname}-docker";
  inherit (niks3-server) version;
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
