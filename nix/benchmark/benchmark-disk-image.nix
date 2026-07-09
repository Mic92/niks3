{ pkgs }:
# Ext4 image with a small closure: a single large NAR that exercises
# streaming compression and multipart upload.
pkgs.callPackage "${pkgs.path}/nixos/lib/make-ext4-fs.nix" {
  storePaths = [
    (pkgs.python3.withPackages (
      ps: with ps; [
        numpy
        pandas
      ]
    ))
  ];
  volumeLabel = "niks3-bench";
}
