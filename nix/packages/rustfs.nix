{
  pkgs,
  lib,
}:

pkgs.rustPlatform.buildRustPackage rec {
  pname = "rustfs";
  version = "1.0.0-alpha.72";

  src = pkgs.fetchFromGitHub {
    owner = "rustfs";
    repo = "rustfs";
    rev = version;
    hash = "sha256-iWaZgvy40RW67oqyVttaWyrFrAVy17UJz5JydI51uDM=";
  };

  patches = [
    ./rustfs-content-encoding.patch
  ];

  cargoHash = "sha256-ApVUUpeLXpMwqRnuNI/Q20/FTEvUyPTtDSpmPsDco2I=";

  nativeBuildInputs = with pkgs; [
    pkg-config
    protobuf
  ];

  buildInputs = with pkgs; [
    openssl
  ];

  # Only build the main rustfs binary
  cargoBuildFlags = [
    "--package"
    "rustfs"
  ];

  # Skip tests for now - they require a full test environment
  doCheck = false;

  meta = {
    description = "High-performance S3-compatible object storage";
    homepage = "https://rustfs.com";
    license = lib.licenses.asl20;
    mainProgram = "rustfs";
  };
}
