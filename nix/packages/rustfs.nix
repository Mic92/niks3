{
  pkgs,
  lib,
}:

pkgs.rustPlatform.buildRustPackage rec {
  pname = "rustfs";
  version = "1.0.0-alpha.79";

  src = pkgs.fetchFromGitHub {
    owner = "rustfs";
    repo = "rustfs";
    rev = version;
    hash = "sha256-f9+khwUOy9qTufhRuZLGNddA9RCNCkE8XdhIpo5X/7U=";
  };

  cargoHash = "sha256-L3g794CNVdWHJAPUBkULxJn8d6pOWOJ65YmgnX9d8p8=";

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
