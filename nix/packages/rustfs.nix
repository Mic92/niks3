{
  pkgs,
  lib,
}:

pkgs.rustPlatform.buildRustPackage rec {
  pname = "rustfs";
  version = "1.0.0-beta.7";

  src = pkgs.fetchFromGitHub {
    owner = "rustfs";
    repo = "rustfs";
    rev = version;
    hash = "sha256-abDQdD0Ws4brB4xbF2XFyb0M6sKUZ6GwNMAdV4/sC3c=";
  };

  cargoHash = "sha256-YNtpiAfq+6Ynd444fPA/6RDNWbnrnA8i4AXsjLCbKhs=";

  nativeBuildInputs = with pkgs; [
    pkg-config
    protobuf
  ];

  buildInputs = with pkgs; [
    openssl
  ];

  # Upstream .cargo/config.toml enables tokio_unstable for io-uring support;
  # buildRustPackage overrides cargo config, so set it explicitly here.
  RUSTFLAGS = "--cfg tokio_unstable";

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
