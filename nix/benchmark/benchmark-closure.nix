{ pkgs }:
# Create a Python environment with several popular packages
# to generate a reasonably large closure for benchmarking
pkgs.python3.withPackages (
  ps: with ps; [
    # Web frameworks and HTTP libraries
    requests
    flask
    django

    numpy
    pandas
    scipy

    pillow
    cryptography
    pyyaml
  ]
)
