{
  pkgs,
  lib,
  go,
  nix,
}:

let
  vendorHash = lib.fileContents ./goVendorHash.txt;
in
pkgs.buildGoModule {
  pname = "niks3";
  version = "1.2.0";
  src = lib.fileset.toSource {
    fileset = lib.fileset.unions [
      ../../api
      ../../cmd
      ../../client
      ../../server
      ../../go.mod
      ../../go.sum
    ];
    root = ../..;
  };

  inherit vendorHash;

  subPackages = [
    "cmd/niks3"
    "cmd/niks3-server"
  ];

  doCheck = false;

  nativeBuildInputs = [ pkgs.makeWrapper ];

  # Add unittest output for pre-compiled test binaries
  outputs = [
    "out"
    "unittest"
  ];

  # Compile test binaries for the unittest output
  postInstall = ''
    # Compile test binaries (one per package)
    go test -c ./client -o client.test
    go test -c ./server -o server.test
    go test -c ./server/oidc -o server-oidc.test

    # Install test binaries to unittest output
    mkdir -p $unittest/bin
    install -D client.test $unittest/bin/niks3-client.test
    install -D server.test $unittest/bin/niks3-server.test
    install -D server-oidc.test $unittest/bin/niks3-server-oidc.test

    # Remove Go compiler reference to reduce closure size
    if command -v remove-references-to >/dev/null; then
      for f in $unittest/bin/*.test; do
        remove-references-to -t ${go} "$f"
      done
    fi

    # Wrap niks3 client to include nix in PATH
    # This avoids compatibility issues with Lix's different `nix path-info --json` output
    # See: https://github.com/Mic92/niks3/issues/181
    # Skip wrapping in cross-compilation (binary is in a subdirectory)
    if [ -f $out/bin/niks3 ]; then
      wrapProgram $out/bin/niks3 \
        --prefix PATH : ${lib.makeBinPath [ nix ]}
    fi
  '';
}
