{
  pkgs,
  lib,
  go,
  postBuildHookSocketPath ? "/run/niks3/upload-to-cache.sock",
}:

let
  vendorHash = lib.fileContents ./goVendorHash.txt;
in
pkgs.buildGoModule {
  pname = "niks3";
  version = "1.4.0";
  src = lib.fileset.toSource {
    fileset = lib.fileset.unions [
      ../../api
      ../../cmd
      ../../client
      ../../ratelimit
      ../../server
      ../../go.mod
      ../../go.sum
    ];
    root = ../..;
  };

  inherit vendorHash;

  ldflags = [
    "-X"
    "main.socketPath=${postBuildHookSocketPath}"
  ];

  subPackages = [
    "cmd/niks3"
    "cmd/niks3-server"
    "cmd/niks3-post-build-hook"
  ];

  doCheck = false;

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
    go test -c ./cmd/niks3-post-build-hook -o post-build-hook.test

    # Install test binaries to unittest output
    mkdir -p $unittest/bin
    install -D client.test $unittest/bin/niks3-client.test
    install -D server.test $unittest/bin/niks3-server.test
    install -D server-oidc.test $unittest/bin/niks3-server-oidc.test
    install -D post-build-hook.test $unittest/bin/niks3-post-build-hook.test

    # Remove Go compiler reference to reduce closure size
    if command -v remove-references-to >/dev/null; then
      for f in $unittest/bin/*.test; do
        remove-references-to -t ${go} "$f"
      done
    fi
  '';
}
