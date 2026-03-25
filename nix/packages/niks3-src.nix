# Shared helpers for niks3 Go packages.
# Each package selects only the source directories it needs via fileset,
# so changes to e.g. server/ don't rebuild the hook package.
{
  lib,
}:

let
  root = ../..;
  noTests = lib.fileset.fileFilter (f: !(lib.hasSuffix "_test.go" f.name)) root;

  srcs = {
    api = ../../api;
    client = ../../client;
    cmdutil = ../../cmdutil;
    hook = ../../hook;
    ratelimit = ../../ratelimit;
    server = ../../server;
    cmd-niks3 = ../../cmd/niks3;
    cmd-niks3-server = ../../cmd/niks3-server;
    cmd-niks3-hook = ../../cmd/niks3-hook;
  };
in
{
  vendorHash = lib.fileContents ./goVendorHash.txt;
  vendorHashServer = lib.fileContents ./goVendorHash-server.txt;
  vendorHashHook = lib.fileContents ./goVendorHash-hook.txt;
  vendorHashTests = lib.fileContents ./goVendorHash-tests.txt;

  inherit root srcs;

  # Go module files needed by every package.
  commonFiles = lib.fileset.unions [
    ../../go.mod
    ../../go.sum
  ];

  # Source directories with test files excluded (for binary builds).
  srcsNoTests = lib.mapAttrs (_: dir: lib.fileset.intersection noTests dir) srcs;
}
