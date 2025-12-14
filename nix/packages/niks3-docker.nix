{
  niks3,
  coreutils,
  bashInteractive,
  dockerTools,
}:
dockerTools.buildLayeredImage {
  name = "niks3-server";
  tag = niks3.version;
  contents = [
    niks3
    coreutils
    bashInteractive
  ];
  config = {
    Entrypoint = [ "/bin/niks3-server" ];
    ExposedPorts = {
      "5751" = { };
    };
  };
}
