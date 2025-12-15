{
  pkgs,
  niks3,
  dockerTools,
}:
dockerTools.buildLayeredImage {
  name = "niks3";
  tag = "latest";
  contents = [
    niks3
    pkgs.busybox
    pkgs.busybox-sandbox-shell
  ];
  config = {
    Entrypoint = [ "/bin/niks3-server" ];
    ExposedPorts = {
      "5751" = { };
    };
  };
}
