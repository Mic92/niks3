# Helm chart in k3s. Pushes authenticate with a service account token.
{
  testers,
  runCommand,
  kubernetes-helm,
  k3s,
  niks3,
  niks3-docker,
  common,
  stdenv,
  ...
}:
let
  image = niks3-docker.perArch.${stdenv.hostPlatform.system};

  chart = runCommand "niks3-chart.tgz" { nativeBuildInputs = [ kubernetes-helm ]; } ''
    cp -r ${../../deploy/helm/niks3} chart
    chmod -R u+w chart
    helm package chart
    mv niks3-*.tgz $out
  '';

  inherit (common) apiToken;
  hostIP = "192.168.1.1";
  nodePort = 30051;
in
testers.nixosTest {
  name = "nixos-test-k3s";

  nodes.machine =
    { pkgs, ... }:
    {
      imports = [
        (common.rustfsModule {
          bucket = "niks3";
          orderNiks3 = false;
        })
      ];

      virtualisation = {
        memorySize = 3072;
        cores = 2;
        diskSize = 8192;
      };
      networking.firewall.enable = false;
      environment.systemPackages = [
        pkgs.kubectl
        pkgs.kubernetes-helm
        niks3
      ];
      nix.settings.experimental-features = [ "nix-command" ];
      environment.sessionVariables.KUBECONFIG = "/etc/rancher/k3s/k3s.yaml";

      services.postgresql = {
        enable = true;
        enableTCPIP = true;
        ensureDatabases = [ "niks3" ];
        ensureUsers = [
          {
            name = "niks3";
            ensureDBOwnership = true;
          }
        ];
        authentication = "host all all 10.42.0.0/16 trust";
      };

      services.k3s = {
        enable = true;
        role = "server";
        package = k3s;
        disable = [
          "traefik"
          "metrics-server"
          "local-storage"
        ];
        images = [
          k3s.airgap-images
          image
        ];
        autoDeployCharts.niks3 = {
          package = chart;
          targetNamespace = "niks3";
          createNamespace = true;
          values = {
            image = {
              repository = image.imageName;
              tag = image.imageTag;
            };
            database.url = "postgres://niks3@${hostIP}/niks3?sslmode=disable";
            s3 = {
              endpoint = "${hostIP}:9000";
              bucket = "niks3";
              useSSL = false;
              accessKey = common.s3AccessKey;
              secretKey = common.s3SecretKey;
            };
            auth = {
              token = apiToken;
              workloadIdentity = {
                enabled = true;
                allowedServiceAccounts = [ "ci:builder" ];
              };
            };
            deploymentAnnotations."reloader.stakater.com/auto" = "true";
            service = {
              type = "NodePort";
              inherit nodePort;
            };
          };
        };
        manifests.ci.content = [
          {
            apiVersion = "v1";
            kind = "Namespace";
            metadata.name = "ci";
          }
          {
            apiVersion = "v1";
            kind = "ServiceAccount";
            metadata = {
              name = "builder";
              namespace = "ci";
            };
          }
          {
            apiVersion = "v1";
            kind = "ServiceAccount";
            metadata = {
              name = "intruder";
              namespace = "ci";
            };
          }
        ];
      };
    };

  testScript = ''
    machine.wait_for_unit("k3s.service")
    machine.wait_for_unit("rustfs-setup.service")
    machine.wait_for_unit("postgresql.service")

    with subtest("chart deploys and becomes ready"):
      machine.wait_until_succeeds("kubectl -n niks3 rollout status deployment niks3 --timeout=10s", timeout=300)
      machine.succeed("kubectl -n niks3 get deployment niks3 -o jsonpath='{.metadata.annotations.reloader\\.stakater\\.com/auto}' | grep true")
      machine.wait_until_succeeds("curl -sf http://localhost:${toString nodePort}/readyz | grep OK", timeout=60)

    # Same JWT a pod gets from a projected serviceAccountToken volume.
    machine.wait_until_succeeds("kubectl -n ci get sa builder", timeout=60)
    machine.succeed("kubectl -n ci create token builder --audience niks3 > /tmp/builder.jwt")
    machine.succeed("kubectl -n ci create token intruder --audience niks3 > /tmp/intruder.jwt")
    push = "NIKS3_SERVER_URL=http://localhost:${toString nodePort} NIKS3_AUTH_TOKEN_FILE=/tmp/{}.jwt niks3 push {} 2>&1"
    path = machine.succeed("readlink -f /run/current-system/sw/bin/niks3").strip().removesuffix("/bin/niks3")

    with subtest("allowed service account can push via workload identity"):
      print(machine.succeed(push.format("builder", path)))
      hash = path.removeprefix("/nix/store/").split("-")[0]
      machine.succeed(f"curl -sf -H 'Authorization: Bearer ${apiToken}' -I http://localhost:${toString nodePort}/api/objects/{hash}.narinfo")

    with subtest("write scope does not grant admin"):
      status = "curl -s -o /dev/null -w '%{{http_code}}' -H \"Authorization: Bearer $(cat /tmp/{}.jwt)\" http://localhost:${toString nodePort}/api/gc/status"
      assert machine.succeed(status.format("builder")).strip() == "403"

    with subtest("other service accounts are rejected"):
      out = machine.fail(push.format("intruder", path))
      assert "401" in out or "nauthorized" in out, out

    with subtest("gc cronjob runs against the service"):
      machine.succeed("kubectl -n niks3 create job --from=cronjob/niks3-gc gc-manual")
      machine.wait_until_succeeds("kubectl -n niks3 wait --for=condition=complete job/gc-manual --timeout=10s", timeout=120)

    with subtest("helm test hook passes"):
      machine.succeed("helm test -n niks3 niks3 --timeout 2m >&2")
  '';
}
