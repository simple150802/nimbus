#!/usr/bin/env python3
"""gen_filler.py — emit a Deployment that RESERVES CPU on the pool nodes to create
controlled contention for the waterfall experiments.

One pod per pool node (podAntiAffinity on hostname), each requesting --cpu
(requests==limits → Guaranteed) but running only `sleep` — so it holds the CPU
RESERVATION without burning actual CPU. NIMBUS's free-math and kube-scheduler both
count the request, so the effective serverless budget drops by --cpu per node.
Sweep --cpu to sweep contention; `kubectl delete -f filler.yaml` to release.

For 3×16-core nodes (~30 CPU usable serverless): --cpu 7000m per node → ~9 CPU
pool effective, the recommended operating point (static-opt overflows, NIMBUS fits).
Sweep {5000m, 7000m, 9000m} for the load-vs-budget frontier.
"""
import argparse

TMPL = """apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {ns}
  labels:
    app: {name}
spec:
  replicas: {replicas}
  selector:
    matchLabels:
      app: {name}
  template:
    metadata:
      labels:
        app: {name}
    spec:
      nodeSelector:
        {pool_key}: {pool_val}
      # One filler pod per node so each pool node loses --cpu of headroom.
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchLabels:
                app: {name}
            topologyKey: kubernetes.io/hostname
      terminationGracePeriodSeconds: 0
      containers:
      - name: filler
        image: {image}
        command: ["sh", "-c", "sleep infinity"]
        resources:
          requests:
            cpu: "{cpu}"
          limits:
            cpu: "{cpu}"
"""


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--cpu", required=True, help="CPU reserved per pool node, e.g. 7000m")
    p.add_argument("--replicas", type=int, default=3, help="= number of pool nodes")
    p.add_argument("--name", default="loadtest-filler")
    p.add_argument("--namespace", default="serverless")
    p.add_argument("--pool-selector", default="nimbus.io/pool=serverless")
    p.add_argument("--image", default="busybox:1.36")
    p.add_argument("--out", default="filler.yaml")
    args = p.parse_args()

    pool_key, pool_val = args.pool_selector.split("=", 1)
    with open(args.out, "w") as f:
        f.write(TMPL.format(name=args.name, ns=args.namespace, replicas=args.replicas,
                            pool_key=pool_key, pool_val=pool_val, image=args.image, cpu=args.cpu))
    print(f"[gen_filler] {args.replicas} pods × {args.cpu} on pool {args.pool_selector} -> {args.out}")
    print(f"[gen_filler] apply:  kubectl apply -f {args.out}")
    print(f"[gen_filler] verify: kubectl get pods -n {args.namespace} -l app={args.name} -o wide")
    print(f"[gen_filler] release: kubectl delete -f {args.out}")


if __name__ == "__main__":
    main()
