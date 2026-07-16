#!/usr/bin/env python3
"""gen_fleet.py — emit a fleet of identical Knative ksvcs + one Nimbus CR that
reuses an already-measured profile, so the online path (/decide) actually DECIDES
instead of returning "passthrough".

Why: /decide only decides for a ksvc that (a) is listed in some Nimbus CR's
selector and (b) that Nimbus has a completed profile. All fleet ksvcs share the
measure-yolo image, so ONE preloaded profile (preMeasured.loadFromDir) covers the
whole fleet — no re-measurement. The emitted Nimbus sets online.enabled: true
(the shipped preload sets it false).

Output is one multi-document YAML: N Services + one Nimbus. Apply with:
    kubectl apply -f fleet.yaml
Then wait for the controller log:
    Skipping binary search — all N candidate node(s) saturated

Values below MUST match the run you preload from (metric / SLO / cpuBudget) or the
loaded c_opt/c_min are semantically wrong — see my-boost-preload-yolo.yaml.
"""
import argparse

SVC_TMPL = """apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: {name}
  namespace: {ns}
spec:
  template:
    metadata:
      annotations:
        autoscaling.knative.dev/window: "10s"
        autoscaling.knative.dev/min-scale: "0"
        autoscaling.knative.dev/max-scale: "1"
    spec:
      containerConcurrency: 1
      containers:
        - image: {image}
          resources:
            requests:
              cpu: "{cpu}"
            limits:
              cpu: "{cpu}"
          ports:
            - containerPort: 8080
          env:
            - name: RTMP_STREAM_URL
              value: '{rtmp}'
"""

NIMBUS_TMPL = """apiVersion: lazyken.io/v1alpha1
kind: Nimbus
metadata:
  name: {nimbus}
  namespace: {ns}
selector:
  matchExpressions:
  - key: serving.knative.dev/service
    operator: In
    values: [{values}]
spec:
  placement:
    nodeSelector:
      {pool_key}: {pool_val}
  metric: {metric}
  acceptableResponseTime:
    cold: {cold_slo}
    warm: {warm_slo}
  preMeasured:
    loadFromDir: "{load_dir}"
  online:
    enabled: true          # ON — so /decide runs the waterfall for the fleet
  measurement:
    coldSamples: 20
    warmSamples: 20
  resourcePolicy:
    containerPolicies:
    - containerName: user-container
      cpuBudget: "{cpu_budget}"
  durationPolicy:
    coldApiCondition:
      path: "/status"
      response: "READY"
    warmApiCondition:
      path: "/detect/local"
      statusCode: 200
      bodyContains: "\\"success\\":true"
"""


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--count", type=int, default=20)
    p.add_argument("--base-name", default="loadtest-yolo")
    p.add_argument("--namespace", default="serverless")
    p.add_argument("--image", default="docker.io/lazyken/measure-yolo:v1")
    p.add_argument("--rtmp", default="192.168.17.129:2000")
    p.add_argument("--cpu", default="100m",
                   help="fixed ksvc CPU (requests==limits). For static baselines: "
                        "static-low e.g. 100m, static-high e.g. the measured c_opt.")
    p.add_argument("--no-nimbus", action="store_true",
                   help="emit ONLY the Services (no Nimbus CR) — a true static "
                        "baseline where NIMBUS does not manage the fleet.")
    p.add_argument("--nimbus-name", default="loadtest-boost")
    p.add_argument("--pool-selector", default="nimbus.io/pool=serverless")
    p.add_argument("--load-dir", default="./results/yolo")
    p.add_argument("--metric", default="p95")
    p.add_argument("--cold-slo", type=int, default=16000)
    p.add_argument("--warm-slo", type=int, default=5000)
    p.add_argument("--cpu-budget", default="1200m")
    p.add_argument("--out", default="fleet.yaml")
    args = p.parse_args()

    pool_key, pool_val = args.pool_selector.split("=", 1)
    names = [f"{args.base_name}-{i:03d}" for i in range(1, args.count + 1)]

    docs = [SVC_TMPL.format(name=n, ns=args.namespace, image=args.image,
                            rtmp=args.rtmp, cpu=args.cpu)
            for n in names]
    if not args.no_nimbus:
        docs.append(NIMBUS_TMPL.format(
            nimbus=args.nimbus_name, ns=args.namespace,
            values=", ".join(f'"{n}"' for n in names),
            pool_key=pool_key, pool_val=pool_val,
            metric=args.metric, cold_slo=args.cold_slo, warm_slo=args.warm_slo,
            load_dir=args.load_dir, cpu_budget=args.cpu_budget))

    with open(args.out, "w") as f:
        f.write("---\n".join(docs))

    # A plain ksvc-name list for gen_schedule.py --ksvcs-file.
    with open(args.out + ".ksvcs", "w") as f:
        f.write("\n".join(names) + "\n")

    print(f"[gen_fleet] {args.count} ksvcs + Nimbus '{args.nimbus_name}' -> {args.out}")
    print(f"[gen_fleet] ksvc name list -> {args.out}.ksvcs")
    print(f"[gen_fleet] next: kubectl apply -f {args.out}  (ensure --load-dir '{args.load_dir}' exists)")


if __name__ == "__main__":
    main()
