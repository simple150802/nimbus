#!/usr/bin/env python3
"""set_baseline.py — pin the managed apps to a STATIC CPU policy for baselines.

For each Nimbus, reads its converged profile from `.status.perNode`, then for every
ksvc it manages sets a FIXED cold (StartupCPUBoost CR) + warm (ksvc spec) CPU and
turns `online.enabled=false` so the waterfall doesn't override it. This is the
"what boost level does each baseline get" knob:

  --policy opt      cold=c_opt_cold, warm=c_opt_warm   (profiled, generous)
  --policy min      cold=c_min_cold, warm=c_min_warm   (profiled, tight = max density)
  --policy uniform  cold=warm=--cpu                    (no profiling, one-size-fits-all)
  --policy nimbus   just online.enabled=true           (restore adaptive — undo a baseline)

Uses kubectl (run on the control-plane node). Patching a ksvc's CPU creates a new
Knative revision (one-time). Restore with `--policy nimbus` then let NIMBUS reconcile.
"""
import argparse
import json
import subprocess
import sys


def sh(args, check=True):
    r = subprocess.run(args, capture_output=True, text=True)
    if check and r.returncode != 0:
        print(f"  ! {' '.join(args[:4])}... -> {r.stderr.strip()}", file=sys.stderr)
    return r


def get_json(args):
    r = sh(args, check=False)
    if r.returncode != 0:
        return None
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        return None


def representative_profile(nb, ns):
    """Return (startingCpu, runningCpu, cMinStarting, cMinRunning) from the first
    perNode entry with a complete c_opt pair, or None."""
    obj = get_json(["kubectl", "get", "nimbus", nb, "-n", ns, "-o", "json"])
    if not obj:
        return None, None
    per = (obj.get("status", {}) or {}).get("perNode", {}) or {}
    ksvcs = []
    try:
        ksvcs = obj["selector"]["matchExpressions"][0]["values"]
    except (KeyError, IndexError):
        pass
    for node in sorted(per):
        r = per[node] or {}
        if r.get("startingCpu") and r.get("runningCpu"):
            return {
                "opt_cold": r.get("startingCpu"), "opt_warm": r.get("runningCpu"),
                "min_cold": r.get("cMinStarting") or r.get("startingCpu"),
                "min_warm": r.get("cMinRunning") or r.get("runningCpu"),
            }, ksvcs
    return None, ksvcs


def patch_boost(nb, ksvc, ns, cpu):
    name = f"{nb}-{ksvc}"
    sh(["kubectl", "patch", "startupcpuboost", name, "-n", ns, "--type=json", "-p",
        json.dumps([
            {"op": "replace", "path": "/spec/resourcePolicy/containerPolicies/0/fixedResources/requests", "value": cpu},
            {"op": "replace", "path": "/spec/resourcePolicy/containerPolicies/0/fixedResources/limits", "value": cpu},
        ])])


def patch_ksvc_cpu(ksvc, ns, cpu):
    sh(["kubectl", "patch", "ksvc", ksvc, "-n", ns, "--type=json", "-p",
        json.dumps([
            {"op": "add", "path": "/spec/template/spec/containers/0/resources/requests/cpu", "value": cpu},
            {"op": "add", "path": "/spec/template/spec/containers/0/resources/limits/cpu", "value": cpu},
        ])])


def set_online(nb, ns, enabled):
    sh(["kubectl", "patch", "nimbus", nb, "-n", ns, "--type=merge", "-p",
        json.dumps({"spec": {"online": {"enabled": enabled}}})])


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--policy", required=True, choices=["opt", "min", "uniform", "nimbus"])
    p.add_argument("--nimbuses", required=True, help="comma list, e.g. boost-001,boost-002")
    p.add_argument("--namespace", default="serverless")
    p.add_argument("--cpu", help="fixed CPU for --policy uniform, e.g. 2000m")
    args = p.parse_args()

    if args.policy == "uniform" and not args.cpu:
        p.error("--policy uniform requires --cpu")
    nbs = [n.strip() for n in args.nimbuses.split(",") if n.strip()]

    for nb in nbs:
        if args.policy == "nimbus":
            set_online(nb, args.namespace, True)
            print(f"[{nb}] online.enabled=true (restored adaptive)")
            continue

        prof, ksvcs = representative_profile(nb, args.namespace)
        if not ksvcs:
            print(f"[{nb}] no ksvcs / no selector — skip", file=sys.stderr)
            continue
        if args.policy == "uniform":
            cold = warm = args.cpu
        else:
            if not prof:
                print(f"[{nb}] no complete profile in .status.perNode — skip", file=sys.stderr)
                continue
            cold = prof[f"{args.policy}_cold"]
            warm = prof[f"{args.policy}_warm"]

        set_online(nb, args.namespace, False)
        for ksvc in ksvcs:
            patch_boost(nb, ksvc, args.namespace, cold)
            patch_ksvc_cpu(ksvc, args.namespace, warm)
        print(f"[{nb}] policy={args.policy} cold={cold} warm={warm} ksvcs={len(ksvcs)} (online OFF)")

    print("\nDone. Replay with --no-decide for these apps. Restore later with "
          "`--policy nimbus`.")


if __name__ == "__main__":
    main()
