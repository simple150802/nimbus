#!/usr/bin/env python3
"""sample_resources.py — snapshot pool-node CPU while a schedule runs.

Records, per pool node, per tick:
  - alloc_m      : allocatable CPU (millicores, constant)
  - requested_m  : Σ CPU requests of pods bound to the node (what the scheduler
                   reserves — this is what NIMBUS's tier decisions actually cost)
  - used_m       : actual CPU usage from `kubectl top` (needs metrics-server)
  - free_m       : alloc_m - requested_m
  - pods         : pod count on the node

This is the GROUND-TRUTH, config-agnostic view (it does not use NIMBUS's own
snapshot), so NIMBUS and the static baselines are measured the same way. Run it
in parallel with replay.py (same window), once per config, then compare
requested_m / CPU-seconds across configs to show NIMBUS reserves less CPU for the
same cold-start latency.

Rows are timestamped with wall-clock epoch to join against replay.py's
send_wallclock column on a common timeline.

Uses kubectl (run on the control-plane node). No Python deps beyond stdlib.
"""
import argparse
import csv
import json
import subprocess
import time


def _run(args):
    return subprocess.run(args, capture_output=True, text=True)


def parse_cpu_milli(s):
    """Parse a k8s CPU quantity (or `kubectl top` value) to millicores."""
    if s is None:
        return 0
    s = str(s).strip()
    if not s or s == "<none>":
        return 0
    try:
        if s.endswith("m"):
            return int(float(s[:-1]))
        if s.endswith("n"):  # nanocores (metrics-server)
            return int(float(s[:-1]) / 1e6)
        if s.endswith("u"):  # microcores
            return int(float(s[:-1]) / 1e3)
        return int(float(s) * 1000)  # plain cores
    except ValueError:
        return 0


def pool_nodes(selector):
    out = _run(["kubectl", "get", "nodes", "-l", selector, "-o", "json"])
    nodes = {}
    if out.returncode != 0:
        return nodes
    try:
        data = json.loads(out.stdout)
    except json.JSONDecodeError:
        return nodes
    for n in data.get("items", []):
        name = n["metadata"]["name"]
        alloc = parse_cpu_milli(n.get("status", {}).get("allocatable", {}).get("cpu"))
        nodes[name] = alloc
    return nodes


def requested_by_node(node_set):
    """Σ CPU requests + pod count per node, over Running/Pending pods (all namespaces)."""
    out = _run(["kubectl", "get", "pods", "-A", "-o", "json"])
    req = {n: 0 for n in node_set}
    cnt = {n: 0 for n in node_set}
    if out.returncode != 0:
        return req, cnt
    try:
        data = json.loads(out.stdout)
    except json.JSONDecodeError:
        return req, cnt
    for p in data.get("items", []):
        nn = p.get("spec", {}).get("nodeName")
        if nn not in node_set:
            continue
        if p.get("status", {}).get("phase") not in ("Running", "Pending"):
            continue
        c = 0
        for ct in p.get("spec", {}).get("containers", []):
            c += parse_cpu_milli(ct.get("resources", {}).get("requests", {}).get("cpu"))
        req[nn] += c
        cnt[nn] += 1
    return req, cnt


def used_by_node(node_set):
    """Actual CPU usage per node from `kubectl top nodes` (empty if no metrics-server)."""
    out = _run(["kubectl", "top", "nodes", "--no-headers"])
    used = {}
    if out.returncode != 0:
        return used
    for line in out.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[0] in node_set:
            used[parts[0]] = parse_cpu_milli(parts[1])
    return used


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pool-selector", default="nimbus.io/pool=serverless")
    p.add_argument("--interval", type=float, default=2.0, help="seconds between samples")
    p.add_argument("--duration", type=float, default=320.0, help="total sampling window (s)")
    p.add_argument("--label", default="nimbus", help="config label written to each row")
    p.add_argument("--no-top", action="store_true", help="skip kubectl top (no metrics-server)")
    p.add_argument("--out", default="resources.csv")
    args = p.parse_args()

    nodes = pool_nodes(args.pool_selector)
    if not nodes:
        print(f"[sample] no nodes match {args.pool_selector} — check the label")
        return
    node_set = set(nodes)
    print(f"[sample] label={args.label} nodes={sorted(nodes)} "
          f"interval={args.interval}s duration={args.duration}s top={not args.no_top}")

    rows = []
    start = time.monotonic()
    while time.monotonic() - start < args.duration:
        ts = time.time()
        req, cnt = requested_by_node(node_set)
        used = {} if args.no_top else used_by_node(node_set)
        for n, alloc in sorted(nodes.items()):
            rows.append({
                "label": args.label, "epoch": f"{ts:.3f}", "node": n,
                "alloc_m": alloc, "requested_m": req.get(n, 0),
                "used_m": used.get(n, ""), "free_m": max(0, alloc - req.get(n, 0)),
                "pods": cnt.get(n, 0),
            })
        due = start + (len(rows) / max(1, len(nodes))) * args.interval
        sleep = due - time.monotonic()
        if sleep > 0:
            time.sleep(sleep)

    cols = ["label", "epoch", "node", "alloc_m", "requested_m", "used_m", "free_m", "pods"]
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

    # Quick summary: peak + mean requested CPU summed across the pool.
    by_ts = {}
    for r in rows:
        by_ts.setdefault(r["epoch"], 0)
        by_ts[r["epoch"]] += r["requested_m"]
    if by_ts:
        series = list(by_ts.values())
        peak = max(series)
        mean = sum(series) / len(series)
        print(f"[sample] pool requested CPU: peak={peak}m mean={mean:.0f}m "
              f"over {len(series)} ticks -> {args.out}")
    else:
        print(f"[sample] wrote {len(rows)} rows -> {args.out}")


if __name__ == "__main__":
    main()
