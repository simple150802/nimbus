#!/usr/bin/env python3
"""analyze.py — turn replay + resource CSVs into a comparison table (and plots).

Reads one or more `res_<config>.csv` (replay.py output) and their sibling
`res_<config>.nodes.csv` (sample_resources.py output), groups rows by the `label`
column, and computes per config:

  cold-start latency : p50 / p95 / p99 / mean over served (HTTP 200) events
  admission          : served, pending, passthrough, failed counts + served %
  reserved CPU       : peak / mean pool requested_m, and CPU-seconds (integral)
                       — both TOTAL and ABOVE-BASELINE (system floor subtracted)
  utilization        : mean used_m / mean requested_m (needs metrics-server)

Outputs a printed table + `<prefix>_summary.csv` + `<prefix>_timeseries.csv`
(for external plotting). With --plot (and matplotlib installed) it also writes:
  <prefix>_reserved.png   reserved CPU over time, one line per config
  <prefix>_frontier.png   p95 latency vs CPU-seconds (the efficiency frontier)
  <prefix>_cpusec.png     CPU-seconds bar per config

You do NOT need every config measured first — it analyses whatever is present.
Run it after each config to accumulate; the comparison figures need >= 2 configs.
"""
import argparse
import csv
import glob
import os
import statistics as st


def load_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def pct(vals, p):
    if not vals:
        return None
    s = sorted(vals)
    k = min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1))))
    return s[k]


def replay_metrics(rows):
    total = len(rows)
    served = [r for r in rows if r.get("http_code") == "200" and r.get("latency_ms")]
    lat = [float(r["latency_ms"]) for r in served]
    dec = {}
    for r in rows:
        dec[r.get("decision", "")] = dec.get(r.get("decision", ""), 0) + 1
    return {
        "events": total,
        "served": len(served),
        "served_pct": (100.0 * len(served) / total) if total else 0.0,
        "failed": total - len(served),
        "admit": dec.get("admit", 0),
        "pending": dec.get("pending", 0),
        "passthrough": dec.get("passthrough", 0),
        "lat_p50": pct(lat, 50), "lat_p95": pct(lat, 95),
        "lat_p99": pct(lat, 99),
        "lat_mean": (sum(lat) / len(lat)) if lat else None,
    }


def nodes_metrics(rows):
    """Pool-total requested/used per tick (epoch), then peak/mean/CPU-seconds."""
    by_epoch_req, by_epoch_used = {}, {}
    for r in rows:
        e = r["epoch"]
        by_epoch_req[e] = by_epoch_req.get(e, 0) + int(r["requested_m"])
        u = r.get("used_m", "")
        if u not in ("", None):
            by_epoch_used[e] = by_epoch_used.get(e, 0) + int(u)
    if not by_epoch_req:
        return None, []
    keys = sorted(by_epoch_req, key=lambda k: float(k))  # epoch keys are raw strings
    req = [by_epoch_req[k] for k in keys]
    used = [by_epoch_used.get(k) for k in keys]
    duration = float(keys[-1]) - float(keys[0]) if len(keys) > 1 else 0.0
    # floor = system baseline (requested when no experiment pods are up). 10th
    # percentile, not min(), so a single transient low kubectl read doesn't
    # inflate the above-baseline numbers.
    floor = pct(req, 10)
    mean_req = sum(req) / len(req)
    mean_attr = mean_req - floor
    used_present = [u for u in used if u is not None]
    mean_used = (sum(used_present) / len(used_present)) if used_present else None
    m = {
        "peak_req": max(req), "mean_req": mean_req, "floor": floor,
        "peak_attr": max(req) - floor, "mean_attr": mean_attr,
        "cpu_sec": mean_req * duration,
        "cpu_sec_attr": mean_attr * duration,
        "duration_s": duration,
        "mean_used": mean_used,
        "util": (mean_used / mean_req) if (mean_used and mean_req) else None,
    }
    t0 = float(keys[0])
    series = [(float(k) - t0, by_epoch_req[k], by_epoch_used.get(k)) for k in keys]
    return m, series


def fmt(v, nd=0):
    if v is None:
        return "-"
    return f"{v:.{nd}f}"


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("results", nargs="*", help="res_<config>.csv files (default: glob res_*.csv)")
    p.add_argument("--prefix", default="analysis", help="output file prefix")
    p.add_argument("--plot", action="store_true", help="also write PNGs (needs matplotlib)")
    args = p.parse_args()

    files = args.results or [f for f in sorted(glob.glob("res_*.csv"))
                             if not f.endswith(".nodes.csv")]
    if not files:
        print("no res_*.csv found — pass files or run from the results dir")
        return

    configs = {}   # label -> {"replay": metrics, "nodes": metrics, "series": [...]}
    for rf in files:
        rows = load_csv(rf)
        if not rows:
            continue
        label = rows[0].get("label") or os.path.basename(rf)
        entry = configs.setdefault(label, {"replay_rows": [], "nodes": None, "series": []})
        entry["replay_rows"].extend(rows)
        nf = rf[:-4] + ".nodes.csv"   # res_mix.csv -> res_mix.nodes.csv
        if os.path.exists(nf):
            nm, series = nodes_metrics(load_csv(nf))
            entry["nodes"] = nm
            entry["series"] = series

    # Finalise replay metrics.
    for label, e in configs.items():
        e["replay"] = replay_metrics(e["replay_rows"])

    # ---- printed table ---------------------------------------------------
    hdr = ["config", "events", "served%", "p50ms", "p95ms", "peakCPUm",
           "meanCPUm", "meanAttrm", "CPU-s", "CPU-s(attr)", "util"]
    widths = [16, 7, 8, 8, 8, 9, 9, 10, 9, 12, 6]
    line = "  ".join(h.ljust(w) for h, w in zip(hdr, widths))
    print(line)
    print("-" * len(line))
    summary_rows = []
    for label in sorted(configs):
        e = configs[label]
        r, n = e["replay"], e["nodes"] or {}
        cells = [
            label, str(r["events"]), fmt(r["served_pct"], 1),
            fmt(r["lat_p50"]), fmt(r["lat_p95"]),
            fmt(n.get("peak_req")), fmt(n.get("mean_req")), fmt(n.get("mean_attr")),
            fmt(n.get("cpu_sec")), fmt(n.get("cpu_sec_attr")), fmt(n.get("util"), 2),
        ]
        print("  ".join(c.ljust(w) for c, w in zip(cells, widths)))
        summary_rows.append({
            "config": label, "events": r["events"], "served": r["served"],
            "served_pct": r["served_pct"], "pending": r["pending"],
            "passthrough": r["passthrough"], "failed": r["failed"],
            "lat_p50": r["lat_p50"], "lat_p95": r["lat_p95"], "lat_p99": r["lat_p99"],
            "lat_mean": r["lat_mean"],
            "peak_req_m": n.get("peak_req"), "mean_req_m": n.get("mean_req"),
            "floor_m": n.get("floor"), "mean_attr_m": n.get("mean_attr"),
            "cpu_seconds": n.get("cpu_sec"), "cpu_seconds_attr": n.get("cpu_sec_attr"),
            "mean_used_m": n.get("mean_used"), "utilization": n.get("util"),
            "duration_s": n.get("duration_s"),
        })

    # ---- summary CSV -----------------------------------------------------
    scols = list(summary_rows[0].keys()) if summary_rows else []
    with open(f"{args.prefix}_summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=scols)
        w.writeheader()
        w.writerows(summary_rows)

    # ---- timeseries CSV (for external plotting) --------------------------
    with open(f"{args.prefix}_timeseries.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["config", "t_rel_s", "requested_m", "used_m"])
        for label in sorted(configs):
            for t, req, used in configs[label]["series"]:
                w.writerow([label, f"{t:.1f}", req, "" if used is None else used])
    print(f"\nwrote {args.prefix}_summary.csv + {args.prefix}_timeseries.csv")

    if args.plot:
        make_plots(configs, args.prefix)


def make_plots(configs, prefix):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed — skipping PNGs (timeseries CSV still written)")
        return

    # Fig 1 — reserved CPU over time.
    plt.figure()
    for label in sorted(configs):
        s = configs[label]["series"]
        if s:
            plt.plot([t for t, _, _ in s], [r for _, r, _ in s], label=label)
    plt.xlabel("time (s)"); plt.ylabel("pool requested CPU (m)")
    plt.title("Reserved CPU over time"); plt.legend()
    plt.savefig(f"{prefix}_reserved.png", dpi=120, bbox_inches="tight")

    # Fig 2 — efficiency frontier: p95 latency vs CPU-seconds.
    plt.figure()
    for label in sorted(configs):
        n, r = configs[label]["nodes"], configs[label]["replay"]
        if n and r["lat_p95"] is not None and n.get("cpu_sec_attr") is not None:
            plt.scatter(n["cpu_sec_attr"], r["lat_p95"])
            plt.annotate(label, (n["cpu_sec_attr"], r["lat_p95"]))
    plt.xlabel("CPU-seconds (above baseline)"); plt.ylabel("cold-start p95 (ms)")
    plt.title("Efficiency frontier — lower-left is better")
    plt.savefig(f"{prefix}_frontier.png", dpi=120, bbox_inches="tight")

    # Fig 3 — CPU-seconds bar.
    labels = [l for l in sorted(configs) if configs[l]["nodes"]]
    vals = [configs[l]["nodes"].get("cpu_sec_attr") or 0 for l in labels]
    if labels:
        plt.figure()
        plt.bar(labels, vals)
        plt.ylabel("CPU-seconds (above baseline)"); plt.title("Total reserved CPU-seconds")
        plt.xticks(rotation=30, ha="right")
        plt.savefig(f"{prefix}_cpusec.png", dpi=120, bbox_inches="tight")
    print(f"wrote {prefix}_reserved.png, {prefix}_frontier.png, {prefix}_cpusec.png")


if __name__ == "__main__":
    main()
