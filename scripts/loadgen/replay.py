#!/usr/bin/env python3
"""replay.py — replay a schedule.csv and measure cold-start latency.

Runs the OPTION-B sequence for every scheduled cold-start (no forked KPA needed):

  1. POST /decide {namespace, ksvc}  -> NIMBUS runs the waterfall, feeds the burst
     detector, and patches the StartupCPUBoost CR to the decided cold CPU.
  2. Wait --settle seconds so the boost webhook picks up the new CR value before
     the pod is created.
  3. GET http://<ksvc>.<ns>.<dns-suffix><path> — this hits the scaled-to-zero
     ksvc, so Knative's own KPA scales 0->1 using the just-patched spec. We time
     the response = cold-start latency.

Run this ON THE CONTROL-PLANE NODE (it has cluster DNS + pod-network access, the
same path verify-probe.sh uses). NIMBUS must be running (its /decide server is up).

Events are dispatched CONCURRENTLY (one worker thread each, fired at its offset)
so a burst of near-simultaneous cold-starts is reproduced faithfully — a cold
start takes seconds and the next event may fire before it finishes.

For BASELINES / ablations that must NOT consult NIMBUS per request, pass
--no-decide: the /decide POST is skipped (the ksvc keeps whatever CPU offline/
preload/reconciler left on it), while request timing stays identical (--settle is
still applied) so latency is comparable across configs.
"""
import argparse
import csv
import json
import threading
import time
import urllib.error
import urllib.request


def post_decide(url, namespace, ksvc, timeout):
    body = json.dumps({"namespace": namespace, "ksvc": ksvc}).encode()
    req = urllib.request.Request(url, data=body,
                                 headers={"Content-Type": "application/json"},
                                 method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode()), None
    except urllib.error.HTTPError as e:
        return None, f"http {e.code}"
    except Exception as e:  # noqa: BLE001 — record any failure, keep going
        return None, str(e)


def poll_until_ready(url, ready_body, poll_interval, timeout):
    """Retry the request until the service is READY, timing the whole wait.

    Cold-start latency = time from first traffic (which triggers scale 0->1) to a
    ready response. This workload's /status returns 503 while the YOLO model loads,
    then 200/READY once served, so a SINGLE shot can't measure cold-start — it must
    poll (same as NIMBUS's offline cold probe). ready_body: require the body to
    contain this token; empty means any HTTP 200 counts as ready.

    Returns (elapsed_ms, final_code, attempts); elapsed_ms is None on timeout.
    """
    t0 = time.monotonic()
    deadline = t0 + timeout
    attempts, last_code = 0, None
    while time.monotonic() < deadline:
        attempts += 1
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                body = r.read().decode(errors="replace")
                last_code = r.status
                if r.status == 200 and (not ready_body or ready_body in body):
                    return (time.monotonic() - t0) * 1000.0, last_code, attempts
        except urllib.error.HTTPError as e:
            last_code = e.code
        except Exception as e:  # noqa: BLE001
            last_code = f"ERR:{e}"
        time.sleep(poll_interval)
    return None, last_code, attempts


def worker(ev_offset, ksvc, args, rows, lock):
    row = {"label": args.label, "event_offset": f"{ev_offset:.3f}", "ksvc": ksvc,
           "decided": 0, "decision": "", "tier": "", "boost_cpu": "", "mode": "",
           "send_wallclock": "", "latency_ms": "", "http_code": "", "attempts": ""}

    if not args.no_decide:
        resp, err = post_decide(args.decide_url, args.namespace, ksvc, args.req_timeout)
        row["decided"] = 1
        if resp:
            row["decision"] = resp.get("decision", "")
            row["tier"] = resp.get("tier", "")
            row["boost_cpu"] = resp.get("boostCpu", "")
            row["mode"] = resp.get("mode", "")
        else:
            row["decision"] = f"decide_err:{err}"

    time.sleep(args.settle)

    url = f"http://{ksvc}.{args.namespace}.{args.dns_suffix}{args.path}"
    row["send_wallclock"] = f"{time.time():.3f}"
    lat, code, attempts = poll_until_ready(url, args.ready_body, args.poll_interval, args.req_timeout)
    row["latency_ms"] = "" if lat is None else f"{lat:.1f}"
    row["http_code"] = str(code)
    row["attempts"] = str(attempts)

    with lock:
        rows.append(row)
    print(f"[t+{ev_offset:6.1f}] {ksvc:20s} tier={row['tier'] or '-':9s} "
          f"mode={row['mode'] or '-':6s} cold={row['boost_cpu'] or '-':6s} "
          f"ready={row['latency_ms'] or 'TIMEOUT':>8s}ms attempts={attempts} code={row['http_code']}")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--schedule", required=True)
    p.add_argument("--namespace", default="serverless")
    p.add_argument("--decide-url", default="http://localhost:8080/decide")
    p.add_argument("--path", default="/status", help="request path polled to time the cold-start")
    p.add_argument("--ready-body", default="READY",
                   help="poll until the response body contains this (empty = any HTTP 200 is ready)")
    p.add_argument("--poll-interval", type=float, default=0.1,
                   help="seconds between readiness polls")
    p.add_argument("--dns-suffix", default="svc.cluster.local")
    p.add_argument("--settle", type=float, default=1.5,
                   help="seconds to wait after /decide so the boost CR propagates")
    p.add_argument("--req-timeout", type=float, default=120.0,
                   help="max seconds to wait for READY (cold-start at low CPU can be slow)")
    p.add_argument("--no-decide", action="store_true",
                   help="skip /decide (baseline/ablation) — request timing unchanged")
    p.add_argument("--label", default="nimbus", help="config label written to each output row")
    p.add_argument("--out", default="results.csv")
    args = p.parse_args()

    with open(args.schedule) as f:
        events = [(float(r["offset_sec"]), r["ksvc"]) for r in csv.DictReader(f)]
    events.sort(key=lambda e: e[0])
    print(f"[replay] label={args.label} events={len(events)} decide={'OFF' if args.no_decide else 'ON'} "
          f"settle={args.settle}s path={args.path}")

    rows, lock, threads = [], threading.Lock(), []
    start = time.monotonic()
    for offset, ksvc in events:
        due = start + offset
        now = time.monotonic()
        if due > now:
            time.sleep(due - now)
        t = threading.Thread(target=worker, args=(offset, ksvc, args, rows, lock))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    rows.sort(key=lambda r: float(r["event_offset"]))
    cols = ["label", "event_offset", "ksvc", "decided", "decision", "tier",
            "boost_cpu", "mode", "send_wallclock", "latency_ms", "http_code", "attempts"]
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

    ok = [float(r["latency_ms"]) for r in rows if r["latency_ms"]]
    if ok:
        ok.sort()
        p95 = ok[min(len(ok) - 1, int(0.95 * len(ok)))]
        print(f"[replay] done: {len(ok)}/{len(rows)} ok  "
              f"p50={ok[len(ok)//2]:.0f}ms  p95={p95:.0f}ms  -> {args.out}")
    else:
        print(f"[replay] done: 0 successful responses -> {args.out}")


if __name__ == "__main__":
    main()
