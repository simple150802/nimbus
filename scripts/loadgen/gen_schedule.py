#!/usr/bin/env python3
"""gen_schedule.py — produce a FIXED, replayable cold-start schedule.

A "schedule" is a list of (offset_seconds, ksvc) events: at offset_seconds into
the run, fire one cold-start against `ksvc`. replay.py plays it back identically
for every config (NIMBUS / static-low / static-high / ablations), so runs are
comparable — the ONLY thing that changes between runs is the CPU-assignment
policy, never the arrival pattern.

Why a fleet + round-robin: with max-scale=1 + containerConcurrency=1, one ksvc
produces at most ONE cold-start at a time. So the arrival process is spread over
a POOL of ksvcs, and a ksvc is not reused until it has had time to scale back to
zero (--cooldown). The arrival MODEL only decides the timestamps; ksvc pick is a
least-recently-used cold ksvc.

Models (one per experiment — see README):
  periodic : one cold-start every 1/rate s      (E1 right-sizing — repeatable)
  poisson  : inter-arrival ~ Exp(rate)          (E2 waterfall — random arrivals)
  burst    : quiet baseline + injected wave(s)   (E3 burst — turn the detector
             on/off on command: quiet -> N in --wave-window s -> quiet)

All models take --seed so the schedule is reproducible.
"""
import argparse
import csv
import random
import sys


def make_ksvc_names(base, count):
    return [f"{base}-{i:03d}" for i in range(1, count + 1)]


class Picker:
    """Least-recently-used cold-ksvc picker honouring a reuse cooldown."""

    def __init__(self, ksvcs, cooldown):
        self.ksvcs = list(ksvcs)
        self.cooldown = cooldown
        self.last_used = {k: -1e9 for k in ksvcs}
        self.skipped = 0

    def pick(self, t):
        cands = [k for k in self.ksvcs if self.last_used[k] <= t - self.cooldown]
        if not cands:
            self.skipped += 1
            return None
        k = min(cands, key=lambda k: self.last_used[k])
        self.last_used[k] = t
        return k


def gen_periodic(picker, duration, rate):
    events, t, period = [], 0.0, 1.0 / rate
    while t < duration:
        k = picker.pick(t)
        if k:
            events.append((t, k))
        t += period
    return events


def gen_poisson(picker, duration, rate, rng):
    events, t = [], 0.0
    while True:
        t += rng.expovariate(rate)
        if t >= duration:
            break
        k = picker.pick(t)
        if k:
            events.append((t, k))
    return events


def gen_burst(picker, total, baseline_rate, wave_size, wave_window, wave_at, rng):
    # Quiet baseline over the whole run...
    events = gen_poisson(picker, total, baseline_rate, rng)
    # ...plus a dense wave of distinct cold-starts at each --wave-at time.
    for w in wave_at:
        for _ in range(wave_size):
            t = w + rng.uniform(0.0, wave_window)
            k = picker.pick(t)
            if k:
                events.append((t, k))
    events.sort(key=lambda e: e[0])
    return events


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["periodic", "poisson", "burst"], required=True)
    p.add_argument("--out", default="schedule.csv")
    p.add_argument("--seed", type=int, default=42)
    # ksvc fleet
    p.add_argument("--base-name", default="loadtest-yolo")
    p.add_argument("--count", type=int, default=20, help="fleet size")
    p.add_argument("--ksvcs-file", help="one ksvc name per line (overrides base/count)")
    p.add_argument("--cooldown", type=float, default=60.0,
                   help="seconds before a ksvc may be reused (scale-to-zero + margin)")
    # periodic / poisson
    p.add_argument("--duration", type=float, default=300.0)
    p.add_argument("--rate", type=float, default=0.2, help="events/sec (period=1/rate for periodic; lambda for poisson)")
    # burst
    p.add_argument("--baseline-rate", type=float, default=0.15)
    p.add_argument("--wave-size", type=int, default=12)
    p.add_argument("--wave-window", type=float, default=6.0)
    p.add_argument("--wave-at", default="60", help="comma list of wave start times, e.g. 60,180")
    args = p.parse_args()

    rng = random.Random(args.seed)
    if args.ksvcs_file:
        with open(args.ksvcs_file) as f:
            ksvcs = [ln.strip() for ln in f if ln.strip()]
    else:
        ksvcs = make_ksvc_names(args.base_name, args.count)
    picker = Picker(ksvcs, args.cooldown)

    if args.mode == "periodic":
        events = gen_periodic(picker, args.duration, args.rate)
    elif args.mode == "poisson":
        events = gen_poisson(picker, args.duration, args.rate, rng)
    else:
        wave_at = [float(x) for x in args.wave_at.split(",") if x.strip()]
        total = max([args.duration] + [w + 60 for w in wave_at])
        events = gen_burst(picker, total, args.baseline_rate,
                           args.wave_size, args.wave_window, wave_at, rng)

    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["offset_sec", "ksvc"])
        for t, k in events:
            w.writerow([f"{t:.3f}", k])

    span = events[-1][0] if events else 0.0
    print(f"[gen_schedule] mode={args.mode} events={len(events)} span={span:.1f}s "
          f"fleet={len(ksvcs)} cooldown={args.cooldown}s -> {args.out}")
    if picker.skipped:
        print(f"[gen_schedule] WARNING: {picker.skipped} events dropped — fleet too "
              f"small for this rate/cooldown. Increase --count or --cooldown, or lower --rate.",
              file=sys.stderr)


if __name__ == "__main__":
    main()
