# loadgen — cold-start load harness for NIMBUS online experiments

Simulate load, measure cold-start latency, and observe the waterfall + burst
detector. Uses **option (b)**: the replayer itself POSTs `/decide` (no forked KPA
needed). Run everything **on the control-plane node** (cluster DNS + pod network).

Three tools, decoupled so the arrival model is separate from replay:

| Tool | Job |
|------|-----|
| `gen_fleet.py`     | emit N identical ksvcs (+ a Nimbus CR that preloads a measured profile) |
| `gen_schedule.py`  | turn an arrival MODEL (periodic / poisson / burst) into a fixed `schedule.csv` |
| `replay.py`        | replay `schedule.csv`, POST `/decide`, fire the request, log cold-start latency |

## Prerequisites (read this or every `/decide` returns `passthrough`)

1. **NIMBUS is running** (`go run ./cmd`) so `/decide` is up on `:8080`. The three
   online goroutines start automatically.
2. **The fleet is under a Nimbus CR with a COMPLETED profile.** `/decide` only
   decides for a ksvc listed in a Nimbus whose profile is loaded. `gen_fleet.py`
   emits that Nimbus with `preMeasured.loadFromDir` — point `--load-dir` at your
   existing measured yolo run (e.g. `./results/yolo`). On apply you should see:
   `Skipping binary search — all N candidate node(s) saturated`.
3. **Pool nodes are labelled** to match `--pool-selector` (default
   `nimbus.io/pool=serverless`).
4. **Sanity check**: after applying the fleet, tail `nimbus.log` during one replay
   event — you want `event=admit tier=...`. If you see only `passthrough`, the
   profile did not load (prereq 2).

## Quick start (E1 — right-sizing, idle cluster)

```bash
cd scripts/loadgen

# 1. Fleet + Nimbus (adaptive). --load-dir must be a real measured run.
python3 gen_fleet.py --count 20 --load-dir ./results/yolo --out fleet.yaml
kubectl apply -f fleet.yaml          # from repo root; wait for "saturated" log

# 2. Fixed, repeatable schedule (same file replayed for every config).
python3 gen_schedule.py --mode periodic --ksvcs-file fleet.yaml.ksvcs \
    --duration 300 --rate 0.2 --cooldown 60 --out sched_e1.csv

# 3a. NIMBUS run (decide ON)
python3 replay.py --schedule sched_e1.csv --label nimbus --out res_nimbus.csv
```

For the **baselines**, replay the SAME `sched_e1.csv` (only the CPU policy changes):

```bash
# static-low: fixed 100m, NIMBUS not managing → --no-decide
python3 gen_fleet.py --count 20 --cpu 100m --no-nimbus --out static_low.yaml
kubectl apply -f static_low.yaml
python3 replay.py --schedule sched_e1.csv --no-decide --label static-low --out res_low.csv

# static-high: fixed CPU = measured c_opt (e.g. 900m), no NIMBUS
python3 gen_fleet.py --count 20 --cpu 900m --no-nimbus --out static_high.yaml
kubectl apply -f static_high.yaml
python3 replay.py --schedule sched_e1.csv --no-decide --label static-high --out res_high.csv
```

Compare `latency_ms` (p50/p95) and `boost_cpu` across `res_*.csv`. Expected:
NIMBUS ≈ static-high latency at LOWER CPU (it stops at the knee); both ≫ better
than static-low.

## E2 — waterfall under contention

Fill the pool first (a "filler" that reserves CPU on the pool nodes — **not yet in
this harness**, see "Next"), then replay a poisson schedule. Compare full NIMBUS
vs the **offline-only ablation** (`online.enabled: false` in the Nimbus CR, replay
`--no-decide`) and vs static-high. Metric: fraction of events with a non-empty
`latency_ms` / non-`pending` `decision` (admission rate), plus latency.

```bash
python3 gen_schedule.py --mode poisson --ksvcs-file fleet.yaml.ksvcs \
    --duration 300 --rate 0.4 --cooldown 60 --out sched_e2.csv
python3 replay.py --schedule sched_e2.csv --label nimbus --out res_e2_nimbus.csv
```

## E3 — burst detector

```bash
# quiet baseline + a wave of 12 cold-starts at t=60 and t=180
python3 gen_schedule.py --mode burst --ksvcs-file fleet.yaml.ksvcs \
    --baseline-rate 0.1 --wave-size 12 --wave-window 6 --wave-at 60,180 \
    --duration 240 --cooldown 60 --out sched_e3.csv

python3 replay.py --schedule sched_e3.csv --label nimbus --out res_e3_nimbus.csv
# ablation: burst OFF — restart NIMBUS with NIMBUS_BURST_THRESHOLD_RATE=99999
python3 replay.py --schedule sched_e3.csv --label burst-off --out res_e3_off.csv
```

While it runs, watch `nimbus.log` for `event=mode_change mode=BURST` (should fire
just after each wave) and `mode=NORMAL` (~30s after the wave, the quiet window).
Correlate wave time → BURST on → reserve applied → NORMAL off.

**Fleet sizing for bursts:** a wave needs `--wave-size` DISTINCT cold ksvcs, and a
ksvc can't be reused within `--cooldown`. If `gen_schedule.py` warns "events
dropped", raise `--count` (fleet size ≥ wave-size + baseline churn) or lower
`--cooldown`.

## How cold-start latency is measured

This workload's `/status` returns **503 while the YOLO model loads**, then
`200 + READY` once served (~11s at c_opt) — the same readiness signal NIMBUS's
offline cold probe polls. So `replay.py` **polls until READY** and reports the
time-to-ready = the real cold-start latency (NOT a single 503 shot). Tune with
`--ready-body` (default `READY`; empty = any HTTP 200) and `--poll-interval`.
This is exactly the quantity NIMBUS optimises: more cold CPU → model loads faster
→ shorter time-to-READY.

## Output columns (`results.csv`)

`label, event_offset, ksvc, decided, decision, tier, boost_cpu, mode,
send_wallclock, latency_ms, http_code, attempts`

`latency_ms` = time-to-READY (cold-start). `attempts` = readiness polls until
ready. `send_wallclock` (epoch) joins rows against `nimbus.log` + a resource
sampler on a common timeline.

## Knobs worth knowing

- `--settle` (replay): seconds after `/decide` before the request, so the boost CR
  reaches the webhook before the pod is created. Raise it if cold pods come up at
  the wrong CPU (the boost-controller poll race in CLAUDE.md).
- `--path` (replay): request endpoint timed as the cold-start. `/status` is light
  (isolates spin-up time); `/detect/local` mixes in YOLO compute.
- `--seed` (gen_schedule): fixed → identical schedule across configs.

## Next (not yet built)

- **Resource sampler**: `kubectl top nodes/pods` → CSV every 1–2s (needs
  metrics-server) for the "system resources" view.
- **Filler**: a Deployment that reserves a tunable CPU on the pool nodes, to create
  the constrained-cluster state E2/E3 need.
- **Timeline merger**: join `results.csv` + `nimbus.log` + resource CSV by time.
