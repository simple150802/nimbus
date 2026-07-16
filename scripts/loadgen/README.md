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

`label, event_offset, ksvc, decided, decision, tier, boost_cpu, warm_cpu, mode,
send_wallclock, latency_ms, http_code, attempts`

`latency_ms` = time-to-READY (cold-start). `boost_cpu` = cold CPU NIMBUS set on
the StartupCPUBoost CR; `warm_cpu` = the CPU the pod reverts to (`/decide`'s `cpu`
field — INTENDED value, not the live pod's actual reverted CPU). `attempts` =
readiness polls until ready. `send_wallclock` (epoch) joins rows against
`nimbus.log` + `resources.csv` (sample_resources.py) on a common timeline.

## Knobs worth knowing

- `--settle` (replay): seconds after `/decide` before the request, so the boost CR
  reaches the webhook before the pod is created. Raise it if cold pods come up at
  the wrong CPU (the boost-controller poll race in CLAUDE.md).
- `--path` (replay): request endpoint timed as the cold-start. `/status` is light
  (isolates spin-up time); `/detect/local` mixes in YOLO compute.
- `--seed` (gen_schedule): fixed → identical schedule across configs.

## Node resource monitoring (compare resource efficiency vs baselines)

`sample_resources.py` snapshots the pool nodes every `--interval` seconds while a
schedule runs — the ground-truth, config-agnostic view (it does NOT use NIMBUS's
own snapshot, so NIMBUS and the baselines are measured identically). Run it in
parallel with `replay.py`, once per config:

```bash
# terminal A — sampler (start just before replay, same window)
python3 sample_resources.py --label nimbus --interval 2 --duration 320 --out res_nimbus.nodes.csv
# terminal B — the run
python3 replay.py --schedule sched_e1.csv --label nimbus --out res_nimbus.csv
```
Repeat with `--label static-low` / `static-high` (and `--no-decide` on replay) to
get `res_low.nodes.csv` / `res_high.nodes.csv`.

Columns: `label, epoch, node, alloc_m, requested_m, used_m, free_m, pods`.
Add `--no-top` if there is no metrics-server (drops `used_m`, keeps requested).

**What proves NIMBUS is more efficient** — from `requested_m` (Σ pod CPU requests
NIMBUS/baseline reserved on the pool) over time:

| Metric | How | NIMBUS should be |
|--------|-----|------------------|
| Peak reserved CPU | max of pool `requested_m` | ≤ static-high, at similar latency |
| CPU-seconds | Σ(`requested_m` × interval) | lower than static-high |
| Utilization | `used_m / requested_m` | closer to 1 (requests match need) |
| Admission density | cold-starts served ÷ CPU-seconds | higher |

The story: NIMBUS (right-sized c_opt) reserves **less CPU than static-high for the
same cold-start latency**, and far better latency than static-low — so it sits on
the efficiency frontier. Join `res_*.nodes.csv` (epoch) with `res_*.csv`
(send_wallclock) to line up "resource reserved" against "cold-start served".

> **Also** — NIMBUS already computes per-node free in `buildPoolSnapshot`; if you
> want NIMBUS's own view logged per `/decide`, that's a one-line add in
> `internal/online/budget.go`. The external sampler above is the fair cross-config
> comparison; the internal log is only for cross-checking NIMBUS's math.

## Options reference

**gen_fleet.py** — emit N ksvcs (+ Nimbus CR):

| Flag | Default | Purpose |
|------|---------|---------|
| `--count` | 20 | fleet size |
| `--base-name` | loadtest-yolo | ksvc name prefix |
| `--namespace` | serverless | namespace |
| `--image` | measure-yolo:v1 | container image |
| `--rtmp` | 192.168.17.129:2000 | RTMP_STREAM_URL env |
| `--cpu` | 100m | fixed ksvc CPU (static baselines) |
| `--no-nimbus` | off | emit only Services (true static baseline) |
| `--nimbus-name` | loadtest-boost | Nimbus CR name |
| `--pool-selector` | nimbus.io/pool=serverless | node pool label |
| `--load-dir` | ./results/yolo | preloaded profile dir |
| `--metric` / `--cold-slo` / `--warm-slo` / `--cpu-budget` | p95 / 16000 / 5000 / 1200m | must match the preloaded run |
| `--out` | fleet.yaml | output (+ `.ksvcs` name list) |

**gen_schedule.py** — arrival model → `schedule.csv`:

| Flag | Default | Purpose |
|------|---------|---------|
| `--mode` | (required) | `periodic` / `poisson` / `burst` |
| `--ksvcs-file` | — | ksvc list (from `fleet.yaml.ksvcs`) |
| `--base-name` / `--count` | loadtest-yolo / 20 | fleet if no `--ksvcs-file` |
| `--cooldown` | 60 | s before a ksvc is reused (≥ cold-start + scale-to-zero) |
| `--duration` / `--rate` | 300 / 0.2 | periodic/poisson: length, events/sec |
| `--baseline-rate` | 0.15 | burst: quiet-period rate |
| `--wave-size` / `--wave-window` / `--wave-at` | 12 / 6 / 60 | burst: wave shape + start times |
| `--seed` | 42 | fixed → identical schedule across configs |
| `--out` | schedule.csv | output |

**replay.py** — replay + measure:

| Flag | Default | Purpose |
|------|---------|---------|
| `--schedule` | (required) | schedule.csv to replay |
| `--namespace` | serverless | namespace |
| `--decide-url` | http://localhost:8080/decide | NIMBUS /decide endpoint |
| `--no-decide` | off | skip /decide (baselines/ablations) |
| `--path` | /status | endpoint polled for cold-start |
| `--ready-body` | READY | body token = ready (empty = any HTTP 200) |
| `--poll-interval` | 0.1 | s between readiness polls |
| `--settle` | 1.5 | s after /decide before request (boost-CR propagation) |
| `--req-timeout` | 120 | max s to wait for READY |
| `--dns-suffix` | svc.cluster.local | cluster DNS suffix |
| `--label` | nimbus | config label in output |
| `--out` | results.csv | output |

**sample_resources.py** — node CPU sampler:

| Flag | Default | Purpose |
|------|---------|---------|
| `--pool-selector` | nimbus.io/pool=serverless | nodes to sample |
| `--interval` | 2 | s between samples |
| `--duration` | 320 | total sampling window (s) |
| `--no-top` | off | skip `kubectl top` (no metrics-server) |
| `--label` | nimbus | config label in output |
| `--out` | resources.csv | output |

## Next (not yet built)

- **Filler**: a Deployment that reserves a tunable CPU on the pool nodes, to create
  the constrained-cluster state E2/E3 need.
- **Timeline merger**: join `results.csv` + `nimbus.log` + `resources.csv` by time.
