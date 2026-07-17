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

## Real-app mix (recommended for the main experiments)

The `loadtest-yolo` fleet (20 clones of one image) is useful when you need VOLUME
of identical ksvcs (E3 burst on a single workload). For the **main resource story,
use the real, already-profiled apps** — their per-app profiles differ wildly, so a
one-size-fits-all static baseline can't win on all of them, which is exactly the
point NIMBUS makes.

Per-app profile (cold/warm knee, from `results/<app>/`):

| App | Nimbus | ksvcs | endpoint | cold c_opt | warm c_opt | role |
|-----|--------|-------|----------|-----------|-----------|------|
| yolo | boost-001 | measure-yolo-001..003 | /status | 975m | 1125m | demonstrator (cold CPU-bound) |
| face | boost-002 | insignface-001..003 | /status | 937m | 1875m | demonstrator |
| jvm | boost-006 | jvm-probe-001..003 | /status | 437m | 812m | demonstrator |
| llm | boost-005 | measure-llm-001..003 | /status¹ | 1062m | 1812m | demonstrator (minute-scale) |
| io | boost-004 | io-probe-001..005 | /status | 100m | 100m | **control** (CPU-insensitive) |
| sha256² | boost-003 | sha256-001..003 | /status | 75m | 825m | warm demonstrator |

¹ Verify the LLM readiness path (`curl .../status` vs `.../loading-stats`) and use
a long `--req-timeout` (cold-start is minute-scale). ² sha256 ksvcs may not be
deployed — apply `config/go-sha256/*` first if you want it.

**Why this mix proves resource efficiency:** cold c_opt spans 75m→1062m and warm
100m→1875m. A static baseline must pick ONE number: `static-low 100m` times out
yolo/face/llm/jvm; `static-high 2000m` runs everything but wastes ~20× on io/sha256.
NIMBUS sizes each app individually → Pareto-wins on the whole mix. `io` is the
control: NIMBUS correctly gives it the floor (more CPU wouldn't help), so it should
NOT show a NIMBUS-vs-baseline latency gap — that absence is a feature.

### Step 1 — enable online for the demonstrator Nimbuses (they ship `online:false`)

`/decide` returns `passthrough` for a Nimbus with `online.enabled=false`, so the
offline-measured apps must be flipped ON for the online experiment:

```bash
for nb in boost-001 boost-002 boost-005 boost-006 boost-004; do   # yolo face llm jvm io
  kubectl patch nimbus "$nb" -n serverless --type=merge \
    -p '{"spec":{"online":{"enabled":true}}}'
done
# verify one ksvc admits (not passthrough):
curl -s -X POST http://localhost:8080/decide -H 'Content-Type: application/json' \
  -d '{"namespace":"serverless","ksvc":"insignface-001"}'; echo
```

### Step 2 — build the mix ksvcs-file (the /status family: yolo+face+jvm+io)

```bash
printf '%s\n' \
  measure-yolo-001 measure-yolo-002 measure-yolo-003 \
  insignface-001 insignface-002 insignface-003 \
  jvm-probe-001 jvm-probe-002 jvm-probe-003 \
  io-probe-001 io-probe-002 io-probe-003 io-probe-004 io-probe-005 \
  > mix.ksvcs
```

### Step 3 — run the mix (schedule + replay + sampler)

```bash
python3 gen_schedule.py --mode poisson --ksvcs-file mix.ksvcs \
    --duration 600 --rate 0.2 --cooldown 120 --out sched_mix.csv    # cooldown ≥ slowest app

python3 sample_resources.py --label nimbus-mix --duration 620 --out res_mix.nodes.csv &
python3 replay.py --schedule sched_mix.csv --label nimbus-mix --out res_mix.csv
```

### Step 4 — LLM separately (long timeout)

```bash
printf '%s\n' measure-llm-001 measure-llm-002 measure-llm-003 > llm.ksvcs
python3 gen_schedule.py --mode poisson --ksvcs-file llm.ksvcs \
    --duration 600 --rate 0.05 --cooldown 240 --out sched_llm.csv
python3 replay.py --schedule sched_llm.csv --req-timeout 300 \
    --label nimbus-llm --out res_llm.csv   # add --path /loading-stats if /status doesn't READY
```

### Step 5 — baselines (same schedules, for comparison)

- **Offline-only ablation** (easy, reversible): each app stays at its measured
  c_opt, no online adaptation. Flip online OFF and replay with `--no-decide`:
  ```bash
  for nb in boost-001 boost-002 boost-005 boost-006 boost-004; do
    kubectl patch nimbus "$nb" -n serverless --type=merge -p '{"spec":{"online":{"enabled":false}}}'
  done
  python3 sample_resources.py --label offline-only --duration 620 --out res_off.nodes.csv &
  python3 replay.py --schedule sched_mix.csv --no-decide --label offline-only --out res_off.csv
  ```
- **Static-uniform** (the "no profiling" baseline): online OFF + patch every ksvc to
  one fixed CPU + drop its boost CR (scoped, NOT `--all`), replay `--no-decide`.
  Reversible by re-applying the app's `config/*/my-boost-preload-*.yaml`. This is
  the baseline that best exposes the one-size-fits-all waste — build it only when
  you're ready to restore the apps afterwards.

### Cleanup — restore the apps to offline-only

```bash
for nb in boost-001 boost-002 boost-005 boost-006 boost-004; do
  kubectl patch nimbus "$nb" -n serverless --type=merge -p '{"spec":{"online":{"enabled":false}}}'
done
```

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
