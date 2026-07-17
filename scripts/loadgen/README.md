# loadgen — cold-start load harness for NIMBUS online experiments

Simulate load, measure cold-start latency, and observe the waterfall + burst
detector. Uses **option (b)**: the replayer itself POSTs `/decide` (no forked KPA
needed). Run everything **on the control-plane node** (cluster DNS + pod network).

Three tools, decoupled so the arrival model is separate from replay:

| Tool | Job |
|------|-----|
| `gen_fleet.py`        | emit N identical ksvcs (+ a Nimbus CR that preloads a measured profile) |
| `gen_schedule.py`     | turn an arrival MODEL (periodic / poisson / burst) into a fixed `schedule.csv` |
| `replay.py`           | replay `schedule.csv`, POST `/decide`, fire the request, log cold-start latency |
| `sample_resources.py` | snapshot pool-node CPU (requested/used/free) while a run happens |
| `gen_filler.py`       | reserve CPU on pool nodes to create controlled contention |
| `set_baseline.py`     | pin apps to a static CPU policy (opt/min/uniform) for baselines |
| `analyze.py`          | CSVs → comparison table + figures (latency, CPU-seconds, goodput) |

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

### Step 2 — build ONE mix ksvcs-file (all apps, LLM included)

LLM has a different readiness endpoint (`/loading-stats`, minute-scale), but you no
longer need a separate run — `gen_schedule --endpoint` attaches a per-app path /
ready-body / timeout / cooldown, so ONE schedule drives the whole mix.

```bash
printf '%s\n' \
  measure-yolo-001 measure-yolo-002 measure-yolo-003 \
  insignface-001 insignface-002 insignface-003 \
  jvm-probe-001 jvm-probe-002 jvm-probe-003 \
  io-probe-001 io-probe-002 io-probe-003 io-probe-004 io-probe-005 \
  measure-llm-001 measure-llm-002 measure-llm-003 \
  > mix.ksvcs
```

### Step 3 — idle-cluster right-sizing (NIMBUS vs fix-low vs fix-high)

On an **idle** cluster (no filler), this proves NIMBUS meets the SLO at LESS CPU
than a generous fixed policy, while a cheap fixed policy misses the SLO:

- **fix-200m** (all apps 200m): cheap, but yolo/face/llm cold-start needs ~1000m →
  times out → **fails latency SLO**.
- **fix-2000m** (all apps 2000m): meets SLO, but reserves ~2–20× what each app needs
  → **wastes resource**.
- **NIMBUS**: gives each app its c_opt → **meets SLO AND spends far less than 2000m**.

Run all three under BOTH periodic and poisson, with LLM merged in via `--endpoint`:

```bash
LLM='--endpoint measure-llm=/loading-stats,ready,300,240'   # path,ready,timeout,cooldown

for MODE in periodic poisson; do
  python3 gen_schedule.py --mode $MODE --ksvcs-file mix.ksvcs \
      --duration 600 --rate 0.2 --cooldown 120 $LLM --seed 42 --out sched_$MODE.csv

  # NIMBUS (online already true on the mix Nimbuses)
  python3 sample_resources.py --label ${MODE}_nimbus --duration 620 --out res_${MODE}_nimbus.nodes.csv &
  python3 replay.py --schedule sched_$MODE.csv --label ${MODE}_nimbus --out res_${MODE}_nimbus.csv
done
```

### Step 4 — the two fixed baselines (same schedules)

**STOP NIMBUS first (Ctrl-C the `go run ./cmd`).** Even with `online.enabled=false`,
NIMBUS's reconciler re-asserts c_opt every 2 s (`enforceOfflineBootstrap`, drift
correction) — a running NIMBUS would overwrite the fixed CPU back to c_opt. Same
rule as verify-probe.sh / benchmark.sh.

`set_baseline.py --policy uniform` pins EVERY app to one CPU + turns online off; then
replay with `--no-decide`. `NBS` = the Nimbuses of the apps IN your mix (yolo/face/jvm
= boost-001/002/006; add boost-004/005 only if io/llm are in the mix).

```bash
# 1. Ctrl-C NIMBUS.  2. then:
NBS=boost-001,boost-002,boost-006
for CPU in 200m 2000m; do
  python3 set_baseline.py --policy uniform --cpu $CPU --nimbuses $NBS
  sleep 10   # let the new ksvc revision + boost CR roll out before replaying
  for MODE in periodic poisson; do
    python3 sample_resources.py --label ${MODE}_fix$CPU --duration 620 --out res_${MODE}_fix$CPU.nodes.csv &
    python3 replay.py --schedule sched_$MODE.csv --no-decide --label ${MODE}_fix$CPU --out res_${MODE}_fix$CPU.csv
  done
done
```

Restore afterwards (turn online back on, then restart NIMBUS so it re-applies
c_opt / warm=c_min):

```bash
python3 set_baseline.py --policy nimbus --nimbuses $NBS
cd /home/ubuntu/nimbus && go run ./cmd 2>&1 | tee nimbus.log
```

### Step 5 — analyse (per traffic mode)

```bash
SLO='--slo measure-yolo=16000 --slo insignface=16000 --slo jvm-probe=... --slo io-probe=... --slo measure-llm=...'
python3 analyze.py res_periodic_nimbus.csv res_periodic_fix200m.csv res_periodic_fix2000m.csv \
    --plot --prefix periodic $SLO
python3 analyze.py res_poisson_nimbus.csv res_poisson_fix200m.csv res_poisson_fix2000m.csv \
    --plot --prefix poisson $SLO
```

Expected: NIMBUS has **goodput% ≈ fix-2000m** (both meet SLO) but **CPU-seconds ≪
fix-2000m**; **fix-200m** has low goodput% (SLO misses/timeouts) despite low CPU. So
NIMBUS is the only config satisfying both resource AND latency-SLO — under periodic
AND poisson.

### Cleanup — restore the apps to offline-only

```bash
for nb in boost-001 boost-002 boost-005 boost-006 boost-004; do
  kubectl patch nimbus "$nb" -n serverless --type=merge -p '{"spec":{"online":{"enabled":false}}}'
done
```

## Contention experiments (the main resource-efficiency proof)

On an **idle** cluster NIMBUS gives every app Tier-1 c_opt — indistinguishable from
a static-opt policy. Its value only appears under **resource pressure**, where the
waterfall degrades (c_opt → c_min → best-fit) to fit more pods while still meeting
SLO. So the scientific test **must create contention** — that's what `gen_filler.py`
is for.

**Claim (what the figures prove):** under a fixed CPU budget with time-varying load,
NIMBUS serves the most cold-starts *within SLO* — it reserves only `c_min_warm`
steady-state (packs more than a c_opt-warm policy, still meets the warm SLO) and
picks the cold boost adaptively (c_opt when there's room, c_min/best-fit when tight).
No static policy matches its density AND latency across the load range.

**Baselines** (`set_baseline.py` sets the exact boost level, then `online=false`):

| Policy | cold | warm | meaning |
|--------|------|------|---------|
| `--policy opt` | c_opt_cold | c_opt_warm | profiled, generous → **fewer pods fit** |
| `--policy min` | c_min_cold | c_min_warm | profiled, tight → max density, but slow cold-start when idle |
| `--policy uniform --cpu 2000m` | fixed | fixed | no profiling, one-size-fits-all |
| (NIMBUS) `--policy nimbus` | adaptive | c_min_warm | restore adaptive (undo a baseline) |

**Budget for 3×16-core nodes** (~30 CPU usable serverless after 70% cap + system):
the mix needs ~6 CPU warm under NIMBUS vs ~12 under static-opt, so squeeze the
effective pool to **~9 CPU** (filler reserves **7000m/node**) — static-opt overflows
and rejects, NIMBUS fits. Sweep `{5000m, 7000m, 9000m}` per node for the frontier.

### Exp A — load/budget sweep (Pareto frontier)

Fix the budget (one filler level), sweep offered load (`--rate`), and per config
plot goodput / density / p95 vs load. Repeat at a few filler levels.

```bash
python3 gen_filler.py --cpu 7000m --out filler.yaml && kubectl apply -f filler.yaml
# NIMBUS run (online already true on the mix Nimbuses):
python3 sample_resources.py --label expA_nimbus --out res_expA_nimbus.nodes.csv &
python3 replay.py --schedule sched_mix.csv --label expA_nimbus --out res_expA_nimbus.csv
# static-opt baseline (same schedule):
python3 set_baseline.py --policy opt --nimbuses boost-001,boost-002,boost-006,boost-004
python3 sample_resources.py --label expA_opt --out res_expA_opt.nodes.csv &
python3 replay.py --schedule sched_mix.csv --no-decide --label expA_opt --out res_expA_opt.csv
# static-min baseline:
python3 set_baseline.py --policy min --nimbuses boost-001,boost-002,boost-006,boost-004
# ... replay --no-decide --label expA_min ...
python3 set_baseline.py --policy nimbus --nimbuses boost-001,boost-002,boost-006,boost-004  # restore
kubectl delete -f filler.yaml
```

### Exp B — time-varying load (the one-figure proof)

Same constrained budget, but a schedule with lulls + spikes (`--mode burst`). In one
run NIMBUS beats static-opt on goodput during spikes AND static-min on latency during
lulls — no static policy wins both.

### Analyse with goodput

```bash
python3 analyze.py res_expA_nimbus.csv res_expA_opt.csv res_expA_min.csv --plot \
  --prefix expA \
  --slo measure-yolo=16000 --slo insignface=16000 --slo jvm-probe=... --slo io-probe=...
```
`goodput%` = served AND time-to-READY ≤ that app's cold SLO. Read each app's SLO
from its Nimbus CR (`spec.acceptableResponseTime.cold`).

## Schedule modes & naming runs (so analyze picks the right files)

### The three arrival modes

| Mode | Command (mix example) | Use for |
|------|-----------------------|---------|
| **periodic** | `gen_schedule.py --mode periodic --ksvcs-file mix.ksvcs --duration 600 --rate 0.1 --cooldown 120 --seed 42 --out sched.csv` | evenly spaced, repeatable — cleanest A/B (E1) |
| **poisson** | `--mode poisson --rate 0.2 --duration 600 --cooldown 120 ...` | random arrivals (E2) |
| **burst** | `--mode burst --baseline-rate 0.1 --wave-size 12 --wave-at 60,180 --duration 240 ...` | turn the burst detector on/off (E3) |

`--rate` is events/sec: **periodic** spaces them exactly `1/rate` s apart (rate 0.1 =
one every 10 s); **poisson** uses `rate` as the mean. Keep `--seed` fixed so the SAME
schedule is replayed across every config of one experiment.

### Naming so `analyze.py` pairs and separates runs correctly

`analyze.py` pairs `res_<name>.csv` (replay) with `res_<name>.nodes.csv` (sampler) —
the nodes file MUST be the results name **+ `.nodes.csv`** — and groups rows by the
`label` COLUMN inside the CSV. So for each run, keep `<name>` identical across the
three commands:

- **replay:** `--label <name> --out res_<name>.csv`
- **sampler:** `--label <name> --out res_<name>.nodes.csv`  ← same `<name>`, add `.nodes`

Pick `<name> = <experiment>_<config>`, e.g. `e1_nimbus`, `e1_high`, `mixP1_nimbus`
(P1 = periodic rate 0.1). Different experiments (different rate/pattern) → different
`<name>` → runs never clobber each other on disk.

**Draw one figure per experiment by passing EXPLICIT files** — don't rely on the
default `res_*.csv` glob, which would lump every run you've ever done into one plot:

```bash
# experiment E1 (periodic 0.1) — compare THIS experiment's 3 configs only
python3 analyze.py res_e1_nimbus.csv res_e1_low.csv res_e1_high.csv --plot --prefix e1

# a different experiment (periodic 0.2) — its own files + its own prefix
python3 analyze.py res_e2_nimbus.csv res_e2_high.csv --plot --prefix e2
```

Each config = one table row / one frontier point. Two files sharing the SAME `label`
are MERGED into one config — use that on purpose to combine the /status-mix run and
the LLM run of the same config (give both `--label mix_nimbus`).

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

### Turn the CSVs into a table + figures — `analyze.py`

```bash
# after running >= 1 config (comparison figures need >= 2)
python3 analyze.py res_mix.csv res_off.csv res_high.csv --plot --prefix analysis
```
For each config (grouped by the `label` column) it prints and writes
`analysis_summary.csv`: cold-start p50/p95/p99, served %, peak/mean reserved CPU,
**CPU-seconds** (total AND above the system baseline), and utilization
(`used/requested`). `analysis_timeseries.csv` has per-config reserved-CPU-over-time
for external plotting. With `--plot` (matplotlib) it also writes:
`analysis_reserved.png` (reserved CPU over time), `analysis_frontier.png`
(p95 latency vs CPU-seconds — the money figure), `analysis_cpusec.png` (bar).

It analyses whatever configs are present — run it after each config to accumulate.
`CPU-seconds (attr)` subtracts the system floor (10th-percentile pool requested),
so it reflects only the experiment's reserved CPU.

> **NIMBUS's own view** — set `NIMBUS_LOG_SNAPSHOT=1` when starting NIMBUS to log
> per-node free (`event=pool_headroom`) at every `/decide`/tick, to cross-check
> NIMBUS's math against `resources.csv`. The external sampler is the FAIR
> cross-config comparison (baselines don't run NIMBUS); the internal log is only a
> sanity check.

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
| `--endpoint` | — | per-app `prefix=path[,ready_body[,timeout[,cooldown]]]`, repeatable — mix apps with different endpoints (e.g. LLM) in one schedule |
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

**gen_filler.py** — reserve CPU on pool nodes (create contention):

| Flag | Default | Purpose |
|------|---------|---------|
| `--cpu` | (required) | CPU reserved per pool node, e.g. 7000m |
| `--replicas` | 3 | = number of pool nodes (one filler pod each) |
| `--pool-selector` | nimbus.io/pool=serverless | which nodes |
| `--image` | busybox:1.36 | sleep container |
| `--out` | filler.yaml | output |

**set_baseline.py** — pin apps to a static CPU policy:

| Flag | Default | Purpose |
|------|---------|---------|
| `--policy` | (required) | `opt` / `min` / `uniform` / `nimbus` (restore) |
| `--nimbuses` | (required) | comma list, e.g. boost-001,boost-002 |
| `--cpu` | — | fixed CPU for `--policy uniform` |
| `--namespace` | serverless | namespace |

**analyze.py** — CSVs → comparison table + figures:

| Flag | Default | Purpose |
|------|---------|---------|
| `results` (positional) | glob `res_*.csv` | replay output CSVs (nodes CSV auto-found) |
| `--prefix` | analysis | output file prefix |
| `--plot` | off | also write PNGs (needs matplotlib) |
| `--cold-slo` | — | default cold SLO ms → enables `goodput%` |
| `--slo` | — | per-app SLO, repeatable: `--slo measure-yolo=16000` |

## Next (not yet built)

- **nimbus.log join**: `analyze.py` joins `results.csv` + `resources.csv`; adding
  `nimbus.log` (burst mode transitions, tier decisions) on the same time axis is
  still manual.
