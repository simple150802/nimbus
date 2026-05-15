# NIMBUS Algorithm — Offline-Stage Reference

> **Scope of this doc.** This is the offline-stage algorithm reference: CPU level model, binary-search profiling, sample persistence. The implementation plan for the **online stage** (CA-BFD, EWMA burst detection, degrade waterfall, upgrade controller) lives in [`online_plan.md`](online_plan.md). Both stages share the same CPU level model (§2 below).

NIMBUS profiles Knative services offline to derive a CPU level model `(c_floor, c_min, c_opt)` per node, persisted to `.status.perNode`. The online stage (separately documented in `online_plan.md`) consumes that profile at clone-creation / pod-admission time to decide per-pod CPU.

The runtime revert mechanism (boost expiration on `Ready=True`) is **delegated to `kube-startup-cpu-boost`**; NIMBUS provides the `StartupCPUBoost` CR with the offline-derived starting CPU value.

## 1. Problem Formulation

Notation follows the design slides ([SLO-Aware CPU Boost](https://docs.google.com/presentation/d/1e1scyFFjX0j7gYwsk2agD7BBA8D2v4NfZQS6fY05McQ/edit)):

| Symbol            | Type      | Definition                                                       |
|-------------------|-----------|------------------------------------------------------------------|
| `P = {p₁,…,pₙ}`   | set       | pending cold-start pod requests at time `t`                      |
| `N = {n₁,…,nₖ}`   | set       | worker nodes                                                     |
| `H(nⱼ) ∈ ℝ⁺`      | function  | available headroom on node `j`: `allocatable − Σ requests`       |
| `L(pᵢ)`           | tuple     | CPU level profile `(c_floor, c_min, c_opt)` for pod `i`, in *cold* and *warm* variants (§2) |
| `SLO(pᵢ) ∈ ℝ⁺`    | scalar    | target p95 TTFB latency in ms                                    |
| `lat(p, c) → ℝ⁺`  | function  | offline-profiled p95 latency at CPU `c` for the service of `p`   |
| `A : P → N ∪ {∅}` | function  | assignment (∅ = queued)                                          |
| `C : P → ℝ⁺`      | function  | CPU value the pod is admitted at                                 |

**Objective.**

```
maximize    |{ pᵢ : lat(pᵢ, C(pᵢ)) ≤ SLO(pᵢ) }|
subject to  ∀ nⱼ:  Σ_{A(p)=nⱼ}  C(p)  ≤  H(nⱼ)
            ∀ pᵢ:  C(pᵢ)  ≥  c_floor(pᵢ)
```

Plain English: maximize the number of pods that meet their latency target, subject to per-node CPU capacity and a per-pod survival floor.

## 2. CPU Level Model — 3 Levels per Phase

NIMBUS treats every pod under the **`request = limit` (Guaranteed QoS)** assumption — there's a single CPU value the pod runs at, no burst-above-limit. Every NIMBUS decision picks one of three levels per phase. The same triple `(c_floor, c_min, c_opt)` applies independently to the cold-start phase and the warm/running phase, derived from the corresponding offline samples.

| Level | Symbol    | Meaning                                                                                 | Source                                |
|-------|-----------|-----------------------------------------------------------------------------------------|---------------------------------------|
| L₋₁   | `c_floor` | minimum CPU at which the pod doesn't crash-loop. Safety net.                            | `min{c : P(success | cpu = c) = 1.0}` |
| L₁    | `c_min`   | minimum CPU satisfying the phase-appropriate p95 SLO. The default **pack target**.      | `min{c : lat(svc, c) ≤ SLO}`          |
| L₂    | `c_opt`   | knee of the latency curve — past this, marginal improvement is below `ε`. The **upgrade target**. | `argmin{c : ∂lat/∂c > −ε}`   |

Each ksvc therefore carries **two triples**:

```
cold phase:  ( c_floor_cold,  c_min_cold,  c_opt_cold )    derived from  coldRtSamples
warm phase:  ( c_floor_warm,  c_min_warm,  c_opt_warm )    derived from  warmRtSamples
```

**Ordering invariant** (within each phase): `c_floor ≤ c_min ≤ c_opt ≤ node_allocatable`.
**Cross-phase relationship** (typical): `c_*_cold > c_*_warm` — cold-start needs more CPU than steady state.

Visually, the latency curve and its level annotations (per phase):

```
RT (p95)
 │
 │\
 │ \
 │  \                         (c_floor — below this, crash-loop)
 │   \
 │    \
 │─────●─SLO────────────      (c_min — pack target, just under SLO)
 │      \
 │       \____●__             (c_opt — knee; flat beyond)
 │             \____
 │                 \____
 │                      ───────  cpu
 0    c_floor       c_min   c_opt
```

## 3. Phase 1 — Offline Profiling

NIMBUS profiles each ksvc per candidate node with two sequential binary searches (cold then warm), pinned to one pod via `maxScale=1`. Each probe runs `N` samples and returns their **p95**. Entry point: [api/algorithm/binary_search.go](api/algorithm/binary_search.go).

```
ALGORITHM runBinarySearch(svc, low, high, probe):
  samples := []
  rtLow := probe(svc, low);  samples.append((low, rtLow))

  while (high − low) > 100m:                          # convergenceThreshold
    mid := (low + high) / 2
    rtMid := probe(svc, mid);  samples.append((mid, rtMid))

    if (rtLow − rtMid) / rtLow > 10%:                 # improvementGate
      rtHigh := probe(svc, high);  samples.append((high, rtHigh))
      if (rtMid − rtHigh) / rtMid > 10%:
        low := mid;  rtLow := rtMid                   # mid significantly better → move low up
      else:
        high := mid                                   # diminishing returns → narrow toward mid
    else:
      high := mid                                     # mid no better than low → narrow

  return (high, samples)
```

| Constant                | Value | Meaning                                                  |
|-------------------------|-------|----------------------------------------------------------|
| `convergenceThreshold`  | 100 m | stop when `high − low ≤ 100 m`                           |
| `improvementGate`       | 10 %  | RT drop required to move the lower bound up             |
| `runningPhaseLowOffset` | −50 m | warm-phase `low` starts 50m below cold-phase `searchLow`|

**Illustrative example (post-§1.4 status).** Two ksvcs profiled with `coldSamples = warmSamples = 3` (each row's `rtMillis` is the p95 of those 3 samples). Sample lists are **deduplicated** (no two rows share the same `cpu`) and **sorted ascending by `cpu`** — see CLAUDE.md for the corresponding code change.

```json
{
  "master": {
    "coldRtSamples": [
      { "cpu":  "200m", "rtMillis": 47800 },
      { "cpu":  "650m", "rtMillis": 13100 },
      { "cpu":  "875m", "rtMillis":  7910 },
      { "cpu":  "931m", "rtMillis":  6240 },
      { "cpu":  "988m", "rtMillis":  5980 },
      { "cpu": "1100m", "rtMillis":  5480 },
      { "cpu": "2000m", "rtMillis":  5310 }
    ],
    "warmRtSamples": [
      { "cpu": "150m", "rtMillis": 4 },
      { "cpu": "247m", "rtMillis": 3 },
      { "cpu": "345m", "rtMillis": 2 },
      { "cpu": "540m", "rtMillis": 2 },
      { "cpu": "931m", "rtMillis": 2 }
    ],
    "startingCpu": "931m",
    "runningCpu":  "345m"
  },
  "worker": {
    "coldRtSamples": [
      { "cpu":  "200m", "rtMillis": 14000 },
      { "cpu":  "650m", "rtMillis":  8100 },
      { "cpu":  "706m", "rtMillis":  7560 },
      { "cpu":  "875m", "rtMillis":  6390 },
      { "cpu": "1100m", "rtMillis":  5950 },
      { "cpu": "2000m", "rtMillis":  5800 }
    ],
    "warmRtSamples": [
      { "cpu": "150m", "rtMillis": 3 },
      { "cpu": "289m", "rtMillis": 2 },
      { "cpu": "428m", "rtMillis": 2 },
      { "cpu": "706m", "rtMillis": 2 }
    ],
    "startingCpu": "706m",
    "runningCpu":  "289m"
  }
}
```

---

## 4. Sample export

When `spec.export.dir` is set on a Nimbus, the controller writes raw per-probe response-time samples to a timestamped directory tree as the binary search runs. Absent field → no export, identical to legacy behaviour.

### 4.1 CRD field

```yaml
spec:
  export:
    dir: "./results"                    # relative paths resolve against the controller's cwd
                                         # absolute paths are used as-is
                                         # ".." segments are rejected at runtime
```

### 4.2 Output tree

```
<spec.export.dir>/
└── 2026-05-08T14-23-11/                 ← run-start timestamp (UTC, ISO with hyphens)
    ├── meta.json                         ← Nimbus spec snapshot + candidate node list
    ├── master/
    │   ├── cold/
    │   │   ├── 300m.csv                  ← one CSV per probe-point CPU, raw rows
    │   │   ├── 650m.csv
    │   │   └── 706m.csv
    │   ├── warm/
    │   │   ├── 250m.csv
    │   │   └── 219m.csv
    │   └── result.json                   ← {startingCpu, runningCpu, timestamps}
    └── worker/
        ├── cold/...
        ├── warm/...
        └── result.json
```

### 4.3 CSV format

Each `<cpu>.csv` holds one row per **individual sample** (not per probe-point average). For `measurement.coldSamples: 3`, a single probe at 300m yields three rows:

```csv
index,rt_millis
1,4750
2,4810
3,4900
```

If the binary search re-probes the same CPU value later in the loop, rows are **appended** with monotonically-increasing index (4, 5, 6, …) — preserves per-iteration variance.

Everything else (node, phase, cpu, when) is encoded in the file path.

### 4.4 Implementation notes

- **Streaming write**: samples are written one row per sample as soon as they are measured. The controller never holds raw samples in a slice — peak memory for raw data is one `time.Duration` (8 bytes), overwritten on the next loop iteration. Lives in [`internal/export/`](internal/export/).
- **Atomic JSON writes**: `meta.json` and `result.json` are written via temp-file + rename so a concurrent reader never sees a partial file.
- **Per-node `result.json` after every node**: written at end-of-node, not end-of-run, so partial progress survives a mid-loop crash.
- **Errors are non-fatal**: every export call's error is logged via `logging.Warning` and ignored — the binary search continues regardless. This matters when running in a read-only filesystem or with a full disk.

---

## Online stage

The online-stage algorithm (EWMA burst detector, CA-BFD per pod, Predict-Revert admission, Degrade Waterfall, Upgrade Controller) is documented in [`online_plan.md`](online_plan.md). See:

- `online_plan.md` §3.9 — rigorous tier model (c_floor / c_min / c_opt) — same model as §2 above, lifted into the online context
- `online_plan.md` §3.2 — degrade waterfall
- `online_plan.md` §3.12 — EWMA burst detection
- `online_plan.md` §3.4 + Phase F'3 — upgrade controller (in-place pod resize)
- `online_plan.md` §5 Phase E' — CA-BFD + tier-selection logic
