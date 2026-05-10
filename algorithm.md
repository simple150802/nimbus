# NIMBUS Algorithm — SLO-Aware CPU Boost for Knative Cold-Start

NIMBUS is a controller that gates Knative cold-start CPU allocation under a node-budget constraint, using **offline-profiled response-time curves** to decide per-pod CPU at admission time, then upgrades and reverts allocations as the pod transitions through its lifecycle.

The algorithm is structured in **three phases**:

| Phase | What it does | Where it runs |
|-------|--------------|---------------|
| **1. Offline Profiling**          | Sweeps each ksvc across CPU levels, builds a profile store `Π` with `(c_floor, c_min, c_opt)` for both cold and warm phases | Existing NIMBUS controller (`api/algorithm/binary_search.go`) |
| **2. Online SLO-Aware Scheduling**| Per-pod-arrival CA-BFD bin-pack across nodes, with EWMA burst detection and a headroom ledger | NIMBUS mutating webhook (new `internal/gate/`) |
| **3. Runtime Lifecycle**          | Pod starts boosted at a cold-phase level → revert to warm-phase `c_min` on `Ready=True` → upgrade controller drains the queue | `kube-startup-cpu-boost` revert + new NIMBUS upgrade controller |

The repo integration touches three projects: `knative-serving`, `kube-startup-cpu-boost`, and `nimbus`. Pseudocode for each appears in §7.

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

## 4. Phase 2 — Online SLO-Aware Scheduling (CA-BFD)

The mutating webhook receives one pod per pod-creation event. For each pod it runs **CA-BFD** (Capacity-Aware Best-Fit Decreasing) over the cluster's nodes.

> **The pieces below compose — they are not alternatives.** Unlike the offline-stage objectives (e.g. Σ RT vs. proportional fairness vs. Nash bargaining) where you pick *one*, the components in §4 are layers of one system, **ordered here in execution sequence**: when a pod arrives, the **EWMA Burst Detector (§4.1)** ticks first to compute `burst_state`; **CA-BFD (§4.2)** consumes that state and queries the **Headroom Ledger (§4.5)** for per-node CPU; if no node fits at `c_opt`, NIMBUS first tries **Predict-Revert Admission (§4.3)** to wait for an in-flight cold-start to release CPU; if that can't help, it falls through to the **Degrade Waterfall (§4.4)**; once a decision is made, it commits to the Ledger; later, when a pod reaches `Ready=True` and reverts, the **Upgrade Controller (§4.6)** fires asynchronously to reclaim freed CPU. You implement and validate them in order, but you don't drop any.

| Step | Component (§)                       | Type                  | Role                                                                            | If dropped                                                                |
|------|-------------------------------------|-----------------------|---------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| 1    | **EWMA Burst Detector** (§4.1)      | mode parameter        | flip CA-BFD into burst mode (`target = c_min`, reserve 30 % headroom)           | bursts overrun the budget; no proactive headroom reservation              |
| 2    | **CA-BFD** (§4.2)                   | decision engine       | per-pod admission: pick node + target CPU                                       | no NIMBUS Phase 2 — fall back to stock K8s scheduler                      |
| 3    | **Predict-Revert Admission** (§4.3) | conditional queue     | when no fit at `c_opt`, wait briefly for an in-flight cold-start to revert and admit at `c_opt` instead of degrading | pods that *almost fit* get degraded immediately; SLO claim weakens at sweet-spot capacity |
| 4    | **Degrade Waterfall** (§4.4)        | fallback inside CA-BFD| walk `[c_opt → c_min → c_floor]` when no node fits and waiting won't help       | pods go Pending under capacity pressure; cluster appears crashed          |
| 5    | **Headroom Ledger** (§4.5)          | shared state          | logical headroom per node + atomic optimistic-lock commit                       | concurrent webhook calls race; over-commit; stale headroom                |
| 6    | **Upgrade Controller** (§4.6)       | async event handler   | on revert, upgrade admitted-at-`c_min` pods toward `c_opt`; drain pending queue | pods admitted at `c_min` stay there forever; freed CPU sits idle          |

**Minimum-viable subset:** CA-BFD + Headroom Ledger + Degrade Waterfall — validates RQ1 and RQ2 (§7). Adding EWMA Burst Detector + Upgrade Controller unlocks RQ3 (burst stress under capacity pressure). Adding Predict-Revert Admission unlocks RQ4 (sweet-spot capacity-pressure SLO recovery).

**Dependency diagram.** How the pieces wire together (top-to-bottom = execution sequence):

```
                 ┌─────────────────────────────────────┐
                 │  EWMA Burst Detector (§4.1)         │ ← decides "burst" or "normal"
                 └────────────────┬────────────────────┘
                                  │ feeds burst_state into ↓
                 ┌────────────────▼────────────────────┐
                 │  CA-BFD per pod (§4.2)              │ ← the admission decision
                 └────┬───────────────────────────┬────┘
                      │ on success                │ on "no node fits at c_opt"
                      ↓                           ↓
                 ┌────────────┐            ┌──────────────────────────┐
                 │ Ledger     │◄───────────│  Predict-Revert (§4.3)   │ ← skipped if burst mode
                 │ COMMIT     │            └──────────┬───────────────┘
                 └────────────┘                       │ if no in-flight revert helps
                      ▲                               ↓
                      │                    ┌──────────────────┐
                      │                    │ Degrade Waterfall│
                      │  REVERT events     │     (§4.4)       │
                      │  feed back from    └────────┬─────────┘
                      │  Phase 3                    │ commit at degraded c
                      │                             ▼
                      │                        Ledger COMMIT
                 ┌────┴──────────────────────────────────┐
                 │  Upgrade Controller (§4.6)            │ ← post-revert reclamation
                 └───────────────────────────────────────┘

                 Headroom Ledger (§4.5) is the shared state
                 every algorithm above reads/writes.
```

### 4.1 EWMA Burst Detector

The burst state consumed by CA-BFD (§4.2) is driven by two **exponentially-weighted moving averages** — recomputed first on each pod arrival:

```
SIGNAL 1 — instantaneous arrival rate (α = 0.3):
  ewma_rate    ← α · (1 / Δt)  +  (1 − α) · ewma_rate

SIGNAL 2 — rate of change of arrival rate (β = 0.2):
  ewma_delta   ← β · (ewma_rate − prev_ewma_rate)  +  (1 − β) · ewma_delta

is_burst = ( ewma_rate  > threshold_rate )
        OR ( ewma_delta > threshold_delta )

Decay to NORMAL: after 30 s with no new cold-starts.
```

#### What EWMA is

A running average where **recent samples count more than old ones**, with the weight decaying exponentially as samples get older. The general shape is one line:

```
ewma_new = weight · sample + (1 − weight) · ewma_old
```

`weight` decides how reactive the average is to new samples — higher `weight` → fast reaction, noisy; lower `weight` → smooth, slow.

#### Signal 1 — `ewma_rate` (the speedometer)

```
ewma_rate ← α · (1 / Δt) + (1 − α) · ewma_rate
                ────┬───
                    new sample = "instantaneous arrival rate right now"
```

- **`Δt`** — time since the previous pod arrival, in seconds. Two pods 0.5 s apart ⇒ `Δt = 0.5`.
- **`1 / Δt`** — the instantaneous arrival rate in pods/s. `Δt = 0.5 s ⇒ 1/Δt = 2 pods/s`.
- **`α = 0.3`** — the smoothing factor: **30 % weight on the new sample, 70 % on history**.
  - Higher `α` (e.g. 0.8): reactive — `ewma_rate` jumps fast on a single sample. Good detection, but noisy (false positives on one fast arrival).
  - Lower `α` (e.g. 0.05): smooth — `ewma_rate` barely moves on one sample. Misses real bursts until many fast arrivals accumulate.
  - **`α = 0.3` is the middle ground.** Half-life ≈ 2 samples — after ~2 new arrivals, an old reading's contribution is halved.

`ewma_rate` answers: *"averaged over the last few arrivals, how fast are pods coming?"*

#### Signal 2 — `ewma_delta` (the accelerometer)

```
ewma_delta ← β · (ewma_rate − prev_ewma_rate) + (1 − β) · ewma_delta
                  ───────────┬──────────────
                             new sample = "how much did rate change this step?"
```

- **`ewma_rate − prev_ewma_rate`** — the *change* in `ewma_rate` from one tick to the next; a discrete derivative.
- **`β = 0.2`** — same idea as `α`, but applied to the derivative: **20 % weight on the new "rate change", 80 % on history**.
- `β < α` is intentional: the derivative is inherently noisier (subtracting two moving averages), so it gets smoothed harder.

`ewma_delta` answers: *"is the arrival rate growing or shrinking?"*

| `ewma_delta` value | Meaning                                                  |
|--------------------|----------------------------------------------------------|
| `> 0`              | rate is **accelerating** → a burst is building          |
| `≈ 0`              | rate is steady                                           |
| `< 0`              | rate is **decaying** → burst is fading                  |

#### The two-signal decision

```
is_burst = ( ewma_rate  > threshold_rate )
        OR ( ewma_delta > threshold_delta )
```

OR'd, not AND'd — either signal alone is enough to declare burst mode. Why both:

| Signal       | Detects                                       | Limitation alone                                                          |
|--------------|-----------------------------------------------|---------------------------------------------------------------------------|
| `ewma_rate`  | "we *are* busy right now"                     | reacts only after the burst has built up — too late to reserve headroom   |
| `ewma_delta` | "we're *about to be* busy" (acceleration)     | can spike on a single fast inter-arrival; needs corroboration             |

**`ewma_delta` typically crosses its threshold one event before `ewma_rate` does** — that one-tick early warning is what lets NIMBUS reserve 30 % headroom *before* the wave hits.

| Physics analogy | Signal      | Detects                 |
|-----------------|-------------|-------------------------|
| Speed           | `ewma_rate` | "currently going fast"  |
| Acceleration    | `ewma_delta`| "speeding up"           |

#### The decay rule

```
Decay to NORMAL: after 30 s with no new cold-starts.
```

The **hysteresis** that prevents flip-flopping. Without it, after a burst dies down `ewma_rate` and `ewma_delta` both decay back below their thresholds — but a single late-arriving pod could re-trip them, producing burst → normal → burst → normal oscillation. The 30 s silence requirement forces a clean exit.

### 4.2 CA-BFD per pod

```
ALGORITHM CA_BFD(p, N, Π, ledger, burst_state):
  (c_floor, c_min, c_opt) ← Π[svc(p)].cold        # cold-phase triple for admission

  # 1. Choose target CPU based on burst mode (from §4.1)
  if burst_state.is_burst():
    target  ← c_min                     # pack tighter under burst
  else:
    target  ← c_opt

  # 2. Best-Fit across nodes — pick node with smallest slack after assign
  candidates ← { nⱼ : H(nⱼ) ≥ target }
  if candidates non-empty:
    n* ← argmin_{nⱼ ∈ candidates} ( H(nⱼ) − target )
    A(p) ← n*;   C(p) ← target
    ledger.commit(p, n*, target)        # atomic; see §4.5
    annotate p with nodeAffinity → n*.hostname
    return (n*, target)

  # 3. No fit at c_opt: try predict-revert (see §4.3) before degrading
  if not burst_state.is_burst() and PredictRevertAdmit(p, ledger, MAX_WAIT):
    return QUEUED(admit_after = t_rev, target_cpu = c_opt(p))

  # 4. Last resort — Degrade waterfall (see §4.4)
  return Degrade(p, N, Π, ledger)
```

**Complexity.** O(log K) per pod with a heap-sorted headroom ledger. K = number of nodes.

**Competitive ratio.** Online BFD has worst-case ratio ≈ 1.7 × OPT (vs offline FFD+BFD's `(11/9)·OPT + 6/9`). Acceptable in exchange for not needing batch aggregation.

### 4.3 Predict-Revert Admission

When CA-BFD finds no node has `H ≥ c_opt`, NIMBUS doesn't immediately Degrade. First it asks: **"is an in-flight cold-start about to revert? If yes, can the new pod wait for that revert and be admitted at `c_opt` instead?"**

Every in-flight admission carries a **predicted revert time** computed at admission and stored alongside its boost in the Ledger (§4.5):

```
expected_revert(p) = t_admit(p) + lookupRT(coldRtSamples_{svc(p)}, C(p))
```

`lookupRT` is the same piecewise-linear interpolation over the §3 samples that CA-BFD uses to evaluate `c_opt`/`c_min`/`c_floor` — **no additional offline data needed**.

```
ALGORITHM PredictRevertAdmit(new_pod, ledger, MAX_WAIT):
  candidates ← [ (peer, expected_revert(peer))
                 for peer in ledger.boost_active
                 if 0 < (expected_revert(peer) − now()) ≤ MAX_WAIT ]
  if candidates is empty:
    return false                                    # nothing to wait for

  sort candidates by expected_revert ascending
  for (peer, t_rev) in candidates:
    n := peer.node
    # Headroom that will exist *after* peer's boost reverts to c_min_warm
    post_revert_headroom := ledger.hr_logical[n] + ( peer.boost_size − c_min_warm(peer) )
    if post_revert_headroom ≥ c_opt(new_pod):
      ledger.reserve(new_pod, n, c_opt(new_pod), admit_after = t_rev)
      schedule_admit_at(t_rev, new_pod, n, c_opt(new_pod))
      return true                                   # queued for c_opt, will fire at t_rev

  return false                                      # no in-flight revert frees enough
```

**Watchdog** — handles stuck cold-starts whose predicted revert never arrives:

```
WATCHDOG runs every 1 s:
  for (peer, t_rev) in ledger.boost_active:
    if now() > t_rev + grace:                       # grace = 0.5 · expected_duration
      mark peer as stuck
      pull all queued waiters reserved on peer.node
      Degrade(each waiter, ledger)                  # they couldn't wait any longer
```

**Constants:**

| Constant   | Suggested value           | Meaning                                        |
|------------|---------------------------|------------------------------------------------|
| `MAX_WAIT` | 5 s                       | upper bound on admission queue time            |
| `grace`    | 0.5 × `expected_duration` | timeout buffer before marking a peer stuck     |

**Burst-mode interaction:** Predict-Revert is **disabled** when `burst_state.is_burst()`. Under burst, EWMA has chosen "pack tight at `c_min`"; waiting contradicts that decision. CA-BFD's call site (above) makes this skip explicit.

**Why it matters:** in the operating regime where `max(c_opt) + c_min_warm < headroom < Σ c_opt`, this branch flips a pod from being admitted at degraded CPU (likely SLO miss) to being admitted at full `c_opt` after a brief wait. Outside that regime, it's a no-op (returns `false`, falls through to Degrade). The expected effect-size on SLO miss rate is documented under RQ4 / experiment E4 in §7.

### 4.4 Degrade Waterfall

When no node has `H ≥ target`, NIMBUS walks down the level ladder rather than failing outright:

```
ALGORITHM Degrade(p, N, Π, ledger):
  candidates ← [ c_opt, c_min, c_floor ]            # cold-phase triple, descending

  for c in candidates:
    n* ← argmin_{nⱼ : H(nⱼ) ≥ c} H(nⱼ)
    if n* exists:
      A(p) ← n*;   C(p) ← c
      annotate(p, level = level_of(c), slo_miss = (c < c_min))
      ledger.commit(p, n*, c)            # see §4.5
      return (n*, c)

  # No node has ≥ c_floor → queue
  pending_queue.push(p)
  return (∅, QUEUE)
```

**SLO-miss is explicit.** When a pod is admitted below `c_min`, NIMBUS annotates it with `nimbus.io/slo_expected_miss = "true"` so the SLO breach is visible in metrics and never silent. With `request = limit`, the pod runs at exactly `C(p)` for the entire cold-start phase — no burst-above-limit safety valve.

### 4.5 Headroom Ledger

Shared state read by CA-BFD (§4.2), Predict-Revert (§4.3), and Degrade (§4.4); committed by all three; updated by REVERT events from Phase 3 that feed the Upgrade Controller (§4.6). Separates *physical* node headroom (refreshed from kubelet every 5 s) from *logical* headroom (which accounts for in-flight boosts that haven't reverted yet).

```
LEDGER:
  hr_physical[nⱼ]      from kubelet, refreshed every 5 s
  hr_logical[nⱼ]       = hr_physical[nⱼ] − Σ active boosts on nⱼ
  boost_active         map[ pod_id → (node, boost_size) ]

  # Atomic operations (optimistic lock)
  COMMIT(p, n, c):
    lock(n)
    hr_fresh ← read_kubelet(n)
    if |hr_fresh − hr_physical[n]| > δ:    # 10% drift tolerance
      retry CA_BFD(p, …)                    # at most 2 retries, then degrade
    hr_logical[n] -= c
    boost_active[p] = (n, c)
    unlock(n)

  REVERT(p):
    (n, c) ← boost_active[p]
    hr_logical[n] += c
    delete boost_active[p]
    trigger Upgrade_Controller(n, freed=c)   # see §4.6
```

The ledger also stores `expected_revert(p)` per in-flight pod (used by Predict-Revert, §4.3) and exposes a `reserve(p, n, c, admit_after)` operation for queued admissions whose actual COMMIT fires at `admit_after`.

### 4.6 Upgrade Controller

Triggered asynchronously by a `StartupCPUBoost` revert event (Phase 3) routed through the Ledger's REVERT operation (§4.5). Walks the pending queue and uses freed headroom to *upgrade* admitted pods from `c_min` toward `c_opt`:

```
UPGRADE_CONTROLLER(node, freed):
  if burst_state.is_burst():
    return                                  # hold the headroom; another burst is incoming

  P_n ← pending pods on `node`, sorted by gap (c_opt − c_min) descending
  for p in P_n while freed > 0:
    Δ ← min( c_opt(p) − C(p),  freed )
    PATCH p.spec.containers[*].resources.limits.cpu = C(p) + Δ
    C(p) += Δ
    freed -= Δ
```

The "JVM-first" priority falls out naturally: services with the largest gap (`c_opt − c_min`, e.g. JVM workloads) get upgraded before lighter ones (Flask, Node.js).

## 5. Phase 3 — Runtime Lifecycle

End-to-end timeline for one cold-start pod:

```
t₀          : scale-from-zero request arrives at Knative
t₀ + ε      : KPA decides desiredScale; pod created
t₀ + ε      : NIMBUS webhook runs CA-BFD, sets requests.cpu = limits.cpu = C(p)
              (cold-phase level, Guaranteed QoS), adds nodeAffinity → A(p),
              creates StartupCPUBoost CR
t₁          : pod scheduled on A(p)
t₂          : container init at C(p) CPU (boosted)
t₃          : pod.status.conditions[Ready]=True
              → revert: requests.cpu = limits.cpu := c_min_warm(p)
              → delete StartupCPUBoost CR
              → ledger.REVERT(p)  // frees C(p) − c_min_warm(p) on the node
              → Upgrade_Controller fires
```

Visualized:

```
CPU
 │
 │     boosted (C(p) ∈ {c_min_cold, c_opt_cold})
 │      ──────────────●
 │                     ↘ revert at Ready=True
 │                       ●─────────────────  c_min_warm (steady state)
 │                                ↑ optional warm-upgrade to c_opt_warm
 │
 └────┬──────┬─────────┬──────────────────  time
     t₀    init       Ready
```

**Burst behavior.** If `m` pods arrive in a window `[t₀, t₀ + Δ]`:

1. Each pod's webhook call independently runs CA-BFD against the live ledger — no batch-window aggregation.
2. EWMA detects the spike; subsequent pods admit at `c_min` instead of `c_opt`, reserving 30 % headroom.
3. As pods reach `Ready=True`, their reverts free CPU; `Upgrade_Controller` drains the pending queue and / or upgrades admitted pods toward `c_opt`.
4. Feedback-loop latency (revert → re-admit) is bounded by ~500 ms.

## 6. Properties & Guarantees

| Property | Statement |
|----------|-----------|
| **Feasibility** | `∀ p admitted: C(p) ≥ c_floor(p)`; `∀ nⱼ: Σ_{A(p)=nⱼ} C(p) ≤ H(nⱼ)`. Enforced by `Degrade`'s ladder + ledger's atomic commit. |
| **SLO preservation (when admitted at L₁)** | If `A(p) ≠ ∅` and `C(p) ≥ c_min(p)`, then `lat(p, C(p)) ≤ SLO(p)` (by definition of `c_min`). |
| **SLO miss is explicit** | If `C(p) < c_min(p)`, NIMBUS annotates `slo_expected_miss=true` — never silent. |
| **Pack optimality (offline FFD+BFD)** | `OPT(I) ≤ BFD(I) ≤ (11/9) · OPT(I) + 6/9`. |
| **Pack optimality (online)** | Online competitive ratio ≈ 1.7 × OPT worst-case; near-optimal under Poisson arrivals. |
| **Upgrade optimality** | Greedy fractional knapsack on `(c_opt − C)`, sorted descending — optimal CPU utilization on each node. |
| **Revert safety** | After `Ready=True`, `C(p) := c_min_warm(p)`; never reverts to `c_floor`. Prevents startup-CPU leak into steady state. With `request = limit`, this single value applies to both. |
| **Webhook overhead** | All steps `O(N · K)`; `H_cache` refresh async every 5 s; optimistic lock retries ≤ 2; target p99 webhook latency < 50 ms. |

