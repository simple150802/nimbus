# NIMBUS Future Plan — Offline + Online Phases

> Status: design report covering both reconcile phases of the NIMBUS controller.
> Scope: a self-contained specification of inputs, algorithms, outputs, and the
> design decisions behind each.
>
> For the end-user how-to-use guide see [README.md](README.md). OVERVIEW.md
> is self-contained — it does not depend on other docs in this repository.

---

## Table of Contents

- [1. Abstract](#1-abstract)
- [2. Problem Statement](#2-problem-statement)
- [3. Scope and Goals](#3-scope-and-goals)
  - [3.1 Offline Phase](#31-offline-phase)
  - [3.2 Online Phase](#32-online-phase)
- [4. User Input — the `Nimbus` Custom Resource](#4-user-input--the-nimbus-custom-resource)
- [5. Phase Definitions — Cold vs Warm](#5-phase-definitions--cold-vs-warm)
  - [5.1 Cold Phase](#51-cold-phase)
  - [5.2 Warm Phase](#52-warm-phase)
- [6. Operating Points — `c_opt`, `c_min`, `cpuBudget`](#6-operating-points--c_opt-c_min-cpubudget)
- [7. Offline Phase — Plan](#7-offline-phase--plan)
  - [7.1 Reconcile Flow](#71-reconcile-flow)
  - [7.2 Algorithms (Pseudocode)](#72-algorithms-pseudocode)
  - [7.3 Output — `status.perNode`](#73-output--statuspernode)
- [8. Online Phase — Plan](#8-online-phase--plan)
  - [8.1 Architecture Overview](#81-architecture-overview)
  - [8.2 Burst Detector](#82-burst-detector)
  - [8.3 Placement Decision](#83-placement-decision)
  - [8.4 Output — `status.online`](#84-output--statusonline)
- [9. Glossary](#9-glossary)

---

## 1. Abstract

NIMBUS is a Kubernetes controller that **automatically right-sizes CPU** for
Knative services running serverless workloads (e.g. CPU-bound inference). A
single reconcile loop drives two phases against one Nimbus Custom Resource:

- **Offline phase** ([§7](#7-offline-phase--plan)) — measure, on one
  representative node from the declared pool, the CPU-vs-response-time curve
  for both **cold start** and **warm steady-state** request handling. Output:
  the pool's CPU operating points the online phase consumes.
- **Online phase** ([§8](#8-online-phase--plan)) — expose a synchronous
  HTTP `/decide` endpoint invoked by the Knative autoscaler on every
  `0 → 1` cold-start. A background **burst detector** maintains a shared
  state (NORMAL / BURST); the placement decision applies a mode-aware
  waterfall over live cluster headroom (`c_opt` pool-wide → `c_min`
  pinned → `Pending`) and patches the ksvc PodSpec + `StartupCPUBoost`
  CR before returning to KPA.

### The operator declares two endpoints and two SLOs

The platform user (the operator deploying their service on NIMBUS) declares,
in one `Nimbus` Custom Resource, **two probe URLs and two response-time
budgets**. NIMBUS — acting as the cloud-platform layer — commits to
satisfying both:

| Endpoint | What it does on the workload | What its SLO measures |
|---|---|---|
| `/status` | The app's **readiness probe**. Returns `READY` *only when the workload is fully initialized*. For a YOLO-style inference service, "initialized" means model weights loaded into RAM, OpenCV / PyTorch / native bindings set up, and any first-request lazy work completed. Returns `LOADING` (HTTP 503) until then. | **Cold-start latency** — how long from pod creation to the app being serviceable end-to-end (`spec.acceptableResponseTime.cold`, e.g. 1500 ms). |
| `/detect/local` | A **representative steady-state workload** — for YOLO, one inference pass on a bundled 640×640 image. CPU-intensive in proportion to the model's per-frame cost. | **Warm request latency** — per-request response time once the pod is warm (`spec.acceptableResponseTime.warm`, e.g. 50 ms). |

These two SLOs are how the operator expresses their bargain with end users:
*"cold-start no slower than X, warm requests no slower than Y."* NIMBUS
sizes CPU on every candidate node so both bargains are met at the
**cheapest possible CPU**. Everything else — node discovery, CPU sweeps,
sample collection, operating-point derivation, placement, tier downgrade
under contention — is automated.

---

## 2. Problem Statement

Knative deployments require operators to set CPU limits/requests manually. In practice
this is **either over-provisioned** (wasting cluster capacity) **or
under-provisioned** (violating user-facing latency targets), because:

- CPU sensitivity is **workload-dependent** and **bimodal** (cold start ≠
  steady state); a single CPU number cannot describe both correctly.
- CPU sensitivity is **node-dependent**: different SKUs or generations yield
  different cold-start times at the same CPU limit.
- Latency-vs-CPU is **non-linear** with a clear knee point that human
  operators consistently misjudge.

NIMBUS mechanizes the measurement (offline) and the per-ksvc placement
(online) so the operator's only manual input is **(a) a CPU budget ceiling
(`cpuBudget`)** and **(b) the SLO** they want to meet.

### Economic value to the cloud provider

The two inputs together turn NIMBUS into a **revenue-positive** platform
mechanism:

- The operator (cloud customer) commits to paying for **at most `cpuBudget`
  CPU** per ksvc, in exchange for the SLO being honored.
- NIMBUS finds the **smallest CPU strictly below `cpuBudget`** that still
  meets the SLO. The difference, `cpuBudget − assigned`, is **reclaimable
  by the platform provider** — it can be sold to additional customers on
  the same hardware, lowering per-customer infrastructure cost.
- Without NIMBUS, operators must over-provision conservatively (set
  `cpuBudget` high "just in case") and the provider must pessimistically
  reserve the full `cpuBudget` per pod, leaving headroom unused. NIMBUS
  converts that pessimism into measured precision and frees the unused
  budget back to the provider's pool.

So `cpuBudget` is **not a NIMBUS-internal search parameter**: it is the
operator's economic contract with the provider, and the provider's value
proposition is to honor the operator's SLO at a strictly smaller CPU than
the declared budget.

---

## 3. Scope and Goals

### 3.1 Offline Phase

**In scope.** Resolve the declared node pool (`spec.placement.nodeSelector`)
and pick one representative node; run an automated CPU sweep on it under cold
and warm phases; converge on the saturated point (`c_opt`); derive the
SLO-meeting point (`c_min`) from the sample curves; persist the representative's
operating points + full sample curves to `.status.perNode` (treated as the
pool profile); write the pool `nodeSelector` + running CPU onto every managed
ksvc (recorded in `.status.applied`); support resume-after-restart and preload
of previously-exported runs. **Pool homogeneity is an operator contract**
(see §7.1) — NIMBUS measures one node and trusts that the rest are equivalent.

**Out of scope.** Per-ksvc placement, headroom-aware live reconciliation,
burst detection, custom kube-scheduler plugins, multi-tenant access control.

### 3.2 Online Phase

**In scope.** Expose a synchronous HTTP `/decide` endpoint that the
modified Knative KPA calls on every `0 → 1` cold-start; run a background
**burst detector** that maintains a shared `BurstState` (NORMAL / BURST)
from observed cold-start arrivals; apply a mode-aware waterfall
(`c_opt` pool-wide → `c_min` pinned → `Pending`) using live cluster
headroom; patch each ksvc's PodSpec (`nodeSelector`, CPU `requests`/`limits`)
plus its `StartupCPUBoost` CR so kube-scheduler + the boost webhook
execute NIMBUS's intent; record each decision in
`.status.online.assignments` for downstream experiment scripts.

**Out of scope.** Anticipatory deferral / pending-cold-start registry
(Problem 3 — deferred, see decision #16);
in-place pod `/resize` for tier upgrades (deferred until Kubernetes 1.27+
feature gate is universally available); production single-URL routing;
ksvc creation / deletion automation.

**Required Knative modification.** KPA's `kpa/scaler.go` reconciler gains
one synchronous outbound HTTP call to NIMBUS before patching
`Deployment.spec.replicas`. The change is contained — one call site, one
timeout, one fail-open path — but is not present in upstream Knative.

---

## 4. User Input — the `Nimbus` Custom Resource

The operator applies **one YAML manifest**. All measurement and SLO
parameters live here; no controller-side configuration is required.

```yaml
apiVersion: lazyken.io/v1alpha1
kind: Nimbus
metadata:
  name: boost-001
  namespace: serverless

# Which ksvcs to manage. They must be clones of the same workload — they
# share ONE measured profile. Offline pins values[0] to the representative
# node to take the measurement; online responds to per-ksvc cold-start
# events via its /decide endpoint, using the shared operating points. All
# listed ksvcs must already exist.
selector:
  matchExpressions:
    - key: serving.knative.dev/service
      operator: In
      values:
        - measure-yolo-001
        - measure-yolo-002
        - measure-yolo-003

spec:
  # REQUIRED. The Nimbus-owned node pool. Offline resolves this to Ready
  # nodes, sorts by name, and measures the first as the representative for
  # the whole pool; at apply time the same selector is written verbatim
  # onto every ksvc in selector.values[] (overwriting any user-set keys).
  # Label only nodes of EQUAL computing power into one pool — NIMBUS
  # measures one and applies it pool-wide without verifying equivalence.
  placement:
    nodeSelector:
      nimbus.io/pool: serverless

  # Which RT percentile the SLO check gates on. Default p95.
  metric: p95

  # Per-container CPU budget. cpuBudget is the operator's economic
  # commitment: "I am willing to pay for at most this much CPU per ksvc
  # for the named container." NIMBUS will NEVER assign more CPU than
  # this. Its value proposition is to find an assignment STRICTLY BELOW
  # cpuBudget that still satisfies the SLO; the unused headroom
  # (cpuBudget − assigned) is reclaimable by the platform provider.
  #
  # No lower bound is needed: NIMBUS auto-discovers the lower operating
  # range from the workload's response curve (halving down from
  # cpuBudget until the SLO breaches). The operator should not have to
  # guess a lower CPU — that is what the controller is for.
  #
  # The term cpuBudget is deliberately distinct from Kubernetes'
  # `resources.limits.cpu` to avoid overload: cpuBudget is an *economic
  # ceiling*; resources.limits.cpu is what NIMBUS will actually write
  # into the pod spec at apply time (always ≤ cpuBudget, set equal to
  # resources.requests.cpu for Guaranteed QoS).
  containerPolicies:
    - containerName: user-container
      cpuBudget: "2000m"

  # Two HTTP probes, one per phase.
  durationPolicy:
    # Cold-phase gate. Used by NIMBUS during cold probes AND embedded in
    # the per-ksvc StartupCPUBoost CR so the upstream boost webhook polls
    # the same URL.
    coldApiCondition:
      path: "/status"
      response: "READY"           # body must contain this substring

    # Warm-phase gate. Hits a real workload endpoint so RT scales with CPU.
    warmApiCondition:
      path: "/detect/local"
      statusCode: 200              # HTTP code that means success
      bodyContains: "\"success\":true"   # optional defensive check

  # How many raw samples per probe-point. Lower N = faster sweep, less
  # statistical confidence at the chosen percentile.
  measurement:
    coldSamples: 3
    warmSamples: 10

  # User SLO. Offline derives c_min from these.
  # cold: time-to-first-response (model load + cold path).
  # warm: per-request steady-state latency.
  acceptableResponseTime:
    cold: 1500   # ms
    warm: 50     # ms

  # Optional: stream raw samples to disk for offline analysis.
  export:
    dir: "./results"

  # Optional: load a previously-exported run instead of remeasuring.
  # Thesis-scope rule: the loaded run should have been exported with the
  # same metric and acceptableResponseTime values. c_min is imported as a
  # snapshot, not recomputed under a different SLO.
  # preMeasured:
  #   loadFromDir: "./results/backup"
```

**Summary of operator input**: the operator declares (a) which ksvcs are
involved, (b) which two URLs to probe, and (c) what response time is
acceptable for each phase. NIMBUS performs every other step automatically.

---

## 5. Phase Definitions — Cold vs Warm

NIMBUS measures **two distinct workload regimes** because they have
fundamentally different CPU sensitivity. The split is what makes the warm
operating point load-bearing — using one probe for both regimes produces
unusable warm numbers.

### 5.1 Cold Phase

- **What it measures**: total wall-clock from pod creation to first
  user-visible successful response. Dominated by container start + model /
  framework load.
- **Probe mechanism**: hit `coldApiCondition.path` (e.g. `/status`) and
  re-issue every few seconds until the response body contains
  `coldApiCondition.response` (e.g. `READY`). Elapsed time is one
  cold-start measurement.
- **Why this gate**: `/status`-style endpoints return immediately once the
  app is initialized, so the substring match is a clean "the app is ready"
  signal. Measurement duration is dominated by initialization itself, not by
  HTTP work.
- **Operating point**: `startingCpu` per node.

### 5.2 Warm Phase

- **What it measures**: steady-state per-request latency at a given CPU
  limit, after the app is already initialized.
- **Probe mechanism**: hit `warmApiCondition.path` (e.g. `/detect/local`,
  which runs real inference) and gate on `statusCode == 200` plus an
  optional `bodyContains` substring. Request/response time is the
  measurement.
- **Why this gate**: a status-flag endpoint completes in milliseconds
  regardless of CPU and is useless as a steady-state benchmark. The warm
  gate must invoke real CPU-bound work so measured RT varies meaningfully
  across the CPU search range.
- **Operating point**: `runningCpu` per node.

Independence of the two phases — own URL, own gate, own operating point — is
intentional and load-bearing.

---

## 6. Operating Points — `c_opt`, `c_min`, `cpuBudget`

For the measured representative node and each phase, the offline phase emits
two measured CPU values (`c_opt`, `c_min`) that the online phase consumes,
both bounded by one operator-supplied input (`cpuBudget`):

| Symbol | Meaning | How derived |
|---|---|---|
| **`cpuBudget`** | Operator-declared **economic ceiling** — the maximum CPU per ksvc the operator is willing to pay for. NIMBUS will never assign more than this. | `nimbus.spec.containerPolicies[0].cpuBudget`. Required field. |
| **`c_opt`** | The CPU value where the latency curve **saturates** — increasing CPU further produces less than a small percent improvement in RT. The "knee" of the curve. NIMBUS's preferred tier. | Output of the offline binary search (one value per phase per node). |
| **`c_min`** | The smallest CPU value at which measured RT is **within the user's SLO** budget. NIMBUS's fallback tier when `c_opt` doesn't fit. | Derived by `DeriveMin` from the sample curve produced by `runBinarySearch`: the nearest (smallest-CPU) sample whose RT meets the SLO. |

### Thesis scope — cold-start optimization only

The online stage optimizes the **cold/boost CPU only**. The steady-state warm
side is **locked to `c_opt_warm`** across every tier: `ksvc.spec.template.spec
.containers[0].resources.{requests,limits}.cpu = c_opt_warm` is written once
by offline at saturation and never changes thereafter. The online waterfall
varies only the `StartupCPUBoost` CR's `cpu` (the cold/boost value) and the
ksvc `nodeSelector` (pool-wide or pinned). This keeps the steady-state CPU
contract simple and makes cold-start the sole experimental dimension.

### Graceful degradation, not strict refusal

There **is** a third tier — `best_fit` — but it is degraded, pinned, and
explicitly labelled as such. When neither `c_opt` nor `c_min` fits pool-wide,
NIMBUS pins the ksvc to the node with the most free CPU and uses *all of that
node's available room* as the cold/boost CPU. Admission floor is `c_opt_warm`:
the pinned node must have at least `c_opt_warm` free so the post-revert
steady-state request still fits. Below that, `/decide` returns **Pending** and
KPA aborts the scale-up. The status row carries `tier=best_fit` and
`degraded=true` for experiment-CSV attribution. See [§8.3](#83-placement-decision)
for the full waterfall.

Invariant: `c_min ≤ c_opt ≤ cpuBudget`, holding **independently for the
cold and warm phases** (i.e. `c_min_cold ≤ c_opt_cold ≤ cpuBudget` and
`c_min_warm ≤ c_opt_warm ≤ cpuBudget`). The cross-phase relation
`c_opt_warm ≤ c_opt_cold` is also expected — steady-state work needs less
CPU than cold-start work — but is not enforced; the offline algorithm
treats the two phases as independent measurements. If `c_min` is undefined
for a phase (no sample satisfies the SLO budget), the controller logs a
warning and the online phase falls back to a single-tier model (`c_opt`
only for that phase). If even `c_opt` doesn't fit anywhere, the `/decide`
call returns Pending — see [§8.3](#83-placement-decision).

**Edge case — `c_min = c_opt` collapse.** If the workload's RT only meets
the SLO at (or very near) `cpuBudget`, the offline algorithm produces
`c_min = c_opt = cpuBudget`: there is no smaller CPU that satisfies the SLO,
so the two operating points coincide. The online phase still functions but
has **no tier separation** — `c_min` and `c_opt` are the same value, so the
mode-aware waterfall in BURST mode (which would normally drop from `c_opt`
to `c_min`) becomes a no-op. Economically, the provider has no headroom to
reclaim — every assignment consumes the full `cpuBudget`. **This plan
assumes the case does not occur in practice** for
the workloads in scope; if it does, the operator should either raise
`cpuBudget` or loosen the SLO until a meaningful gap reappears. (A stricter
sub-case — the SLO is not met *even at* `cpuBudget` — is reported by the
algorithm as `infeasible` and surfaces as a controller error rather than a
collapsed assignment.)

```
   RT
   │
   │\
   │ \
   │  \
   │   \      ╱─── flat region: extra CPU buys little RT improvement
   │    \    ╱
   │     \  ╱
   │      \╱
   │      ╱╲────────── (≈ saturation point = c_opt) ──────────────
   │     ╱  \
   │    ╱    \
   │ ──┤·······┐ user SLO budget (acceptableResponseTime)
   │   :       :\
   │   :       : \
   │   :       :  \____________
   │   :       :                              ▲
   └───┴───────┴──────────────────────────────┴──────────────────► CPU
              c_min                          c_opt           cpuBudget
              (smallest                      (saturated      (operator
               sampled                        point —         budget
               CPU meeting                    NIMBUS's        ceiling)
               SLO)                           ideal pick)
```

---

## 7. Offline Phase — Plan

### 7.1 Reconcile Flow

For the thesis POC, offline placement is **node-pool-only**. Nimbus does
not infer the pool from existing ksvc placement and does not profile every
candidate node. The Nimbus manifest declares one pool selector; every ksvc
controlled by that Nimbus belongs to the same pool.

> **Pool-homogeneity contract.** A pool is, by definition, a set of nodes
> with **equal computing power**. NIMBUS measures one representative and
> applies its profile pool-wide, so this only holds if every member is
> equivalent. NIMBUS does **not** verify capacity/model across the pool —
> it is the operator's responsibility to label only equal-compute nodes
> together (and to use separate pools + separate Nimbus CRs for distinct
> hardware classes).

The offline reconciler runs on every worker tick where the Nimbus's
`AllSaturated` flag is false. Per Nimbus, per tick:

```mermaid
flowchart TD
    Start[Worker tick fires] --> ReadPlacement[Read Nimbus manifest<br/>spec.placement.nodeSelector]
    ReadPlacement --> ResolvePool[Resolve node pool:<br/>list Ready schedulable Nodes<br/>matching selector]
    ResolvePool --> Pick[Choose representative node:<br/>first Ready node sorted by name]
    Pick --> Load[loadProfileFromStatus:<br/>read existing pool profile]
    Load --> Check{Pool profile<br/>already saturated?}
    Check -->|Yes| HandOff[/decide endpoint serviceable<br/>online phase ready]
    Check -->|No| Pin[Pin measured ksvc to<br/>representative hostname<br/>measurement only]
    Pin --> Prep[ResetCpuToFloor<br/>+ SetMaxScale=1]
    Prep --> ColdBS[Binary-search cold phase<br/>using /status gate]
    ColdBS --> WarmBS[Binary-search warm phase<br/>using workload gate]
    WarmBS --> Derive[Derive c_min<br/>from sample curves<br/>+ acceptableResponseTime]
    Derive --> Persist[WriteStatus<br/>pool profile + resolved pool]
    Persist --> Cleanup[UnsetMaxScale<br/>+ remove hostname pin]
    Cleanup --> Mark[Set AllSaturated = true]
    Mark --> HandOff
```

Only one representative node is measured. That keeps the offline experiment
simple and repeatable. The measured result is interpreted as a profile for
the whole labeled pool, not as a profile for only the representative host —
valid precisely because the pool-homogeneity contract above guarantees every
member is equivalent.

### 7.2 Algorithms (Pseudocode)

**Top-level offline reconcile**

```text
function ReconcileOffline(nimbus):
    pool_selector = nimbus.spec.placement.nodeSelector
    if pool_selector is empty:
        log warning "missing Nimbus node-pool selector"; return

    pool_nodes = listReadySchedulableNodesMatching(pool_selector)
    if pool_nodes is empty:
        log warning "no Ready nodes match Nimbus node-pool selector"; return

    sort pool_nodes by name
    representative = pool_nodes[0]

    if status.profile.is_saturated:
        nimbus.allSaturated = true
        return

    cpu_budget = nimbus.spec.containerPolicies[0].cpuBudget

    PinKsvcToNode(measured_ksvc, representative)  # measurement-only hostname pin
    ResetKsvcCpuToFloor(measured_ksvc)            # clears residual CPU
    SetMaxScale(measured_ksvc, 1)                 # one pod for determinism

    try:
        (c_opt_cold, c_min_cold, samples_cold) =
            runBinarySearch(phaseCold, slo.cold, cpu_budget)
        (c_opt_warm, c_min_warm, samples_warm) =
            runBinarySearch(phaseWarm, slo.warm, cpu_budget)

        status.placement = {
            nodeSelector:          pool_selector,
            resolvedNodes:         pool_nodes,
            representativeNode:    representative,
            representativeReason:  "first_ready_sorted_by_name",
        }
        status.profile = {
            scope:          "node-pool",
            measuredOnNode: representative,
            startingCpu:    c_opt_cold,
            runningCpu:     c_opt_warm,
            cMinStarting:   c_min_cold,       # nullable
            cMinRunning:    c_min_warm,       # nullable
            coldRtSamples:  samples_cold,
            warmRtSamples:  samples_warm,
        }
    finally:
        UnsetMaxScale(measured_ksvc)
        UnpinKsvc(measured_ksvc)

    WriteStatus(nimbus)                           # persist pool profile
    nimbus.allSaturated = true
```

**runBinarySearch — unified bisect over `[0, cpuBudget]` for `c_opt`** (per phase, per node)

The main objective is `c_opt` (the lower edge of the latency plateau).
The algorithm is a **single binary search** between `low = 0` and
`high = cpuBudget`. The invariant maintained throughout the loop:

- `high` = smallest CPU known to be ON the plateau (RT "good")
- `low`  = largest CPU known to be OFF the plateau (RT "bad"), or `0`
  if the plateau hasn't been left yet.

`c_opt` falls out as `high` when the bracket `(high − low)` shrinks below
`CONVERGENCE_THRESHOLD`. `c_min` is then read off the recorded samples
by `DeriveMin` — no extra measurement is taken.

Two safety knobs make the algorithm robust on real clusters:

- **`MIN_PROBE_CPU_MILLI` (= 50)** — never probe below this. Some
  workloads crash-loop at very small CPU values; stopping early protects
  the search from those failure modes. If the next `mid` would be below
  this floor, we exit the loop and return the current `high` as `c_opt`.
- **Feasibility check at `cpuBudget`** — informational only. If RT at the
  ceiling already exceeds the SLO, we log a warning but **don't abort
  the search**: `c_opt` is still a meaningful number (the latency plateau
  edge); `c_min` will be reported as `""` by `DeriveMin` since no sample
  passes SLO.

```text
function runBinarySearch(phase, slo_rt, cpu_budget):
    samples = []                                # accumulated (cpu, rt) rows
    cache   = {}                                # probeOnce cache, keyed by cpu

    # ─── Feasibility check at the ceiling (informational) ───────────────
    stats_budget = ProbeOnce(phase, cpu_budget) # adds to samples+cache
    if gate(stats_budget) > slo_rt:
        log warning "SLO unachievable at cpuBudget = " + cpu_budget
        # Continue — c_opt search still produces a useful number;
        # DeriveMin will return "" since no sample meets the SLO.

    # ─── Single bisect loop over [0, cpuBudget] ─────────────────────────
    low  = 0
    high = cpu_budget

    while (high - low) > CONVERGENCE_THRESHOLD:
        mid = (high + low) / 2
        if mid < MIN_PROBE_CPU_MILLI:
            # Safety floor: refuse to probe at potentially-crash CPU.
            # current high is our best-known plateau edge.
            break

        stats_mid  = ProbeOnce(phase, mid)
        stats_high = ProbeOnce(phase, high)      # cache hit unless high just changed
        rt_mid     = gate(stats_mid)
        rt_high    = gate(stats_high)

        improvement = (rt_mid - rt_high) / rt_mid
        if improvement >= IMPROVEMENT_GATE:
            # mid is meaningfully WORSE than high → mid is OFF plateau.
            # c_opt is in (mid, high]. Move low up.
            low = mid
        else:
            # mid is ~equal to high → mid is ON plateau.
            # c_opt ≤ mid. Narrow high down to mid.
            high = mid

    c_opt = high

    # ─── Derive c_min from the recorded samples (no extra probes) ───────
    c_min = DeriveMin(samples, slo_rt)

    return (c_opt, c_min, samples)


function ProbeOnce(phase, cpu):
    # Per-phase memoization. Appends to samples on cache miss; no-op on hit.
    if cpu in cache:
        log "[search] cache hit cpu=" + cpu + " — skipping re-probe"
        return cache[cpu]
    stats = Measure(phase, cpu)                  # see below
    cache[cpu] = stats
    samples.append((cpu, stats))                 # one row per UNIQUE probed cpu
    return stats
```

Worked example (one phase, one node):

```
cpuBudget = 1000m, true plateau edge ≈ 500m, SLO = 400ms.

Feasibility — ProbeOnce(1000m), rt = 100ms ≤ 400ms ✓.

Iter | low  | high | mid  | rt(mid) | rt(high) | improvement | decision
-----+------+------+------+---------+----------+-------------+------------------
  1  |   0  | 1000 |  500 |   110   |    100   |    9.1%     | high = 500  (on plateau)
  2  |   0  |  500 |  250 |   200   |    110   |   45.0%     | low  = 250  (off plateau)
  3  | 250  |  500 |  375 |   130   |    110   |   15.4%     | low  = 375  (off plateau)
  4  | 375  |  500 |  437 |   115   |    110   |    4.3%     | high = 437  (on plateau)
  5  | 375  |  437 |  (gap = 62m < 100m → STOP)             | c_opt = 437m

5 unique probes: {1000m, 500m, 250m, 375m, 437m}.

c_min = DeriveMin(samples, slo=400ms)
      = smallest cpu in samples where rt ≤ 400ms
      = 250m  (rt=200 ≤ 400; smaller probes were not taken)
```

(`c_min` here is below `c_opt` — typical when the SLO is much looser than
the plateau RT. If the SLO were tight at 120ms, `c_min` would be 437m,
coincident with `c_opt`.)

**Note on the stop conditions.** Three rules govern the loop exit:

- **Gap threshold** (`CONVERGENCE_THRESHOLD`, e.g. `100m`) — primary exit.
  When `(high − low) ≤ 100m`, the resolution is fine enough; report
  `c_opt = high`.
- **Safety floor** (`MIN_PROBE_CPU_MILLI`, e.g. `50m`) — early exit. If
  the next `mid` would fall below this, stop without probing; report
  `c_opt = high`. Protects against pod-crashes at tiny CPU.
- **Improvement gate** (`IMPROVEMENT_GATE`, e.g. `10%`) — not an exit
  condition, but the *direction* signal: whether to move `low` up or
  `high` down on each iteration.

`CONVERGENCE_THRESHOLD_MILLI` and `IMPROVEMENT_GATE` are already defined
in [`api/algorithm/binary_search.go`](api/algorithm/binary_search.go).
`MIN_PROBE_CPU_MILLI` is new and lives next to them.

**DeriveMin — smallest sample meeting the SLO**

```text
function DeriveMin(samples, slo_rt):
    # samples: list of (cpu, rt) collected during runBinarySearch.
    # c_min is the smallest CPU whose measured RT meets the SLO
    # (rt ≤ slo_rt). The check is non-strict: any sample at or under
    # the SLO budget qualifies — exact equality is not required.
    sorted = sort samples by cpu ascending
    for (cpu, rt) in sorted:
        if rt ≤ slo_rt:
            return cpu                              # nearest passing point
    return null                                     # no sample met the SLO
```

**One measurement at a CPU point**

```text
function Measure(phase, cpu):
    UpsertStartupCPUBoost(ksvc, cpu)             # boost CR: requests = limits = cpu
    samples = []
    N = phase == COLD ? spec.measurement.coldSamples : spec.measurement.warmSamples

    for i in 1..N:
        if phase == COLD:
            ForcePodRecycle(ksvc)                # fresh pod required per cold
            WaitForScaleToZero(ksvc)
            CoolDown(10s)                        # endpoint propagation
        rt_i = TriggerHttp(phase.gate)           # GET + retry until gate passes
        samples.append(rt_i)

    DeleteStartupCPUBoost(ksvc)
    return Percentile(samples, spec.metric)      # p95 / p90 / avg
```

> Note: `c_min` is derived from the recorded samples in a single pass via
> `DeriveMin` (above) — no additional probing is required. `runBinarySearch`'s
> main loop targets `c_opt`; `c_min` falls out of the sample curve for free.
> For preload/import, thesis scope treats exported `c_min` as part of the
> measured profile snapshot. Do not change `acceptableResponseTime` or
> `spec.metric` between export and preload unless you re-run offline profiling.

### 7.3 Output — `status.perNode`

```yaml
status:
  perNode:
    worker-1:
      startingCpu:  "1500m"      # c_opt for cold phase
      runningCpu:   "500m"       # c_opt for warm phase
      cMinStarting: "900m"       # smallest cold-sample CPU meeting SLO
      cMinRunning:  "350m"       # smallest warm-sample CPU meeting SLO
      startingRt: { avgMillis: 1100, p90Millis: 1180, p95Millis: 1240 }
      runningRt:  { avgMillis:   38, p90Millis:   42, p95Millis:   45 }
      coldRtSamples:
        - { cpu: "300m",  rtMillis: 4200, rtP90Millis: 4400, rtP95Millis: 4500 }
        - { cpu: "650m",  rtMillis: 2100, rtP90Millis: 2240, rtP95Millis: 2300 }
        - { cpu: "1150m", rtMillis: 1310, rtP90Millis: 1380, rtP95Millis: 1410 }
        - { cpu: "1500m", rtMillis: 1100, rtP90Millis: 1180, rtP95Millis: 1240 }
      warmRtSamples:
        - { cpu: "300m",  rtMillis:  180, rtP90Millis:  210, rtP95Millis:  220 }
        - { cpu: "500m",  rtMillis:   45, rtP90Millis:   48, rtP95Millis:   52 }
```

Sample arrays are the full per-probe-point curves the binary search visited,
sorted ascending by CPU. They make online's `c_min` derivation possible and
support thesis-chapter analysis (plotting latency-vs-CPU curves).

Alongside `perNode`, the offline apply step writes `status.applied` — one row
per managed ksvc recording the `nodeSelector` + CPU NIMBUS wrote and any
`applyError`. It is the source of truth for the invariant "every managed ksvc's
nodeSelector equals `spec.placement.nodeSelector` after offline":

```yaml
status:
  applied:
    measure-yolo-001:
      nodeSelector: { nimbus.io/pool: serverless }
      startingCpu:  "1500m"
      runningCpu:   "500m"
```

---

## 8. Online Phase — Plan

The online phase is built around a single synchronous coordination point:
the Knative autoscaler (KPA) calls NIMBUS at every `0 → 1` cold-start,
NIMBUS computes a placement decision based on live cluster state and a
**burst-detection** signal, patches the ksvc spec and the per-ksvc
`StartupCPUBoost` CR, and returns to KPA — which then proceeds with the
actual scale-up. The pod that kube-scheduler binds therefore lands on the
node NIMBUS just chose, at the CPU NIMBUS just programmed.

The integration requires a small Knative modification: KPA's
`kpa/scaler.go` reconciler must perform a synchronous outbound HTTP call
to NIMBUS before patching `Deployment.spec.replicas`. The change is
contained — one new call site, one timeout, one fail-open path — but it
is not present in upstream Knative.

### 8.1 Architecture Overview

The online phase introduces two long-running NIMBUS components and one
modified Knative component:

NIMBUS's two sub-components communicate only through the shared
`BurstState` value: the detector writes it on every observed cold-start;
the placement decision reads it once per `/decide` call. The decision
itself is fully synchronous from KPA's perspective — KPA blocks on the
HTTP call, then continues with the scale-up using the values NIMBUS
returned.

The user-facing request flow becomes:

```text
user → gateway → activator (no pod) → KPA → NIMBUS (patches spec + boost)
                                              ↓
                                           return 200 ok
                                              ↓
                                   KPA → kube-scheduler → pod
```

NIMBUS sits **between KPA and the actual scale-up**, not in the request
path itself.

### 8.2 Burst Detector

The burst detector is a background goroutine that observes `0 → 1`
cold-start events (one per `/decide` invocation) and continuously updates
a shared `BurstState`. Under quiet load NIMBUS uses every available
node CPU freely; under a wave of incoming cold-starts NIMBUS proactively
**reserves** a fraction of node headroom so the cold-starts arriving in
the next few seconds will still find capacity.

The detector tracks **two signals**, both EWMA-smoothed:

- **Velocity** — events per second.
- **Acceleration** — rate-of-change of velocity.

A high velocity is the obvious "we are in a burst" signal. The
acceleration signal lets the detector flip to BURST mode **before** the
velocity threshold is reached — useful during the rising edge of a wave,
when reserving early prevents the first few pods from consuming all
headroom and starving the rest.

#### State and parameters

```text
type BurstState:
    mode         : NORMAL | BURST
    rate         : float    # smoothed events/sec (velocity)
    Δrate        : float    # smoothed acceleration
    reserveRatio : float    # 0.0 in NORMAL, 0.30 in BURST
```

| Symbol | Default | Meaning |
|---|---|---|
| `α` | 0.30 | EWMA smoothing for `rate` |
| `β` | 0.20 | EWMA smoothing for `Δrate` |
| `θ_rate` | 1.0 | velocity threshold (events/sec) for BURST |
| `θ_Δrate` | 0.15 | acceleration threshold for BURST early-flip |
| `θ_normal` | 1.0 | rate must drop below this to count as "quiet" |
| `quiet_secs_threshold` | 30 | seconds of quiet before flipping back to NORMAL |
| `decay_α` | 0.30 | per-tick rate/Δrate decay when idle |
| `tick_secs` | 5 | decay-loop tick period |

All defaults are overridable per Nimbus CR under `spec.burstDetector.{alpha,
beta, thresholdRate, thresholdAccel, quietSecs, decayAlpha, tickSecs}`.

#### Pseudocode

```text
INIT:
    rate          ← 0
    Δrate         ← 0
    mode          ← NORMAL
    reserveRatio  ← 0.0
    last_event_t  ← nil
    quiet_secs    ← 0

# (a) Event-driven update — fires on every observed 0 → 1 cold-start
ON cold_start_event(now):
    if last_event_t ≠ nil:
        Δt        ← now − last_event_t
        raw_rate  ← 1 / Δt

        new_rate  ← α · raw_rate + (1 − α) · rate
        Δrate     ← β · (new_rate − rate) + (1 − β) · Δrate
        rate      ← new_rate

        # OR-logic — either signal flips the mode
        if rate > θ_rate ∨ Δrate > θ_Δrate:
            mode          ← BURST
            reserveRatio  ← 0.30
            quiet_secs    ← 0

    last_event_t ← now

# (b) Decay loop — runs on its own ticker; lets the detector return to NORMAL
EVERY tick_secs:
    rate   ← (1 − decay_α) · rate
    Δrate  ← (1 − decay_α) · Δrate

    if rate < θ_normal:
        quiet_secs ← quiet_secs + tick_secs
    else:
        quiet_secs ← 0

    if quiet_secs ≥ quiet_secs_threshold:
        mode          ← NORMAL
        reserveRatio  ← 0.0
```

#### The two ways the detector flips to BURST

```text
Case A — sustained burst (rate crosses θ_rate)

  events/s
     ▲
  2  │              ●
  1  │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ θ_rate ─ ─ ─ ─
     │       ●
  0  │  ●
     └─────────────────────────────────────► t
              ↑ BURST flips here (rate > θ_rate)


Case B — early flip via acceleration (rate climbing fast)

  events/s
     ▲                          ●
  1  │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  θ_rate
     │                  ●
     │            ●
     │      ●
  0  │ ●
     └─────────────────────────────────────► t
                ↑ BURST flips here — rate < θ_rate
                  but Δrate > θ_Δrate
```

The acceleration signal lets the system commit to reservation **before** the
rate has fully climbed to `θ_rate`. This matters at the leading edge of a
burst: reserving from the first few waves prevents over-commit before the
sustained-rate trigger would have fired.

### 8.3 Placement Decision

The placement decision is exposed as a synchronous HTTP endpoint
(`POST /decide`) on the NIMBUS controller. KPA's modified `kpa/scaler.go`
calls it on every `0 → 1` scale-up, blocks on the response (within a
bounded timeout), and proceeds to patch `Deployment.spec.replicas` using
the returned `(nodeSelector, cpu, boostCpu)` triple.

The decision is **burst-aware**: the detector's `reserveRatio` shrinks each
node's *usable* free CPU for the top two tiers (so a wave doesn't grab every
last slice at `c_opt`). It does **not** skip the `c_opt` branch — there is no
"force c_min in BURST" rule; the conservative behaviour falls out naturally
when reduced `usable_n` prevents `c_opt` from fitting.

#### Inputs and outputs

The offline phase produces **two cold values and two warm values per
representative node**; the warm side is locked to `c_opt_warm` across every
online tier (thesis scope), so the waterfall picks only among the COLD values:

```text
Inputs:
  ksvc            : namespace/name of the Knative service being scaled up
  pool_label      : the Nimbus's spec.placement.nodeSelector
  BurstState      : read once, O(1), under RLock (mode, reserveRatio)
  free_n          : live per-node free CPU =
                    budget_n − Σ serverless-Knative committed_requests on n
                    where budget_n = (NIMBUS_BUDGET_PCT/100) × allocatable_n
  usable_n        : free_n × (1 − reserveRatio)         (Tiers 1 & 2 only)

  Per-pool values (read from the offline profile — representative node):
    c_opt_cold    : .status.perNode[rep].startingCpu      (Tier 1 boost CPU)
    c_opt_warm    : .status.perNode[rep].runningCpu       (locked warm CPU)
    c_min_cold    : .status.perNode[rep].cMinStarting     (Tier 2 boost CPU)
    c_min_warm    :                                       (UNUSED — warm locked)

Outputs (success):
  tier         : c_opt | c_min | best_fit
  nodeSelector : pool selector verbatim (Tiers 1 & 2)
                  OR pool selector ∪ {kubernetes.io/hostname: <pinned>}  (Tier 3)
  cpu          : c_opt_warm                             (locked across all tiers;
                                                         written to ksvc spec
                                                         requests=limits=cpu)
  boostCpu     : c_opt_cold (T1) | c_min_cold (T2) | <pinned>.free_n (T3)
                                                        (written to the
                                                         StartupCPUBoost CR)
  degraded     : false for T1/T2; true for T3 (cold below c_min_cold).

Outputs (failure):
  Pending      : no node has free_n ≥ c_opt_warm — the steady-state request
                 wouldn't fit anywhere. KPA aborts the scale-up.
```

**Headroom check uses the COLD value** (boost peak) for Tiers 1 and 2 — that
is the peak CPU the pod occupies, because the kube-startup-cpu-boost webhook
mutates `requests=limits=boostCpu` at admission. Tier 3's floor is `c_opt_warm`
(the post-revert steady-state request must still fit on the pinned host).

**Graceful degradation, not strict refusal.** When neither `c_opt` nor `c_min`
fits pool-wide, Tier 3 admits with `degraded=true` at whatever the best-fit
node can offer (always ≥ `c_opt_warm`). Pending occurs only when even
`c_opt_warm` doesn't fit any node — i.e. the cluster is genuinely saturated.

#### Pseudocode

```text
on POST /decide (ksvc, pool_label):

    # ─── Step 1 — Snapshot burst state (cluster-wide, O(1)) ───────────
    mode, reserveRatio := burstDetector.Read()

    # ─── Step 2 — Snapshot live cluster headroom (budget-capped) ──────
    nodes := nodes_matching(pool_label)            # Ready + schedulable
    for n in nodes:
        budget_n := (NIMBUS_BUDGET_PCT / 100) × n.allocatable.cpu
        used_n   := Σ requests of serverless-Knative pods bound to n
        free_n   := max(0, budget_n − used_n)
        usable_n := free_n × (1 − reserveRatio)    # NORMAL: =free_n

    # ─── Step 3 — Read tier values (warm locked to c_opt_warm) ────────
    c_opt_cold := status.perNode[rep].startingCpu
    c_opt_warm := status.perNode[rep].runningCpu      # ALSO the locked warm CPU
    c_min_cold := status.perNode[rep].cMinStarting    # may be ""

    # ─── Step 4 — 3-tier waterfall ────────────────────────────────────
    pool_max_usable := max(usable_n over nodes)

    # Tier 1 — c_opt pool-wide
    if pool_max_usable ≥ c_opt_cold:
        return (
            tier         : c_opt,
            nodeSelector : pool selector,
            cpu          : c_opt_warm,                # locked
            boostCpu     : c_opt_cold,
            degraded     : false,
        )

    # Tier 2 — c_min pool-wide. Guard against the rare case c_min_cold <
    # c_opt_warm (post-revert request must still fit any pool node).
    if c_min_cold is defined and pool_max_usable ≥ max(c_min_cold, c_opt_warm):
        return (
            tier         : c_min,
            nodeSelector : pool selector,
            cpu          : c_opt_warm,                # locked
            boostCpu     : c_min_cold,
            degraded     : false,
        )

    # Tier 3 — best-fit pinned. Floor = c_opt_warm so the post-revert
    # steady-state still fits the chosen host. Cold = the chosen node's
    # full raw free_n. Reserve does NOT apply (already constrained).
    best := argmax { n.free over nodes  :  n.free ≥ c_opt_warm }
    if best ≠ ∅:
        return (
            tier         : best_fit,
            nodeSelector : pool selector ∪ {kubernetes.io/hostname: best.name},
            cpu          : c_opt_warm,                # locked
            boostCpu     : format(best.free),         # node's full free room
            degraded     : true,
        )

    # Pending — no node meets c_opt_warm. Cluster genuinely saturated.
    return Pending
```

The patches NIMBUS issues before returning to KPA:

```text
1. ksvc.spec.template.spec.nodeSelector
     pool-wide  → carries the operator's pool label, no hostname key
     pinned     → adds kubernetes.io/hostname: <n*>

2. ksvc.spec.template.spec.containers[0].resources
     requests.cpu = limits.cpu = cpu                (warm value; Guaranteed QoS)

3. StartupCPUBoost CR for this ksvc
     fixedResources.{requests,limits} = boostCpu    (cold value; Guaranteed QoS)
```

At pod admission the boost webhook raises the pod's resources from `cpu`
to `boostCpu`. Once the boost controller observes `/status` returning the
expected body, it reverts the pod's resources back to `cpu`. The pod
therefore runs at the **cold** operating point during initialization and
at the **warm** operating point in steady state.

KPA then proceeds with the scale-up; kube-scheduler reads the just-patched
spec and binds the new pod to the chosen node.

#### Effect of mode on the same headroom snapshot

Three workers each with `free = 800m`. Offline measured for the pool:
- `c_opt_cold = 700m`, `c_opt_warm = 400m`
- `c_min_cold = 500m`

The headroom check uses the **cold** value for Tiers 1/2 (the boost peak) and
`c_opt_warm` for Tier 3 (the post-revert floor). The `cpu` (steady-state ksvc
spec) is **always `c_opt_warm = 400m`** — only `boostCpu` varies.

**NORMAL mode** (`reserveRatio = 0`):
- `usable = 800m` everywhere.
- `800m ≥ c_opt_cold 700m` ⇒ Tier 1.
- Return `(tier=c_opt, nodeSelector=pool-wide, cpu=400m, boostCpu=700m, degraded=false)`.

**BURST mode** (`reserveRatio = 0.30`):
- `usable = 800m × 0.70 = 560m` everywhere.
- `560m ≥ c_opt_cold 700m` ? **No** — Tier 1 fails.
- `560m ≥ max(c_min_cold 500m, c_opt_warm 400m) = 500m` ? **Yes** ⇒ Tier 2.
- Return `(tier=c_min, nodeSelector=pool-wide, cpu=400m, boostCpu=500m, degraded=false)`.

Same cluster state, different boost CPUs — entirely from the shared
`BurstState`. The 240m of headroom NORMAL would have spent on the boost
window stays reserved for the next waves. The steady-state `cpu = 400m` is
unchanged (thesis: cold-only optimization).

**Tier 3 (best_fit pinned) — when no node fits c_min cold-side:**
Same workers but now `worker-1.free = 450m`, others = 200m (other workloads
are using the budget). NORMAL mode:
- Tier 1: 450m ≥ 700m? No.
- Tier 2: 450m ≥ 500m? No.
- Tier 3: ∃ node with `free ≥ c_opt_warm 400m`? Yes → `worker-1` (450m).
- Return `(tier=best_fit, nodeSelector=pool ∪ {hostname:worker-1}, cpu=400m, boostCpu=450m, degraded=true)`.

The pod admits at the best CPU `worker-1` can offer; its steady-state stays
at `c_opt_warm`; the boost is below `c_min_cold` so the experiment row carries
`degraded=true` for SLO attribution.

### 8.4 Output — `status.online`

Each reconcile writes the per-ksvc assignment set to `status.online.assignments`
and the current cluster-wide `burstMode`. The schema below reflects the
**implemented Go shape** (`OnlineStatus` / `OnlineAssignment`); a richer
schema with `decidedAt`, `burstRate`, etc., is a still-open extension.

```yaml
status:
  online:
    burstMode: NORMAL                # current detector mode (cluster-wide)
    activeAssignments: 3
    assignments:
      - ksvc: measure-yolo-001
        node: serverless             # pool label value (Tier 1/2 pool-wide)
        tier: c_opt
        startingCpu: 937m            # cold/boost CPU (varies per tier)
        runningCpu: 437m             # warm CPU (LOCKED to c_opt_warm)
        degraded: false
        ready: true
        url: http://measure-yolo-001.serverless.svc.cluster.local/detect/local
      - ksvc: measure-yolo-002
        node: serverless
        tier: c_min                  # NORMAL c_opt didn't fit, fell to c_min
        startingCpu: 750m
        runningCpu: 437m             # still c_opt_warm
        degraded: false
        ready: true
      - ksvc: measure-yolo-003
        node: worker-1               # Tier 3 pinned to this hostname
        tier: best_fit
        startingCpu: 500m            # = worker-1's full free_n
        runningCpu: 437m             # still c_opt_warm
        degraded: true               # cold < c_min_cold → degraded admit
        ready: true
```

A Pending outcome carries `degraded: true` with `tier="", node="",
startingCpu="", runningCpu=""` — no admission, no patch.

The combination of `(ksvc, tier, startingCpu, degraded, burstMode)` is the
join key the experiment harness uses to attribute observed request latencies
back to the placement context. `runningCpu` is constant across rows by design.

---


## 9. Glossary

| Term | Definition |
|---|---|
| **Ksvc** | Knative Service custom resource; the user-facing serverless workload abstraction. |
| **Candidate node** | A Ready, schedulable node matching the ksvc's `nodeSelector` / `nodeAffinity` / tolerations vs taints. |
| **Cold phase** | Measurement regime that times pod startup + initialization (e.g. model load). Gate: HTTP GET + body substring on `/status`. |
| **Warm phase** | Measurement regime that times steady-state inference requests against an already-initialized pod. Gate: HTTP GET + status code (+ optional body substring) on `/detect/local`. |
| **`c_opt`** | Saturated CPU operating point per node per phase; the "knee" where more CPU stops meaningfully helping. |
| **`c_min`** | Smallest CPU operating point per node per phase that still meets the user's `acceptableResponseTime`. |
| **`startingCpu`** | The cold-phase `c_opt`, written into the `StartupCPUBoost` CR. |
| **`runningCpu`** | The warm-phase `c_opt`, written into the ksvc spec at apply time. |
| **Saturated (offline)** | A node whose `.status.perNode` entry has both `startingCpu` and `runningCpu` populated; measurement is complete. |
| **`AllSaturated`** | Boolean flag on the Nimbus event: true iff every candidate node is saturated. Controls the offline → online handoff in the worker loop. |
| **`/decide`** | The synchronous HTTP endpoint NIMBUS exposes for KPA's pre-scale hook. KPA invokes it on every `0 → 1` cold-start; NIMBUS returns `(nodeSelector, cpu, boostCpu)` or `Pending`. |
| **Pool-wide assignment** | A `/decide` outcome that returns the user's pool label as the `nodeSelector` with no `kubernetes.io/hostname` constraint, leaving kube-scheduler free to bin-pack. |
| **Pinned assignment** | A `/decide` outcome that narrows `nodeSelector` to a specific `kubernetes.io/hostname` chosen by NIMBUS. Used when pool-wide `c_opt` doesn't fit (e.g. in BURST mode). |
| **Pending (online)** | A `/decide` outcome where neither `c_opt` nor `c_min` fits any node after applying the burst reserve. NIMBUS refuses to admit at a sub-`c_min` CPU; KPA aborts the scale-up. Recorded as `state: Pending` on `status.online.assignments[*]`. |
| **BurstState** | The atomic shared value the burst detector writes and the placement decision reads. Carries `mode` (NORMAL / BURST), `rate` (EWMA velocity), `Δrate` (EWMA acceleration), and `reserveRatio` (0 in NORMAL, e.g. 0.30 in BURST). |
| **NORMAL mode** | Burst-detector state under quiet load. `reserveRatio = 0`; placement decision uses every node's full free CPU. |
| **BURST mode** | Burst-detector state under wave load (`rate > θ_rate` OR `Δrate > θ_Δrate`). `reserveRatio > 0`; placement decision treats only `(1 − reserveRatio)` of each node's free CPU as usable. |
| **`reserveRatio`** | Fraction of node free CPU NIMBUS holds back in BURST mode for imminent cold-starts. Default 0.30. |
| **SLO** | Service-level objective. In NIMBUS: `spec.acceptableResponseTime.{cold, warm}` — the operator's promise to end users. |
| **Boost CR** | `StartupCPUBoost` CR from `kube-startup-cpu-boost`. NIMBUS upserts one per ksvc per `/decide` decision; the webhook raises pod CPU to `boostCpu` at admission. |
| **Headroom** | `node.allocatable.cpu − Σ committed requests on the node`. Read live from kube-apiserver on every `/decide` call. |
