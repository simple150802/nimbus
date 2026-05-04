# Multi-Node Profiling — Open Issue & Implementation Plan

> Status: **identified weakness, designed, not implemented.** Treat this document as the reference for future work. The offline stage (§1 of [`algorithm.md`](algorithm.md)) is correct in its own terms but rests on a single-node assumption that doesn't hold on a real cluster.

## 1. The problem in one sentence

NIMBUS profiles a ksvc on **whichever node the test pod happens to land on**, then applies the discovered CPU limit **cluster-wide** — so the limit is wrong for every node whose CPU performance differs from the one that was sampled.

## 2. Verification — what the code actually does

A grep across the entire `nimbus/` tree, scoped to `*.go` and `*.yaml`, confirms the controller has **zero node-aware logic**:

| Concern | Where it would appear | Found in code? |
|---|---|---|
| Pinning probes to a specific node | `nodeSelector`, `nodeAffinity` in the patch built before a probe | **No.** The string appears only in [`config/sampleapp.yaml:14-15`](config/sampleapp.yaml#L14-L15), which is the *test app's* manifest — not anything the controller writes. |
| Reading which node a probe ran on | `pod.Spec.NodeName` after `triggerHttp` | **No.** [`MonitorKsvcResources`](api/kubeapi/knative.go) reads `pod.Spec.Containers` only; node identity is discarded. |
| Per-node breakdown of results | A `map[string]…` or slice on `NimbusStatus` | **No.** [`NimbusStatus`](api/nimbusevent/event_type.go) holds two flat strings, `startingCpu` + `runningCpu`. |
| Per-node patching at apply time | Different limit per node when writing the ksvc | **No.** [`PatchResourceLimits`](api/kubeapi/knative.go) writes one value to `spec.template.spec.containers[0].resources.limits.cpu`, applied uniformly to every future pod regardless of where it schedules. |

The closest the controller comes to node awareness is `PatchMaxScale=1` during a search, which limits how many pods Knative spins up — but **does not** constrain *which* node those pods can run on. The kube-scheduler picks freely.

## 3. Why this is a real problem

A two-node cluster with the following heterogeneity (your actual setup, from `kubectl get nodes`):

| Node    | Kernel    | Likely impact |
|---------|-----------|---------------|
| master  | 6.17      | Different scheduler / cgroup-v2 behavior; control-plane noise |
| worker  | 6.14      | Different microarchitecture exposure; less control-plane interference |

Same `200m` CPU quota cgroup-throttles to *different effective CPU* on the two kernels because the bandwidth controller's behavior under bursty load has changed between kernel versions. Add real hardware differences (CPU model, frequency cap, NUMA, hyperthreading) and the same `cpu: 700m` request can deliver 200ms response time on one node and 800ms on another.

### What this breaks

1. **The binary search converges on whichever node the pod lands on.** The controller can't distinguish "200m is enough on the fast node" from "200m is enough on the slow node" — those answers are different.
2. **Within a single search, individual probes might land on different nodes.** If sample 1 lands on the fast node (rt=120ms) and sample 2 lands on the slow node (rt=480ms), the response-time deltas the convergence loop uses (`(rtLow-rtMid)/rtLow > 0.10`) are noise, not signal.
3. **The applied limit is a one-size-fits-all value.** Even if the search were noise-free, writing one number into the ksvc spec means every future pod gets the same limit regardless of which node it runs on — guaranteed wrong on at least one node.
4. **The online stage (§2 of [`algorithm.md`](algorithm.md)) inherits this flaw.** All three online algorithms (A/B/C) consume `M_cold` and `M_warm` per ksvc. If those numbers are sampled from one node and the online stage allocates on a different node, the bargaining math is meaningless on that node.

### Why your tests didn't expose this

`config/sampleapp.yaml` pins the test app to `worker` via `nodeSelector: kubernetes.io/hostname: "worker"`. Every probe so far has measured worker. You have a single-node *experimental setup* on top of a multi-node cluster — the bug is invisible until you remove the `nodeSelector`.

## 4. Solution options

Five candidates, ordered roughly cheapest → most thorough.

### Option 1 — Track-only (no behavior change)

Capture the node identity on every probe, persist it in `.status.measuredOn`. Don't change scheduling or apply logic.

**Pros**
- Trivial: ~20 LOC. Read `pod.Spec.NodeName` after the trigger; add one string field to `NimbusStatus`.
- Surfaces the bug to the operator: `kubectl get nimbus boost-001 -o jsonpath='{.status.measuredOn}'` shows which node the value came from.

**Cons**
- Doesn't fix anything. A wrong number is still being applied; the operator is just told which node it's wrong for.

**Use as**: stepping stone to any of the other options. Cheap enough to ship first regardless.

### Option 2 — Worst-node calibration

Add a one-shot benchmark phase before the binary search: spawn a tiny pod on every node, run a fixed CPU-bound microbenchmark (e.g., compute a SHA-256 of a 1 MB buffer for 1 second, measure ops/sec). Identify the slowest node. Pin the binary search to that node via `nodeSelector`. Apply the result cluster-wide.

**Pros**
- Single binary-search run, no per-node profiling explosion.
- Result is a *safe* upper bound: every other node has more headroom than measured. SLO can't be silently violated by a slow node — only over-provisioning happens on fast nodes.
- The microbenchmark is cheap (≤ 1 minute total, parallelized across nodes).

**Cons**
- Wastes capacity on faster nodes (they could run with less CPU).
- Microbenchmark may not generalize — a SHA-256 throughput number doesn't predict a YOLO model's cold-start time. Different workloads stress different parts of the CPU (cache, memory bandwidth, SIMD lanes).
- Still one number applied cluster-wide.

**Use as**: middle-ground if you want correctness without per-pod scheduling complexity.

### Option 3 — Per-node profiling, single applied value

Run the binary search **N times** (once per node), each time with `nodeSelector` pinning the test pod. Store all N results in `.status.perNode`. At apply time, pick the worst (or median, or configurable) and write it to the ksvc spec as a single number.

**Pros**
- True per-node measurements; no microbenchmark ambiguity.
- Result populates `.status.perNode` which the online stage can then consume directly.
- Apply path stays simple (one number on the ksvc spec).

**Cons**
- N× the search wall time. With `coldSamples=3` and 5 probes per phase, today's search takes ~15 minutes; on N=2 nodes that's 30 minutes; N=5 nodes is 75 minutes.
- Still applies one number cluster-wide.

**Use as**: the right answer if you want correctness *and* the online-stage prerequisite, and can absorb the longer search time.

### Option 4 — Per-node profiling, per-node apply via mutating webhook

Same N-times search as Option 3, **plus** a mutating admission webhook that rewrites `pod.spec.containers[*].resources.limits.cpu` based on the destination node when the kube-scheduler emits a `Pending` pod for a Nimbus-managed ksvc.

**Pros**
- Optimal: every pod runs at the right CPU for its actual node.
- The webhook is already required for the online stage P1 (§2.2.4 of [`algorithm.md`](algorithm.md)) — building it once serves both features.
- Fastest pods on fast nodes; safest pods on slow nodes; no waste.

**Cons**
- Webhook is the highest-risk piece of code in the project. A buggy mutating webhook can break every pod creation cluster-wide.
- N× the search time, same as Option 3.
- The ksvc spec has only one `resources.limits.cpu` field; the webhook overrides it on every pod — operators reading `kubectl get ksvc -o yaml` won't see the actual applied value, only the baseline.

**Use as**: end-state target. Option 3 is a strict subset, so 3 → 4 is incremental.

### Option 5 — Defer to the online stage

Accept that the offline value is a baseline approximation; let the online tuner adjust per-node at runtime based on observed response times.

**Pros**
- No offline-stage changes at all.
- Clean separation: offline is "best guess from one node"; online is "correct it everywhere".

**Cons**
- The online stage (§2 of `algorithm.md`) has zero implementation today. This option means living with the wrong value indefinitely until that lands.
- The online stage's three candidate algorithms (A/B/C) all consume `M_cold` and `M_warm` as inputs. If those are wrong by 3× because they were sampled from a different node, the bargain math is broken on that node before adjustment can kick in.
- Couples this fix's timeline to the online stage's timeline.

**Use as**: only if Options 1–4 are out of scope and you commit to building the online stage with node-aware adjustment as a first-class feature.

## 5. Recommendation

**Sequence: 1 → 3 → 4.**

- **Now (Option 1):** add `status.measuredOn` and read `pod.Spec.NodeName`. ~1 evening's work. Makes the bug visible; doesn't paper over it.
- **Next (Option 3):** run a per-node search loop, persist per-node profiles. The N× search runtime is acceptable for thesis-stage local testing. This is the data the online stage needs anyway (§2.1 of `algorithm.md` calls for `samples_i` per pod — per-node samples are a strict superset).
- **End-state (Option 4):** add the mutating webhook, route per-node values to per-pod resources at scheduling time. This is the same webhook §2.2.4 of `algorithm.md` requires — pay for it once, get both features.

**Skip Option 2** (microbenchmarking). The microbenchmark-vs-real-workload generalization is unreliable enough that it'll bite you in the thesis defense.

**Don't choose Option 5** unless you decide the online stage is the only worthwhile contribution and the offline stage's accuracy doesn't matter.

## 6. Implementation sketch (for Options 1 → 3 → 4)

### Phase 1 — Track-only (Option 1)

Files to change:

1. [`api/nimbusevent/event_type.go`](api/nimbusevent/event_type.go)
   - Add to `NimbusStatus`:
     ```go
     // MeasuredOn records the node name where the binary search ran.
     // Single-node only — populated post-§1 by Phase 1 of multi-node work.
     MeasuredOn string `json:"measuredOn,omitempty"`
     ```

2. [`config/crd.yaml`](config/crd.yaml)
   - Mirror the field into the CRD `status.properties` block.

3. [`api/algorithm/probe_cold.go`](api/algorithm/probe_cold.go) (and `probe_warm.go`)
   - After a successful `triggerHttp`, fetch the pod and capture `pod.Spec.NodeName`. Pass it back via `NimbusEvent.Status.MeasuredOn` (or a new transient field on `NimbusEvent`).
   - Sanity-check: warn if the same Nimbus's probes report different nodes during a single search — that means scheduling drifted mid-search and the result is poisoned.

4. [`api/kubeapi/nimbus_status.go`](api/kubeapi/nimbus_status.go)
   - Include `measuredOn` in the status patch.

Total: ~50 LOC + CRD schema bump.

### Phase 2 — Per-node profiling (Option 3)

Files to change (additive on top of Phase 1):

1. [`api/algorithm/binary_search.go`](api/algorithm/binary_search.go)
   - Wrap `BinarySearch` in a per-node loop:
     ```go
     for _, node := range listSchedulableNodes(ctx) {
         pinKsvcToNode(ctx, ns, ksvc, node)            // patch nodeSelector
         result, err := searchOneNode(ctx, current)    // existing logic
         storePerNodeResult(current, node, result)
         unpinKsvcFromNode(ctx, ns, ksvc)              // restore default scheduling
     }
     ```
   - Each pin/unpin patches `spec.template.spec.nodeSelector` on the ksvc and waits for scale-to-zero (existing `waitForScaleToZero` already does this) before the next probe.

2. [`api/nimbusevent/event_type.go`](api/nimbusevent/event_type.go)
   - Replace `MeasuredOn string` with:
     ```go
     type NodeProfile struct {
         StartingCpu    string        `json:"startingCpu"`
         RunningCpu     string        `json:"runningCpu"`
         ColdRtSamples  []SamplePoint `json:"coldRtSamples,omitempty"`  // §1.4
         WarmRtSamples  []SamplePoint `json:"warmRtSamples,omitempty"`  // §1.4
     }

     type NimbusStatus struct {
         StartingCpu string                 `json:"startingCpu,omitempty"` // worst across nodes
         RunningCpu  string                 `json:"runningCpu,omitempty"`  // worst across nodes
         PerNode     map[string]NodeProfile `json:"perNode,omitempty"`
     }
     ```

3. CRD schema bump: extend `.status` properties with `perNode`, validated against the same `pattern` regex used for `min` / `max`.

4. Apply policy decision (a new `spec.applyStrategy` field on the Nimbus CR):
   - `worst` (default): pick `max(perNode[*].runningCpu)`. Safe everywhere; over-provisions on faster nodes.
   - `median`: balance.
   - `auto`: fall through to Option 4's webhook (only meaningful once that ships).

Total: ~200 LOC + CRD schema.

### Phase 3 — Per-node apply via mutating webhook (Option 4)

The webhook design lives in `algorithm.md` §2.2.4 since it's also needed for the online stage. Reuse it:

- Inspect incoming pods labeled with a Nimbus-managed ksvc.
- Read `nodeName` (after the scheduler binds — the webhook fires *before* the scheduler decides, so this is harder; you may need to use `nodeAffinity` rules and let the scheduler place, then accept the placement).
- Look up the matching `NodeProfile.RunningCpu` and rewrite `pod.spec.containers[user-container].resources.limits.cpu`.

Total: significant. Webhook scaffolding (admission server, certificate management, RBAC, deployment manifest) is ~500 LOC + cert rotation. The actual mutation logic is small (~50 LOC).

## 7. Compatibility with existing roadmap

This work is **not in conflict with** the in-flight plan documented in [`algorithm.md`](algorithm.md):

| `algorithm.md` item | Multi-node interaction |
|---|---|
| §1.4 — persist RT samples per probe | Phase 2 above stores samples *per node*. Strict superset of §1.4. Implement them together; don't ship §1.4 alone with single-node samples or you'll need to redesign the schema later. |
| §2.1 — online stage inputs (`samples_i`, `M_cold`, `M_warm`) | These become *per-node* values in the new schema (`status.perNode[node].coldRtSamples`, etc.). The online algorithms in §2.2.3 then operate on per-node data, which they need anyway to make sense on heterogeneous clusters. |
| §2.2.4 — mutating admission webhook | Same webhook serves both Phase 3 above and the online stage. Build once. |
| §2.4 — open questions on phase detection, two-writer conflict | All become per-node questions in the new schema. The webhook becomes the single owner of `pod.spec.…limits.cpu` regardless of online vs offline. |

**Ordering implication.** If you're going to do §1.4 anyway (and it's already a hard prerequisite for the online stage), do it as part of Phase 2 of this work. Skipping ahead and shipping §1.4 alone with single-node samples will force a CRD migration later.

## 8. Open questions (decide before implementing)

- **Apply strategy default** — `worst` is safe-by-default but wastes resources. Is over-provisioning acceptable for thesis-stage tests?
- **Node-skew detection during a single search** — what's the right behavior when probes within one search land on different nodes? Abort the search and force a per-node rerun, or warn and trust the result?
- **Excluding control-plane nodes** — `master` is currently `Ready` and schedulable in your cluster, but in production the control plane is often tainted. Should NIMBUS profile only `Ready=true` nodes that don't have `node-role.kubernetes.io/control-plane` taint, or every Ready node?
- **Node lifecycle** — what happens to `status.perNode["worker"]` if `worker` is drained / replaced / renamed? Stale entry, retry, garbage-collect?
- **Custom `applyStrategy=manual`** — let the operator pick which node's profile gets applied cluster-wide (escape hatch for ML / GPU workloads where one node has special hardware).

## 9. Testing the fix

Recipe to demonstrate the bug *today*, before any fix:

```bash
# 1. Remove the worker pin from the test app
kubectl -n serverless patch ksvc measure-yolo --type=json \
  -p='[{"op":"remove","path":"/spec/template/spec/nodeSelector"}]'

# 2. Clear status to force a re-search
kubectl -n serverless patch nimbus boost-001 --subresource=status --type=merge \
  -p '{"status":{"startingCpu":"","runningCpu":""}}'

# 3. Re-apply, run the controller, observe which node each probe lands on
go run ./cmd
# Add a one-line `logger.Infof("[COLD] sample N landed on node=%s", pod.Spec.NodeName)`
# in probe_cold.go to see the drift live.

# 4. Repeat the search 3 times. The converged values will differ run-to-run
#    if probes land on different nodes — that's the proof of the bug.
```

Recipe to verify the fix (after Phase 2 lands):

```bash
# Force a cluster with intentionally heterogeneous nodes.
# Easiest local approach: cgroup-throttle the worker node to half CPU
# via systemd, leave master full-speed.

# After the per-node search:
kubectl get nimbus boost-001 -o yaml
# Expect status.perNode to contain entries for both master and worker
# with materially different runningCpu values (~2× ratio).
```

---

## 10. Refined design — proposed flow

This section captures the operator-driven flow you sketched: **read the ksvc's `nodeSelector`, derive the candidate node list from it, loop the binary search per node**. The flow is sound — this section verifies it, names the one footgun to avoid, and chooses a storage shape that survives `kubectl get`.

### 10.1 The flow you proposed

```
1. Read the ksvc the Nimbus targets.
2. Inspect spec.template.spec.nodeSelector on that ksvc.
3. Build the candidate-node list:
     a. If nodeSelector is non-empty → list cluster nodes matching those labels.
     b. If nodeSelector is absent or empty → list all Ready, schedulable nodes.
4. For each candidate node:
     a. Pin the next probe to that node.
     b. Run the existing binary search (cold + warm).
     c. Persist the result keyed by node.
     d. Unpin so the next iteration is free to pin elsewhere.
5. Compute aggregate values across all nodes for `kubectl get` display.
```

### 10.2 Why the flow is correct

- **Respects developer intent.** If a dev declared `nodeSelector: workload-tier: gpu` on the ksvc, NIMBUS should not probe nodes outside that label set — those nodes will never run real traffic for this ksvc anyway, so measuring them wastes time and produces irrelevant numbers.
- **Reuses Kubernetes' label-matching primitives.** `kubectl get nodes -l <selector>` is the same algorithm Kubernetes itself uses to honor the ksvc's `nodeSelector`, so the candidate set is guaranteed to match what the scheduler will actually pick from at runtime.
- **Preserves the existing single-node code path** as the trivial 1-iteration case. A ksvc pinned to one specific node (`nodeSelector: kubernetes.io/hostname: worker`) loops once; the only change vs. today is that the result is *labeled* with the node it came from instead of being anonymous.
- **Composes with §1.4** (RT-sample persistence) cleanly — samples become per-node, no schema migration later.

### 10.3 The footgun: don't trample the developer's `nodeSelector`

Pinning the probe to a specific node by **rewriting** `spec.template.spec.nodeSelector` to `kubernetes.io/hostname: <node>` will overwrite whatever the dev declared. Examples:

- Dev declared `nodeSelector: workload-tier: gpu`. NIMBUS rewrites to `kubernetes.io/hostname: master`. Now the ksvc loses the GPU-tier constraint for the duration of the probe — pod schedules on `master` (not GPU-tagged), and your dev gets a phone call.
- After the probe, NIMBUS restores… but there's a brief window where the ksvc spec is wrong. Any reconcile during that window is at risk.

**The correct mechanism is `nodeAffinity`, not `nodeSelector` rewriting:**

```yaml
spec:
  template:
    spec:
      # The dev's original nodeSelector stays untouched.
      nodeSelector:
        workload-tier: gpu
      # NIMBUS adds an additional nodeAffinity that further constrains.
      # nodeSelector AND nodeAffinity intersect — both must be satisfied.
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: kubernetes.io/hostname
                    operator: In
                    values: ["<node>"]
```

After the per-node probe, NIMBUS removes the `nodeAffinity` block (or re-applies the dev's original `affinity` if any). The dev's `nodeSelector` is never touched. This is also the same mechanism the eventual mutating webhook (§2.2.4 of [`algorithm.md`](algorithm.md)) will use to honor existing affinity rules.

**Edge case to handle:** the dev's ksvc may already have a `nodeAffinity` block. Strategy:

1. Read `spec.template.spec.affinity` before the search.
2. Stash the original (possibly nil) into a Nimbus annotation, e.g., `nimbus.lazyken.io/original-affinity` (JSON-encoded).
3. Patch by *appending* a NIMBUS-managed `nodeSelectorTerm` to the existing affinity, not by replacing.
4. After the per-node probe, restore exactly the stashed value and remove the annotation.

### 10.4 What to do when the candidate list is empty

| Situation | Behavior |
|---|---|
| `nodeSelector` matches zero nodes (e.g., dev typo, missing label) | Refuse to run the search. Set `status.phase: Failed` with `status.message: "selector matches no schedulable nodes"`. Do not silently fall back to all-nodes — that's a footgun. |
| `nodeSelector` is empty AND cluster has zero schedulable nodes | Wait. Same retry loop as the existing `missingTargetKsvcs` precondition — log, sleep, try again next tick. |
| `nodeSelector` matches multiple nodes but only one is schedulable | Probe just that one; record the rest in `status.unschedulableNodes` so the operator sees what was skipped. |

### 10.5 Storage — three candidate shapes

The current display:

```
$ kubectl get nimbus -n serverless
NAME        STARTING CPU   RUNNING CPU   AGE
boost-001   706m           219m          8d
```

is two flat columns from `.status.startingCpu` and `.status.runningCpu`. With per-node data, those columns need a definition. Three options:

#### Shape A — Aggregate top-level + per-node detail (recommended)

```yaml
status:
  # Backward-compatible top-level: a SINGLE summary value across all nodes.
  # Defined as max(perNode[*].startingCpu) — the worst case.
  # "Worst" is the safest summary: applying it cluster-wide can't starve any node.
  startingCpu: 800m
  runningCpu:  240m

  # Provenance: which nodes were measured, in deterministic order.
  measuredNodes: ["master", "worker"]

  # Per-node detail. Key = node name (kubectl get nodes).
  perNode:
    master:
      startingCpu: 800m
      runningCpu:  240m
      measuredAt:  "2026-04-28T14:00:00Z"   # staleness detection
      coldRtSamples: [ ... ]                 # post-§1.4
      warmRtSamples: [ ... ]                 # post-§1.4
    worker:
      startingCpu: 706m
      runningCpu:  219m
      measuredAt:  "2026-04-28T14:30:00Z"
      coldRtSamples: [ ... ]
      warmRtSamples: [ ... ]
```

`kubectl get nimbus`:

```
NAME        NODES            STARTING CPU   RUNNING CPU   AGE
boost-001   master,worker    800m           240m          8d
```

(`measuredNodes` array renders as comma-joined when used in `additionalPrinterColumns` with `type: string`.)

**Pros**
- Top-level columns stay as two values — minimum disruption for operators used to `kubectl get`.
- Aggregate is explicitly the *safe* (worst-case) value — applying it cluster-wide always works.
- Per-node detail is one `-o yaml` away — full transparency.
- New `NODES` column makes the multi-node nature visible at a glance.
- Cleanly extends with §1.4 sample arrays (already shown above).

**Cons**
- Defining "summary" as `max` is opinionated. A different `applyStrategy` (median, fastest-node, manual override) would change the top-level value and confuse operators who learned the column under one definition.
- `perNode` is a map — `kubectl get -o jsonpath` is awkward for map iteration (`.status.perNode.master.runningCpu` works, but you have to know the node names).

#### Shape B — Per-node rows in printer columns

Render each measured node as its own row in `kubectl get`:

```
NAME        NODE     STARTING CPU   RUNNING CPU   AGE
boost-001   master   800m           240m          8d
boost-001   worker   706m           219m          8d
```

This requires either (a) a separate `NimbusProfile` CRD that holds one row per (Nimbus, node) pair, or (b) a `kubectl` plugin that fans out a single Nimbus into multiple display rows. Neither is part of stock Kubernetes.

**Pros**
- Each row is a single number — no aggregation question.
- Sortable / greppable / scriptable by node.

**Cons**
- Either a new CRD (extra schema, extra reconciler logic, garbage-collection lifecycle) or a kubectl plugin (deployment story for operators).
- Loses the "one Nimbus = one CR" mental model that operators are used to.

#### Shape C — Slashed columns

Pack node→value into a single column:

```
NAME        STARTING CPU                 RUNNING CPU                  AGE
boost-001   master=800m,worker=706m      master=240m,worker=219m      8d
```

This works with stock `kubectl` if you wire the `additionalPrinterColumns` carefully (a JSONPath that joins the per-node values into a single string — but stock JSONPath can't iterate a map and concatenate, so this realistically requires a small status-summarizer in the controller that materializes a `status.startingCpuByNode` string).

**Pros**
- Single CR, single row, full information at a glance.
- No new CRDs, no plugins.

**Cons**
- Truncates badly with >3 nodes. A 10-node cluster produces unreadable columns.
- Composite string isn't scriptable (`-o jsonpath` returns the whole string; you have to split it yourself).

### 10.6 Recommended storage shape

**Shape A.** It's the cheapest, the most forward-compatible with §1.4 and the online stage, and degrades gracefully for the most likely cluster sizes (1–10 nodes). The aggregate-as-`max` rule is conservative-by-default, which is the right bias when the field will eventually be consumed by an SLO-aware online algorithm.

A future `spec.applyStrategy` field can override the aggregate definition (`max | median | min | manual:<node>`) without breaking the schema. Don't add that field in the first multi-node implementation — wait until you have a reason.

### 10.7 Updated printer-columns block (CRD diff)

Replace the current block in [`config/crd.yaml`](config/crd.yaml) with:

```yaml
additionalPrinterColumns:
  - name: Nodes
    type: string
    jsonPath: '.status.measuredNodes'
  - name: Starting CPU
    type: string
    description: max across measured nodes
    jsonPath: .status.startingCpu
  - name: Running CPU
    type: string
    description: max across measured nodes
    jsonPath: .status.runningCpu
  - name: Age
    type: date
    jsonPath: .metadata.creationTimestamp
```

The `description` fields show up in `kubectl explain nimbus.status.startingCpu`, so the "max across nodes" semantics is documented where operators look first.

### 10.8 Step-by-step implementation order

(For when you're ready to code — stays a plan for now.)

1. **Schema first.** Add `measuredNodes`, `perNode`, `perNode[*].measuredAt`, plus the §1.4 sample arrays to `NimbusStatus` and the CRD. Don't change controller behavior yet — just deploy the new CRD so old `.status` data continues to validate.
2. **Node-discovery helper.** A new function in `internal/watcher/` (or a new `internal/scheduling/` package): `candidateNodes(ctx, ksvc)` returning `([]string, error)`. Reads the ksvc's `nodeSelector`, lists matching schedulable nodes, returns names. Empty result is an explicit error (per §10.4).
3. **Affinity stash + restore.** Two helpers in `api/kubeapi/`: `pinKsvcToNode(ctx, ns, ksvc, node)` and `unpinKsvcFromNode(ctx, ns, ksvc)`. Use the annotation pattern from §10.3 for the original-affinity stash. Wait for scale-to-zero between pin and probe (existing `waitForScaleToZero` already does this).
4. **Outer loop in `BinarySearch`.** Wrap the existing per-node search in a loop over `candidateNodes`. Each iteration: pin → search → write `status.perNode[node]` (incrementally so a SIGINT halfway through doesn't lose all results) → unpin.
5. **Aggregate writer.** After the loop, compute `max(perNode[*].startingCpu)` / `max(perNode[*].runningCpu)`, write them as the top-level fields. Also write `measuredNodes` as the deterministic-ordered list (sorted by name).
6. **Compatibility check.** Run `./build.sh` (the activator one) is unrelated; the relevant test is the existing `kubectl get nimbus` continues to show two columns with sensible values, and `kubectl get nimbus -o yaml` reveals the new structure.

### 10.9 Why the update is also a good idea now

Doing the multi-node refactor before §1.4 (RT-sample persistence) means the per-node sample arrays get the right home from day one — they live under `status.perNode[<node>].coldRtSamples` rather than at the top level where they'd need a CRD migration to relocate. **If you're going to do §1.4 anyway, do this work first** (or at the same time, in the same CRD bump). Doing §1.4 first and multi-node later forces a schema rename of the sample arrays, which is annoying.

The sequencing recommended at the top of the doc (Phase 1 track-only → Phase 2 per-node + §1.4 → Phase 3 webhook) is unchanged by §10. §10 just refines what Phase 2 looks like in concrete code, and confirms the `nodeSelector`-driven candidate-node logic is the right design for it.

---

## 11. First mechanism — node-list discovery (verify before coding)

> What this section covers: a stand-alone Go helper that, given a Nimbus event, returns the list of cluster nodes its target ksvc is eligible to run on (filtered by the ksvc's `nodeSelector` + `nodeAffinity`). Read-only — no patching, no scheduling change, no status write. It's the smallest meaningful brick of the multi-node refactor and is fully testable in isolation.

### 11.1 Verdict — yes, this is the right first step

Three reasons it's the correct scope to start with:

1. **Read-only.** It calls `kubectl get ksvc` and `kubectl get nodes` — no PATCH, no DELETE. If it's wrong, the worst case is a logged warning. The current binary search keeps working unchanged while you iterate on this helper.
2. **Foundation for every later phase.** Phase 2 (per-node search) loops over its output. Phase 3 (mutating webhook) checks "is this node in the candidate list?" against it. §1.4 (RT-sample persistence) keys the per-phase samples by node names this function produces. Get the discovery right once, and the rest of the multi-node work is straightforward.
3. **Surfaces the hidden bugs early.** Empty match-result, multi-node label selectors, missing labels — all the corner cases enumerated in §10.4 — will fall out of testing this helper before they bite you in production code paths.

The one thing it doesn't validate is the affinity *patching* logic (§10.3) — that's Phase 2 work, not this brick. Don't try to combine them.

### 11.2 Three placement options for the Go code

A discovery helper has to (a) read the ksvc spec, (b) list cluster nodes, (c) filter. All three need access to `kubeconfig.DYNCLIENT` / `kubeconfig.CLIENTSET`. The question is *where in the package tree* it lives.

#### Placement A — method on `NimbusWatcher`

Add a sibling to the existing `missingTargetKsvcs` in [`internal/watcher/watcher.go`](internal/watcher/watcher.go):

```go
func (nw *NimbusWatcher) candidateNodes(ctx context.Context, ev *nimbusevent.NimbusEvent) ([]string, error) {
    // …
}
```

**Pros**
- Matches the existing pattern. `missingTargetKsvcs` is already a method on `NimbusWatcher` that does similar work (read ksvc, talk to K8s).
- No new package or file to introduce — minimum disruption.
- Watcher's `RunWorker` calls it directly; no import gymnastics.

**Cons**
- `internal/watcher/` is becoming a mixed-concerns package (queue management + scheduling logic + per-Nimbus orchestration). Adding more methods worsens this.
- Hard to unit-test the helper without spinning up the whole watcher. Method requires a `NimbusWatcher` receiver.

#### Placement B — method on `NimbusEvent`

Add to [`api/nimbusevent/event_type.go`](api/nimbusevent/event_type.go):

```go
func (n *NimbusEvent) CandidateNodes(ctx context.Context) ([]string, error) {
    // …
}
```

**Pros**
- Reads naturally at call sites: `current.CandidateNodes(ctx)`.
- Keeps the function near the data it operates on.

**Cons**
- The `nimbusevent` package is currently a **pure type-defs package**. It has zero imports of Kubernetes clients. Adding this method forces it to import `kubeconfig` (or accept the clients as args), which couples a leaf-of-the-graph package to operational concerns.
- Hard to test in isolation for the same reason as A — needs real or fake K8s clients.

#### Placement C — new `internal/scheduling/` package

A fresh package with one job: figure out where pods can run.

```
internal/scheduling/
  nodes.go        // CandidateNodes(ctx, ev) ([]string, error)
  selectors.go    // NodeSelectorMatches, NodeAffinityMatches
  nodes_test.go   // unit tests with a fake clientset
```

**Pros**
- Cleanest separation. `internal/watcher/` and `api/algorithm/` both call into `internal/scheduling/` without owning its complexity.
- Testable in isolation with `k8s.io/client-go/kubernetes/fake` and a few fixture nodes — no real cluster needed.
- The package can grow naturally: when Phase 2 lands, add `Pin(ctx, ksvc, node)` / `Unpin(ctx, ksvc)` here too, keeping all node-related logic in one place.
- Clear naming for the eventual webhook (Phase 3): "scheduling" is exactly its concern.

**Cons**
- One new package + one new file vs. one new function in an existing file.
- Slightly more glue at the call site (`scheduling.CandidateNodes(ctx, ev)` vs. `nw.candidateNodes(ctx, ev)`).

#### Recommendation

**Placement C.** It's a small upfront cost (one new package directory) for a meaningful long-term win — every node/scheduling concern in the project ends up in the same place, and the helper can be unit-tested without touching `NimbusWatcher`. Placement A is the right choice only if you're certain you'll never write a Phase 2 / Phase 3 — which based on `algorithm.md` you definitely will.

If you'd rather start as small as possible, Placement A is fine *and refactor-friendly*: `gofmt` plus `goimports` makes the eventual move from A to C a 5-minute mechanical edit. Just don't pick B — coupling `nimbusevent` to clients is hard to unwind later.

### 11.3 Proposed API

```go
// Package scheduling figures out where pods of a Nimbus-managed ksvc can run.
package scheduling

import (
    "context"

    "nimbus/api/nimbusevent"
)

// CandidateNodes returns the names of cluster nodes that:
//   1. are Ready and schedulable (no NoSchedule taints unless the ksvc tolerates them — for v1, ignore tolerations and just check Ready+!Unschedulable),
//   2. satisfy spec.template.spec.nodeSelector on the ksvc the Nimbus targets, AND
//   3. satisfy any requiredDuringSchedulingIgnoredDuringExecution nodeAffinity terms.
//
// Names are returned in deterministic (lexicographic) order so subsequent
// per-node loops are reproducible across reconciles.
//
// Errors:
//   - ErrNoMatchingNodes when the selector / affinity matches zero schedulable
//     nodes — likely a misconfigured ksvc; caller should refuse to start the
//     binary search rather than fall back to all-nodes (see §10.4).
//   - Any underlying API error from listing the ksvc or nodes.
func CandidateNodes(ctx context.Context, ev *nimbusevent.NimbusEvent) ([]string, error)
```

The function is the only public entry point. Its internal implementation uses two unexported helpers — the user wouldn't see them, but they're worth naming for the test plan below:

```go
// nodeSelectorMatches: does the node's labels satisfy the ksvc's nodeSelector?
func nodeSelectorMatches(node *corev1.Node, sel map[string]string) bool

// nodeAffinityMatches: does the node satisfy any one of the
// requiredDuringSchedulingIgnoredDuringExecution nodeSelectorTerms?
//
// nil affinity → returns true (no constraint = unconstrained, matches all).
// preferredDuringScheduling… is intentionally ignored — for profiling we
// want hard constraints only.
func nodeAffinityMatches(node *corev1.Node, aff *corev1.NodeAffinity) bool
```

For affinity v1, support only `requiredDuringSchedulingIgnoredDuringExecution` and only the `In` / `NotIn` / `Exists` / `DoesNotExist` operators. Skip `preferred…` until you have a real reason — preferred-affinity is a soft hint, not a hard constraint, and including it widens the candidate set in ways that are hard to verify against scheduler intent.

### 11.4 Step-by-step test plan

You can drive this entirely from a tiny standalone Go program (or a `go test` file) before wiring it into the binary search. The order below is from "easiest to verify" to "edge case that's likely to bite later".

#### Step 1 — sanity: the function compiles and returns *some* list

A throwaway `cmd/scheduling-debug/main.go`:

```go
package main

func main() {
    ctx := context.Background()
    ev := &nimbusevent.NimbusEvent{
        Metadata: nimbusevent.NimbusMetadata{Namespace: "serverless", Name: "boost-001"},
        Selector: nimbusevent.NimbusSelector{MatchExpressions: []nimbusevent.MatchExpression{
            {Key: "serving.knative.dev/service", Operator: "In", Values: []string{"measure-yolo"}},
        }},
    }
    nodes, err := scheduling.CandidateNodes(ctx, ev)
    fmt.Println(nodes, err)
}
```

Run `go run ./cmd/scheduling-debug`. Expected output for your current cluster (with `sampleapp.yaml` pinning to `worker`):

```
[worker] <nil>
```

#### Step 2 — remove the pin, verify the list expands

```bash
kubectl -n serverless patch ksvc measure-yolo --type=json \
  -p='[{"op":"remove","path":"/spec/template/spec/nodeSelector"}]'
```

Re-run. Expected:

```
[master worker] <nil>
```

#### Step 3 — break the selector, verify the error

```bash
kubectl -n serverless patch ksvc measure-yolo --type=merge \
  -p='{"spec":{"template":{"spec":{"nodeSelector":{"workload-tier":"gpu"}}}}}'
```

Re-run. Expected:

```
[] errnomatchingnodes (or whatever sentinel you defined)
```

(Then restore: `kubectl … --type=json -p='[{"op":"remove","path":"/spec/template/spec/nodeSelector"}]'`)

#### Step 4 — add nodeAffinity, verify it AND-composes with nodeSelector

Patch the ksvc with both a permissive `nodeSelector` and a restrictive `nodeAffinity`:

```bash
kubectl -n serverless patch ksvc measure-yolo --type=merge -p='
{"spec":{"template":{"spec":{
  "nodeSelector": {"kubernetes.io/os":"linux"},
  "affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{
    "nodeSelectorTerms":[{"matchExpressions":[
      {"key":"kubernetes.io/hostname","operator":"In","values":["worker"]}
    ]}]
  }}}
}}}}'
```

Re-run. Expected: `[worker]` only (intersection of "Linux" — both nodes — and "hostname=worker" — one node).

If you get `[master worker]`, the affinity intersection isn't working — likely you forgot to AND the two checks.

#### Step 5 — make a node `NotReady`, verify it's excluded

Cordon `worker`:

```bash
kubectl cordon worker
```

Re-run. Expected: only `master` is returned (or empty if the ksvc was pinned to worker).

```bash
kubectl uncordon worker
```

#### Step 6 — unit tests with `client-go/kubernetes/fake`

Once steps 1–5 pass against the real cluster, freeze the same cases as table-driven unit tests so future refactors don't regress:

```go
func TestCandidateNodes(t *testing.T) {
    cases := []struct {
        name   string
        ksvc   *unstructured.Unstructured  // the ksvc fixture
        nodes  []*corev1.Node              // node fixtures
        want   []string
        wantErr error
    }{
        {"pin-to-worker", ..., []string{"worker"}, nil},
        {"no-selector",   ..., []string{"master","worker"}, nil},
        {"unmatched-label", ..., nil, ErrNoMatchingNodes},
        {"nodeselector-AND-nodeaffinity", ..., []string{"worker"}, nil},
        {"node-not-ready", ..., []string{"master"}, nil},
        {"node-cordoned",  ..., []string{"master"}, nil},
    }
    // … standard table-driven loop
}
```

This is the first chunk of the test infrastructure (W-R5 in CLAUDE.md). Once you have a `go test` file with this many cases, adding more for Phase 2 / Phase 3 is just adding rows.

### 11.5 What this step does **not** do (intentional scope limits)

Make sure you don't expand the scope of this brick — each of these is a separate later piece:

- **Doesn't pin probes to a node.** That's Phase 2's `Pin(ctx, ksvc, node)` (§10.3 affinity stash + restore).
- **Doesn't loop the binary search.** Calling code (a future Phase 2 wrapper around `BinarySearch`) handles the loop.
- **Doesn't store anything in `.status`.** No write path at all in this brick.
- **Doesn't change `kubectl get nimbus` output.** The aggregate columns from §10.5 only become meaningful after Phase 2.
- **Doesn't honor `tolerations` / `taints`.** Out of scope for v1; document as a known limitation in the function's godoc.
- **Doesn't honor `preferredDuringScheduling…` nodeAffinity.** Hard constraints only.

If any of those creep in during implementation, push them out — they're separate bricks.

### 11.6 Known limitations of `computeCandidateNodes`

The function applies four filters: Ready+!unschedulable, ksvc `nodeSelector`, ksvc `nodeAffinity` (required-only), ksvc `tolerations` vs node taints. Anything else the kube-scheduler considers is **ignored**. Each ignored factor is a potential false-positive (we list a node that the scheduler will actually reject) — but each is also rare enough in app code that handling it costs more than it's worth at this stage.

| Ignored factor | Why kube-scheduler cares | Why we don't | Impact when violated |
|---|---|---|---|
| **`PreferNoSchedule` taints** | Soft hint to avoid the node | Soft hint, not a hard constraint. Matches kube-scheduler's hard-eligibility pass. | None for correctness — kube-scheduler still places there if nothing else fits. We may pick a node the scheduler would have preferred to avoid. |
| **`preferredDuringSchedulingIgnoredDuringExecution` nodeAffinity** | Soft preference toward matching nodes | Same: soft hint, not a hard constraint. | None for correctness — same rationale. |
| **`matchFields` on nodeAffinity terms** | Affinity by node *field* (e.g. `metadata.name`), not labels. Equivalent to nodeName-pinning. | Rare in app manifests; we'd need to walk the node's metadata, not its labels. | If a dev pins via `matchFields: metadata.name=foo`, we'd list other nodes that the scheduler will reject. |
| **`Gt` / `Lt` operators in nodeAffinity matchExpressions** | Numeric comparison on label values (e.g. `cpu-cores > 16`). | Almost never used by app developers. Implementing requires `strconv.ParseInt` per label. | If a dev uses Gt/Lt, our `exprMatches` returns false for that requirement → we under-include nodes (false negatives, not false positives — *safer* error direction). |
| **`podAffinity` / `podAntiAffinity`** | "Place near/away from other pods with label X" | Requires reading every other pod in the namespace and computing affinity at discovery time. Real schedulers do this at admission, with up-to-date pod state. We can't replicate that statically. | If a dev declares "co-locate with redis", we'd list nodes that don't have redis — scheduler rejects → ImagePullBackOff or Pending. Probe fails loudly. |
| **`topologySpreadConstraints`** | Spread pods across zones / nodes / hostnames | Irrelevant for `maxScale=1` single-pod profiling. | None at probe time (only one pod). Becomes relevant if we ever lift `maxScale=1`. |
| **Volume affinity (PVC zone-bound)** | Pod can only run where its PVCs can attach | Knative services are stateless by convention. | Doesn't apply to typical ksvcs. |
| **`schedulerName` (custom scheduler)** | A non-default scheduler with custom logic | We assume the default scheduler. Custom schedulers can do anything. | If the dev uses a custom scheduler, our discovery may diverge from its decisions. Document, don't try to mirror. |
| **`nodeName` (direct binding, bypasses scheduler)** | Pod is hard-bound to one node, no scheduling at all | If a dev sets `nodeName: foo`, the pod *only* runs on `foo`. Our discovery returns the broader candidate list, but at probe time the pod always lands on `foo`. | Functionally harmless — every probe lands on `foo` anyway. We just print a misleadingly large candidate list. |
| **Resource-fit (does the node have CPU/memory available right now?)** | Node must have allocatable headroom for the pod's requests | Dynamic state — changes by the second. Belongs at probe-time, not discovery-time. | If a candidate node is full at probe time, kube-scheduler rejects → existing `waitForScaleToZero` retry loop surfaces it. The probe will re-attempt or eventually time out via `coldSampleWithStuckRecovery`. |
| **Pod priority / preemption** | High-priority pods can evict lower-priority ones | Same dynamic-state argument as resource fit. | Doesn't affect static eligibility. |
| **Node lease / heartbeat freshness** | A "Ready" node whose kubelet heartbeat is stale is effectively offline | We trust `status.conditions[Ready]==True`. If the node controller hasn't yet flipped Ready→False, we'd list it. | Brief window (~node-monitor-grace-period, default 40s) of false positives after a node dies. Probe will then fail and `coldSampleWithStuckRecovery` will retry. |

**Summary by direction of error:**

- **False positives** (we list a node the scheduler will reject): `matchFields`, `podAffinity`/`podAntiAffinity`, `nodeName`, custom scheduler, stale-heartbeat node, resource exhaustion. These produce a probe that fails to schedule — the existing retry/timeout machinery contains the damage.
- **False negatives** (we exclude a node the scheduler would accept): `Gt`/`Lt` operators (we return false). Means the search runs on a smaller candidate set than the dev intended, but the result is still correct for the nodes it *did* measure.

**No silent correctness bugs** — false positives surface as visible probe failures; false negatives just shrink the search scope. Both are recoverable.

**Hard taints are honored.** Adding tolerations support closed the original bug where `master` (with `node-role.kubernetes.io/control-plane:NoSchedule`) would be listed for ksvcs that don't tolerate it. After this change, your `[master worker]` candidate list will only include `master` if the ksvc has the right toleration — exactly matching what kube-scheduler will do.

### 11.7 Filter logic walkthrough — how the four filters combine

Logic question: when a ksvc declares **all three** of `nodeSelector`, `nodeAffinity`, and `tolerations`, how do they compose?

#### The four-filter pipeline

`computeCandidateNodes` applies four predicates in sequence; a node must pass **all four** to be a candidate. The composition is **AND** at the top level — every filter must say yes — with each filter having its own internal boolean shape:

```
For each node in the cluster:
    1. isReadyAndSchedulable(node)         ← node-side gate (always applied)
    2. nodeSelectorMatches(node, sel)      ← ksvc.spec.template.spec.nodeSelector
    3. nodeAffinityMatches(node, aff)      ← required-only nodeAffinity
    4. tolerationsMatch(node, tols)        ← every hard taint must be tolerated

If all four pass → include the node.
If any fails  → skip to the next node.
```

| Filter | Internal logic |
|---|---|
| `nodeSelector` | **AND** over all key/value pairs — *every* label must match |
| `nodeAffinity.requiredDuringScheduling…` | **OR** over `nodeSelectorTerms` — *any one* term suffices |
| One `nodeSelectorTerm` | **AND** over `matchExpressions` — every expression must match |
| `tolerations` vs `taints` | For each `NoSchedule`/`NoExecute` taint: **∃** a toleration that matches it. (Universal over taints, existential over tolerations.) |

Formally:

```
candidate(node) ⇔
    Ready ∧ ¬cordoned
  ∧ ⋀(k,v)∈nodeSelector  node.labels[k] = v
  ∧ ⋁ term∈nodeAffinity.terms  ⋀ expr∈term  matches(node, expr)
  ∧ ⋀ taint∈node.taints  ⋁ tol∈tolerations  tolerates(tol, taint)
```

#### Worked example

**Cluster — 4 nodes:**

| Node     | `zone`    | `gpu`   | `tier`       | Taints                                                                       |
|----------|-----------|---------|--------------|------------------------------------------------------------------------------|
| `node-a` | `us-west` | `true`  | `production` | `gpu-only:NoSchedule`                                                        |
| `node-b` | `us-west` | `false` | `production` | (none)                                                                       |
| `node-c` | `us-east` | `true`  | `staging`    | `gpu-only:NoSchedule`                                                        |
| `node-d` | `us-west` | `true`  | `production` | `gpu-only:NoSchedule`, `node-role.kubernetes.io/control-plane:NoSchedule`    |

**ksvc spec.template.spec uses all three constraints:**

```yaml
nodeSelector:                                     # filter 2 — ALL must match
  zone: us-west
  tier: production
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:                          # filter 3 — ANY term, AND inside term
        - matchExpressions:
            - key: gpu
              operator: In
              values: ["true"]
tolerations:                                      # filter 4 — must cover EVERY hard taint
  - key: gpu-only
    operator: Exists
    effect: NoSchedule
```

**Walk-through (each row = one node going through the four filters):**

| Node     | F1: Ready+sched | F2: nodeSelector                                       | F3: nodeAffinity (`gpu=true`)              | F4: every hard taint tolerated?                                                            | Result |
|----------|-----------------|--------------------------------------------------------|--------------------------------------------|--------------------------------------------------------------------------------------------|--------|
| `node-a` | ✓               | ✓ — `zone=us-west` ✓, `tier=production` ✓              | ✓ — `gpu=true` matches `In ["true"]`       | ✓ — only taint is `gpu-only:NoSchedule`, covered by the `Exists` toleration               | **INCLUDED** |
| `node-b` | ✓               | ✓                                                      | ✗ — `gpu=false` ≠ `"true"`                 | —                                                                                          | excluded at F3 |
| `node-c` | ✓               | ✗ — `zone=us-east` ≠ `us-west` AND `tier=staging` ≠ `production` | —                              | —                                                                                          | excluded at F2 |
| `node-d` | ✓               | ✓                                                      | ✓                                          | ✗ — `gpu-only` is tolerated, but `node-role.kubernetes.io/control-plane:NoSchedule` is not | excluded at F4 |

Final candidates: **`[node-a]`**.

#### Common pitfalls in reading this composition

- **`nodeSelector` and `nodeAffinity` are AND'd, not OR'd.** A common misreading: "if either `zone=us-west` is in `nodeSelector` *or* `gpu=true` is in `nodeAffinity`, include the node." Wrong — Kubernetes (and our code) require both. To express OR, encode the alternatives inside `nodeAffinity`'s `nodeSelectorTerms` (which OR among themselves) and don't use `nodeSelector` at all. Example for "us-west OR gpu":
  ```yaml
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - {key: zone, operator: In, values: ["us-west"]}
          - matchExpressions:
              - {key: gpu, operator: In, values: ["true"]}
  ```
  These two terms are OR'd. A node satisfying *either* term passes F3.

- **Tolerations are permissive, not selective.** A toleration only makes a tainted node *eligible* — it doesn't *prefer* it. Adding the `gpu-only` toleration above doesn't bias the search toward tainted nodes; it just stops F4 from excluding them. If the cluster also had an untainted node that satisfied F1+F2+F3, both nodes would land in `candidates`.

- **Filter order is an optimization, not a logical contract.** The early-exit sequence (Ready → nodeSelector → nodeAffinity → tolerations) is "cheapest checks first" so we skip uninteresting nodes fast. Permuting the order would produce the same candidate set — the result is fully determined by the AND of all four predicates.

- **Empty filters mean "no constraint", not "no nodes".** A ksvc with no `nodeSelector` and no `affinity` and no `tolerations` causes F2/F3/F4 to return `true` for every node, so the candidate set equals every Ready+schedulable node. This is the "no constraints declared → any node works" case the dev expects.

#### Two ways to pin to a single node — only one of them is honored

There's a subtle but consequential difference between two ways of saying "run this pod on `worker`":

**(a) `nodeSelector: { kubernetes.io/hostname: "worker" }`** — pinning by the standard hostname label. Every node carries `kubernetes.io/hostname` set by the kubelet to its own hostname, so this is just a regular label match. **F2 handles it natively.** This is what `config/sampleapp.yaml` does today. Walk-through:

| Node     | `kubernetes.io/hostname` label | F2 nodeSelector match | Result |
|----------|--------------------------------|-----------------------|--------|
| `master` | `master`                       | ✗ `master` ≠ `worker` | excluded at F2 |
| `worker` | `worker`                       | ✓                     | reaches F3/F4 |

If the dev pins to a node *that has untolerated taints*, F4 will catch it and `ErrNoMatchingNodes` will fire — surfacing the misconfiguration loudly instead of letting the search start and then fail at probe time.

**(b) `spec.template.spec.nodeName: "worker"`** — a *separate* PodSpec field (not inside `nodeSelector`) that hard-binds the pod to the named node. **The scheduler is bypassed entirely** — the kubelet on `worker` picks up the pod directly. Our code does **not** read this field today.

Implication: if a dev sets `nodeName: worker-1` on a 3-worker cluster with no other constraints, our `computeCandidateNodes` would return `[worker-1, worker-2, worker-3]` (all three pass F1–F4 with no constraints), but in reality only `worker-1` will ever run the pod. The other two are false positives — we'd "measure" them by patching the ksvc to add a probe-time `nodeAffinity` for them, which would *override* the `nodeName` binding and trigger a fresh scheduling decision (defeating the dev's intent). **Don't pin via `nodeName` and expect this controller to respect it.** Use option (a) instead.

The limitation appears in §11.6's table as the `nodeName` row. Adding explicit support is ~5 LOC (read `podSpec.NodeName` in `readKsvcScheduling`, early-exit in `computeCandidateNodes`) — deferred until a real user hits the case, since `nodeName` in app YAML is exceedingly rare in practice.

### 11.8 Resolved decisions (record of what was chosen)

| Question | Choice | Rationale |
|---|---|---|
| Placement | **Method on `NimbusWatcher`** with the per-Nimbus `CandidateNodes` field on `NimbusEvent`. Lives in `internal/watcher/candidate_nodes.go` — *not* in a separate `scheduling/` package. | Per-Nimbus runtime data belongs on the event (matches `High`/`Low`/`StartingCPU`); computation belongs next to its sibling helper `missingTargetKsvcs`; "scheduling" name is reserved for the future online-stage pod-placement logic. |
| Affinity scope | **`requiredDuringSchedulingIgnoredDuringExecution` only** | Hard-eligibility check; preferred-affinity is a soft hint that the scheduler can override. Listing nodes that violate a soft hint isn't a correctness bug. |
| Empty-match behavior | **Error out** with `ErrNoMatchingNodes`; caller logs + retries on next tick | Misconfigured `nodeSelector` should be loud, not silently masked by a fall-back-to-all-nodes path. |
| Taint handling | **Hard: `NoSchedule` + `NoExecute` excluded if untolerated. Soft: `PreferNoSchedule` ignored.** | Matches kube-scheduler's hard-eligibility pass exactly. Without this, master-tainted clusters would falsely list `master` as a candidate. |
| Defensive selector check | **Removed** — trust `validateNimbus` already ran upstream | Matches the existing convention in `binary_search.go` (also indexes `[0]` directly). Reduces redundant code. |
