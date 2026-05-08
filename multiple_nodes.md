# Multi-Node Profiling — Offline-Stage Audit

> **As of 2026-05-08.** Phase 2 (per-node measurement loop) is wired and stable. This doc was originally a design exploration; with that work landed it's been trimmed to the parts still actionable: (a) what's done, (b) what to improve in the offline state before moving to the online stage, (c) the still-pending Phase 3 (in-place pod resize), and (d) reference material that's worth keeping for thesis writeup.

---

## 1. The problem (recap)

NIMBUS originally measured CPU on whichever node Knative happened to schedule the test pod on, then applied the converged value cluster-wide. On a heterogeneous cluster (different kernels, microarchitectures, frequency caps, NUMA topology) the same CPU quota delivers different effective performance per node, so a single number was wrong on at least one node by construction.

Phase 2 fixed this for measurement: each candidate node is now measured independently. Phase 3 (§4 below) is needed to fix it for *apply* — today the cluster-wide ksvc CPU is the max across nodes, which over-provisions every faster node.

---

## 2. What's done in the offline stage

| Concern | Implementation | Where |
|---|---|---|
| Candidate-node discovery (Ready+!unschedulable, nodeSelector, required nodeAffinity, tolerations vs taints) | `discoverCandidateNodes` + `computeCandidateNodes` + filters in one file | [`internal/watcher/candidate_nodes.go`](internal/watcher/candidate_nodes.go) |
| Per-node binary-search loop with pin/unpin via `nodeSelector["kubernetes.io/hostname"]` merge-patch | `runMultiNodeSearch`, single deferred `UnpinKsvc` | [`internal/watcher/multi_node.go`](internal/watcher/multi_node.go) |
| Per-node result map | `NimbusEvent.PerNodeResults map[string]*NodeResult` | [`api/nimbusevent/event_type.go`](api/nimbusevent/event_type.go) |
| Two-layer saturation | Inner: `NodeResult.{StartingSaturated, RunningSaturated}`. Outer: `NimbusEvent.AllSaturated`. Invariant maintained by `recomputeAllSaturated`. | [`internal/watcher/multi_node.go`](internal/watcher/multi_node.go) |
| Status persistence | `status.perNode: { <node>: {startingCpu, runningCpu} }`. No flat top-level aggregate. | [`api/kubeapi/nimbus_status.go`](api/kubeapi/nimbus_status.go) + [`config/crd.yaml`](config/crd.yaml) |
| Apply-time collapse | `MaxStartingCpu` / `MaxRunningCpu` — slowest node sets the cluster-wide floor | [`api/kubeapi/calculate_cpu.go`](api/kubeapi/calculate_cpu.go) |
| Fast path | Outer skip when `AllSaturated`. Inner skip per node on partial saturation (resumes after restart mid-loop). | [`internal/watcher/watcher.go`](internal/watcher/watcher.go) `RunWorker` |
| Per-phase per-node convergence | `BinarySearch(ctx, current, node)` writes `PerNodeResults[node].{StartingCpu, RunningCpu}` | [`api/algorithm/binary_search.go`](api/algorithm/binary_search.go) |

---

## 3. Improvements to land before moving to online

Ordered roughly by how much each one will hurt the online stage if skipped. **Items 3.1–3.3 are blockers** in the sense that the online stage as currently sketched in `algorithm.md` and CLAUDE.md depends on them; the rest are good hygiene that's cheaper to do now than after online code lands on top.

### 3.1 Sample-array persistence (`coldRtSamples` / `warmRtSamples`)

The online algorithms in §2.2 of `algorithm.md` consume per-probe response-time distributions (`samples_i`), not just the converged CPU number. Today only the converged value is persisted. Wire `getResptCold` / `getResptWarm` to record each individual sample's `(cpu, rt)` pair into `NodeResult`, persist as part of `status.perNode[<node>]`. Skipping this forces a CRD migration after online lands.

**Estimated cost**: ~50 LOC + CRD `.status.perNode[*]` schema bump. Pure additive — no behavior change.

### 3.2 Resource-ownership rule for `containers[0].resources.limits.cpu`

Today four code paths can write this field:
1. `binary_search.go` → `getResptWarm` (per-probe ksvc patch)
2. `RunWorker` apply step (max-collapse write at end of search)
3. `ksvc_watcher.go` (propagation to newly-created ksvcs)
4. *(future)* online tuner

Without an explicit ownership rule, the online tuner will fight the existing writers. Decide and document in CLAUDE.md before adding the fourth writer:

- **Suggested rule**: ksvc template default = offline `MaxRunningCpu`. Per-pod resize (Phase 3) = node-specific override. Online tuner adjusts the *template* default only, never live pods directly. Pod-resize + online-tuner are orthogonal.

### 3.3 Per-node memoization re-entry

Fast path skips when `AllSaturated`. Online stage will want to invalidate *individual* nodes (drift detected on one node only) without redoing the others. The infrastructure already supports it — `recomputeAllSaturated` derives outer from inner — so adding a "clear one node's saturation" code path is small. Without it, online drift on one node forces a full re-search.

**Estimated cost**: ~20 LOC. New helper, plus a CRD subresource patch shape (`status.perNode.<node>.startingCpu = ""`) the online tuner can call.

### 3.4 Warm-probe retry budget (W-R1 in REVIEW.md)

Cold probes have `coldSampleWithStuckRecovery` (probeTimeout=120s, maxStuckRetries=3). Warm probes have nothing — a flaky network kills the whole search. Online stage runs many more probes than offline, so flake resilience matters more there. Mirror the cold-probe pattern in `getResptWarm`.

**Estimated cost**: ~30 LOC (a `warmSampleWithRetry` wrapper paralleling the cold version).

### 3.5 `resizePolicy` injection at apply time

Required by Phase 3 (§4) AND by any online-stage in-place resize. One JSON-patch op alongside the existing `PatchResourceLimits`:

```yaml
spec.template.spec.containers[0].resizePolicy:
  - resourceName: cpu
    restartPolicy: NotRequired
```

Without it, the kubelet rejects in-place resize patches. Adding it once covers both Phase 3 and the online tuner.

**Estimated cost**: ~10 LOC in [`api/kubeapi/knative.go`](api/kubeapi/knative.go), bundled into `PatchResourceLimits` or a new sibling.

### 3.6 Container indexing + name hardcoding (W-R2)

`[0]` everywhere: `current.Selector.MatchExpressions[0].Values[0]`, `containers[0]`, `containerPolicies[0]`. Hardcoded `"user-container"` in `MonitorKsvcResources`. Online stage will hit this if any ksvc has sidecars (Istio, OpenTelemetry collector) and the user-container isn't index 0.

**Estimated cost**: ~50 LOC scattered across `binary_search.go`, `algorithm/utils.go`, `watcher.go`, `kubeapi/knative.go`. Worth a single audit pass + helper.

### 3.7 Probe URL validation (W-R3)

`spec.durationPolicy.apiCondition.url` is unvalidated; `triggerHttp` GETs whatever string is there — in-cluster SSRF surface. Online stage will add metric scrapes (more URLs); fix the validator once and reuse.

**Estimated cost**: ~20 LOC + a CRD `pattern` regex for HTTP/HTTPS-only and same-namespace constraint.

### 3.8 Cleanup: dead `internal/nodes/` package

Created during a refactor attempt but never wired. Either delete or migrate `discoverCandidateNodes` to call `nodes.Candidates`. Pure code-organization, no behavior change.

**Estimated cost**: 5 minutes (delete) or ~30 LOC (migrate + shrink the watcher file to a thin wrapper).

### 3.9 Update CLAUDE.md

Still describes flat `StartingCpu`/`RunningCpu` and the upstream-boost-CR-as-default flow. Drift will accumulate across more sessions; fix in place.

**Estimated cost**: ~30 minutes of editing.

### 3.10 Add a happy-path integration test (W-R5)

Zero tests today. Phase 2's filter logic (§5 worked example below) is pure-logic and especially easy to table-test with `client-go/kubernetes/fake`. A single test covering "one Nimbus, two-node cluster, both nodes saturate, status reflects perNode" gives the online refactor a tripwire for regressions.

**Estimated cost**: ~150 LOC for one fake-cluster test. CI can come later.

---

## 4. Phase 3 — per-pod, per-node CPU via in-place resize

> **Status: designed only.** This was originally planned as a mutating admission webhook (~500 LOC of TLS/cert/MWC scaffolding); the design was switched to in-place pod resize once it became clear the cluster runs k8s 1.33 (`InPlacePodVerticalScaling` is GA-default). Pure pod-watcher in `internal/watcher/`, no admission-time machinery.

### 4.1 Goal

Each pod gets the CPU limit calibrated for the node it actually scheduled to. Phase 2's max-collapse over-provisions every node except the slowest; Phase 3 fixes that.

### 4.2 Two-phase resize state machine

The pod-watcher reacts to `Modified` events on Pods:

| Pod-condition transition | Resize CPU to | Notes |
|---|---|---|
| `nodeName: "" → nodeName = N` (post-bind, pre-Ready) | `current.PerNodeResults[N].StartingCpu` | Cold-start window. Resize races kubelet container start; if late, the container ran on the template's max-starting (no harm — over-provisioned, not under). |
| `Ready: False → True` | `current.PerNodeResults[N].RunningCpu` | Post-startup; safe to drop to running value. Wait for `Ready=True` (not just bind) to avoid fighting the boost reverter or NIMBUS's own starting-phase resize. |

### 4.3 Required ksvc-template change

In-place resize requires `resizePolicy: NotRequired` on the container — see §3.5 above. NIMBUS injects this once at apply time.

### 4.4 Choice: keep `kube-startup-cpu-boost` or replace it?

Per-node *starting* CPU has the same problem the original mutating-webhook plan had: the upstream boost's webhook fires at admission (pre-bind) and can't pick a per-node value. Two paths:

| | Option A: keep upstream boost | Option B: NIMBUS owns the lifecycle |
|---|---|---|
| Boost CR at cold start | `MaxStartingCpu` (today's value) | Removed; ksvc template default = `MaxStartingCpu` |
| Pod-watcher resize at bind | None | Down to `PerNodeResults[node].StartingCpu` |
| Pod-watcher resize at Ready | Down to `PerNodeResults[node].RunningCpu` | Same |
| Per-node win | Running phase only | Both phases |
| External dependency | `kube-startup-cpu-boost` chart | None |
| Cold-start race | None (boost is admission-time) | Watcher latency vs. kubelet container start (typically <200ms; fine for multi-second ML cold starts) |

**Recommendation**: Option B, contingent on testing the resize-vs-cold-start race. If the race produces measurably worse cold starts than Phase 2 + max, fall back to Option A.

### 4.5 Watcher shape

Mirrors `ksvc_watcher.go`:

```go
func (nw *NimbusWatcher) StartPodWatcher(ctx context.Context) {
    w, _ := CLIENTSET.CoreV1().Pods(metav1.NamespaceAll).Watch(ctx, metav1.ListOptions{})
    defer w.Stop()
    for {
        select {
        case <-ctx.Done(): return
        case ev, ok := <-w.ResultChan():
            if !ok || ev.Type != watch.Modified { continue }
            pod, _ := ev.Object.(*corev1.Pod)
            nw.maybeResizePod(ctx, pod)
        }
    }
}
```

`maybeResizePod`:
1. Filter on `serving.knative.dev/service` label matching a Nimbus in `nw.completed` (same logic as `ksvc_watcher.go`).
2. State transitions per the table in §4.2.
3. Idempotency: gate every resize on "current CPU limit ≠ desired" so a flood of `Modified` events doesn't spam patches.

### 4.6 Resize patch shape

```go
patch := []map[string]interface{}{{
    "op":    "replace",
    "path":  "/spec/containers/0/resources/limits/cpu",
    "value": targetCpu,
}}
_, _ = CLIENTSET.CoreV1().Pods(ns).Patch(
    ctx, podName, types.JSONPatchType, payloadBytes,
    metav1.PatchOptions{}, "resize", // the resize subresource
)
```

The `"resize"` subresource string is the k8s 1.33 entrypoint that bypasses the immutability check on `resources.limits`.

### 4.7 Open questions before coding Phase 3

- **Race-window measurement**: empirical — for the user's YOLO workload, how often does the starting-phase resize land before vs. after container init finishes? Drives Option A vs. B in §4.4.
- **Memory**: NIMBUS doesn't measure memory. If a follow-up adds it, the same two-phase resize works (memory is also resizable in 1.33), but memory-shrink can fail on some CRI runtimes. Test before committing to a memory-shrink path.
- **Per-pod observability**: should NIMBUS record which pod got which CPU value (`status.observedPods[<pod>] = {node, cpu, resizedAt}`)? Useful for debugging, adds churn to `.status`. Defer until there's a use case.

---

## 5. Reference — discovery filter behavior

This material remains in the doc because it's load-bearing for understanding `computeCandidateNodes` and useful for the thesis writeup.

### 5.1 The four-filter pipeline

A node enters the candidate list iff it passes **all four** predicates:

```
candidate(node) ⇔
    Ready ∧ ¬cordoned                                      (F1)
  ∧ ⋀(k,v)∈nodeSelector  node.labels[k] = v                (F2)
  ∧ ⋁ term∈nodeAffinity.terms  ⋀ expr∈term  matches(...)   (F3)
  ∧ ⋀ taint∈node.taints  ⋁ tol∈tolerations  tolerates(...) (F4)
```

| Filter | Internal logic |
|---|---|
| F2 `nodeSelector` | AND over all key/value pairs — every label must match |
| F3 `nodeAffinity.requiredDuringScheduling…` | OR over `nodeSelectorTerms` — any one term suffices |
| One `nodeSelectorTerm` (inside F3) | AND over `matchExpressions` — every expression must match |
| F4 `tolerations` vs `taints` | For each `NoSchedule`/`NoExecute` taint: ∃ a toleration that matches it |

### 5.2 Worked example — 4 nodes, all three constraints declared

| Node     | `zone`    | `gpu`   | `tier`       | Taints |
|----------|-----------|---------|--------------|--------|
| `node-a` | `us-west` | `true`  | `production` | `gpu-only:NoSchedule` |
| `node-b` | `us-west` | `false` | `production` | (none) |
| `node-c` | `us-east` | `true`  | `staging`    | `gpu-only:NoSchedule` |
| `node-d` | `us-west` | `true`  | `production` | `gpu-only:NoSchedule`, `node-role.kubernetes.io/control-plane:NoSchedule` |

ksvc spec.template.spec:

```yaml
nodeSelector:               # F2
  zone: us-west
  tier: production
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:    # F3
        - matchExpressions:
            - {key: gpu, operator: In, values: ["true"]}
tolerations:                # F4
  - {key: gpu-only, operator: Exists, effect: NoSchedule}
```

| Node     | F1 | F2 nodeSelector | F3 nodeAffinity | F4 tolerations | Result |
|----------|----|-----------------|-----------------|----------------|--------|
| `node-a` | ✓  | ✓ | ✓ | ✓ | **INCLUDED** |
| `node-b` | ✓  | ✓ | ✗ (gpu=false) | — | excluded at F3 |
| `node-c` | ✓  | ✗ (zone+tier) | — | — | excluded at F2 |
| `node-d` | ✓  | ✓ | ✓ | ✗ (control-plane taint not tolerated) | excluded at F4 |

Final candidates: `[node-a]`.

### 5.3 Common misreadings

- **`nodeSelector` and `nodeAffinity` AND-compose**, not OR. To express OR over hostnames, use multiple `nodeSelectorTerms` (which OR among themselves) and skip `nodeSelector`.
- **Tolerations are permissive, not selective**. Adding a toleration only makes a tainted node *eligible*; it doesn't bias toward tainted nodes.
- **Empty filters mean "no constraint", not "no nodes"**. A ksvc with no `nodeSelector`/`affinity`/`tolerations` matches every Ready+schedulable node.
- **`nodeName: <node>`** (separate PodSpec field) hard-binds the pod and bypasses the scheduler. Our filters don't read it, so a ksvc using `nodeName` will get a misleadingly broad candidate list. Use `nodeSelector: {kubernetes.io/hostname: <node>}` instead.

### 5.4 Known limitations of `computeCandidateNodes`

| Ignored factor | Why kube-scheduler cares | Why we don't | Direction of error |
|---|---|---|---|
| `PreferNoSchedule` taints | Soft hint to avoid the node | Soft, not hard | None for correctness |
| `preferredDuringScheduling…` nodeAffinity | Soft preference | Soft, not hard | None for correctness |
| `matchFields` on nodeAffinity terms | Affinity by node *field* | Rare in app code; would need to walk metadata not labels | False positives (we'd list nodes the scheduler rejects) |
| `Gt` / `Lt` operators in matchExpressions | Numeric comparison on labels | Almost never used in practice | False negatives (we'd exclude valid nodes) |
| `podAffinity` / `podAntiAffinity` | Co-located/anti-co-located pods | Requires snapshotting all pods at discovery time | False positives |
| `topologySpreadConstraints` | Spread across zones/nodes | Irrelevant under `maxScale=1` | None at probe time |
| Volume affinity (PVC zone-bound) | PVC must be attachable | Knative services are stateless by convention | Doesn't apply |
| Custom `schedulerName` | Non-default scheduler logic | We assume the default | False positives |
| `nodeName` direct binding | Hard-bound pod | We don't read this field | False positives (probe still lands on bound node) |
| Resource fit (CPU/memory headroom) | Dynamic node state | Belongs at probe time, not discovery | Probe fails → existing retry/timeout machinery handles |
| Pod priority / preemption | Eviction logic | Dynamic state | None for static eligibility |
| Stale node-lease/heartbeat | Dead kubelet still marked Ready | We trust `Ready=True` | Brief (~40s) false positives after a node dies |

**Summary**: false positives surface as visible probe failures and are caught by the existing `coldSampleWithStuckRecovery` machinery. False negatives just shrink the search scope. No silent correctness bugs.

### 5.5 Resolved decisions (record)

| Question | Choice |
|---|---|
| Discovery-helper placement | Method on `NimbusWatcher` in [`internal/watcher/candidate_nodes.go`](internal/watcher/candidate_nodes.go); the `internal/nodes/` package is dead code (see §3.8) |
| Affinity scope | `requiredDuringSchedulingIgnoredDuringExecution` only |
| Empty-match behavior | Error out with `ErrNoMatchingNodes`; caller logs + retries on next tick |
| Taint handling | Hard (`NoSchedule` + `NoExecute`) excluded if untolerated; soft (`PreferNoSchedule`) ignored |
| Pin mechanism | `nodeSelector` merge-patch (key `kubernetes.io/hostname`), AND-composing with user's existing constraints. Simpler than affinity-stash and safe because the candidate list was computed from those constraints upstream. |
| Per-node result shape | `NodeResult { StartingCpu, RunningCpu, StartingSaturated, RunningSaturated }`. Saturation booleans are runtime-only (`json:"-"`); recomputed from CPU emptiness on load. |
| Outer saturation | `NimbusEvent.AllSaturated` recomputed by `recomputeAllSaturated` after load and after the loop. Invariant: `AllSaturated ⇔ ∀node ∈ candidates: Starting∧RunningSaturated`. |
| Apply-time collapse | Max across `PerNodeResults` (slowest node sets cluster-wide floor). Phase 3 (§4) makes this moot per-pod. |
| Status schema | `status.perNode` only; no flat top-level aggregate. Aggregate is computed in-process at apply time, not persisted. |
| Inner skip | Per-node loop in `runMultiNodeSearch` skips a candidate iff `StartingSaturated && RunningSaturated`. Lets a partially completed Nimbus resume on the next reconcile without redoing saturated nodes. |
