# NIMBUS — project notes & hard-won lessons

NIMBUS is a Kubernetes controller that auto-sizes CPU for Knative serverless
services. Two phases per Nimbus CR:

- **Offline** (`internal/watcher`, `api/algorithm`): binary-search the CPU-vs-RT
  curve for cold-start and warm steady-state, per representative node. Outputs
  `c_opt` (knee) and `c_min` (smallest CPU meeting the SLO) for each phase,
  written to `.status.perNode`.
- **Online** (`internal/online`): `/decide` RPC + polling reconciler run a
  3-tier waterfall (c_opt → c_min → best-fit) over live headroom.

Entry point: `cmd/main.go`. Workloads measured: YOLO, insightface (`face-track`),
and `cpuprobe` (SHA256, in `/home/ubuntu/cpuprobe`).

---

## The c_opt / c_min algorithm (current design)

Per phase: **find c_opt → find c_min → clamp c_opt ≥ c_min**.

- **c_opt** (`runBinarySearch`, both phases): knee bisect over `[0, cpuBudget]`,
  10% improvement gate, 30 samples/point, gate metric = p95 (`spec.metric`).
- **c_min cold** (`deriveMin`): reads ONLY the points c_opt already probed →
  smallest meeting SLO. Coarse (≥ cpuBudget/2) but adds zero probes. Cold is too
  expensive for a dedicated search (see cost asymmetry below).
- **c_min warm** (`searchCMinWarm`): dedicated downward bisect `[50m, cpuBudget]`
  on ONE in-place-resized pod, 5 capped samples/point. Accurate AND cheap.
- **clamp** (`clampOptToMin`): `c_opt = max(c_opt, c_min)`. The knee can land
  below the SLO floor when the curve declines < 10%/step; without the clamp
  c_opt violates the SLO and inverts the online tiers (Tier 1 cheaper AND worse
  than Tier 2).

---

## Hard-won lessons (DO NOT re-break these)

### Warm-phase measurement (the big saga)
1. **Each warm timed sample must run at the target CPU, not the boost CPU.**
   `kube-startup-cpu-boost` reverts pod CPU asynchronously on its own poll
   schedule; `containerConcurrency=1` means the warmup request blocks its polls,
   the pod scales to zero before revert, and waitForBoostRevert times out → all
   samples run at boost CPU → flat RT curve. **Fix: don't use boost for warm.**
   Roll the pod at `cpuCold` (= c_opt_cold) for fast init, wait `/status`=READY,
   then **in-place resize** to the target CPU.
2. **In-place pod resize REQUIRES the `resize` subresource.** A plain pod-spec
   JSON patch is rejected ("pod updates may not change fields other than ...").
   `kubeapi.PatchPodCpu` passes `"resize"` as the subresource. The cluster has
   `InPlacePodVerticalScaling` enabled (verified: `kubectl patch pod ...
   --subresource=resize`).
3. **Sample 1 is systematically ~2× slower on CPU-bound workloads.** After the
   resize, Go-runtime background goroutines (GC, connection pool) compete for the
   CFS quota on the first heavy request. **Fix: one discarded warmup `/detect/local`
   call after resize+settle, before timed samples.** (YOLO didn't show this in old
   data only because the old data was the flat boost-CPU bug, not real RT — YOLO
   IS CPU-bound once measured correctly.)
4. **`interWarmSampleSleep` must stay well below the autoscaling window (10s).**
   Cold uses `interColdSampleSleep` (10s for endpoint propagation); warm uses 2s
   so the pod doesn't scale to zero between samples and turn warm into cold.
13. **Warm per-request HTTP timeout must scale with the warm SLO**, not be a
    hardcoded 90s. Minute-scale workloads (LLM `/text2text`, 200 tokens ≈ 60 s at
    budget, ≈ 120 s at half budget) exceed 90 s at low CPU → every request times
    out client-side, `triggerHttpWithCodeBody` logs "pod not reachable, retrying"
    and the c_opt loop spins forever (looked like a hang at 1000m). Fix:
    `warmRequestTimeout(sloMillis) = max(90s, 2×SLO)`, passed into
    `triggerHttpWithCodeBody`; `warmReqCapped` uses its cap as the client timeout.
    Symptom signature: warm `2000m.csv` has data, the next bisect point (1000m)
    hangs with monitor spam + "pod not reachable, retrying".

### Cold-phase measurement
5. **Cold is ~40 min/probe-point** (30 samples × scale-to-zero ~80s each). A
   dedicated downward c_min search on cold adds hours → impractical. Cold c_min
   stays `deriveMin`. To speed cold up, lower `spec.measurement.coldSamples` to
   5–10; cold RT is consistent enough.
6. **Boost-controller poll race injects spurious ~8ms cold samples.** The active
   StartupCPUBoost CR polls the service URL during the cool-down sleep, which can
   scale up a pod that becomes READY before NIMBUS triggers. `probe_cold.go`
   re-checks/evicts pods after the sleep. p95 gate already shrugs these off.
7. **Cold-start RT is bimodal from OS page cache** (model on disk vs cached).
   Use p95 over enough samples; don't trust a single fast sample.

### Controller / watcher
8. **WriteNimbusStatus triggers a self watch.Modified → Upsert nils
   PerNodeResults.** Capture `startingMax`/`runningMax` BEFORE any status write,
   or the apply loop reads an empty map and patches `cpu=` (webhook rejects).
9. **`c_min`/`startingRt`/`runningRt` must be written to AND read back from
   `.status.perNode`** (`nimbus_status.go` + `multi_node.go`). They used to live
   only in RAM, so online lost Tier 2 after a restart.
10. **Re-measure = delete the CR (or clear `.status.perNode`), not just re-apply.**
    `loadPerNodeFromStatus` reloads saturated state and skips the search.

### Infra
11. **Workload manifests omit memory → Burstable QoS, OOM risk.** A namespace
    `LimitRange` (`config/namespace-serverless.yaml`) injects a memory default so
    pods are Guaranteed and the CPU study isn't perturbed by memory.
12. **face-track image needs `libgl1` + `libglib2.0-0`** (Debian Bookworm) or cv2
    import fails with `libGL.so.1: cannot open shared object file`.

### Verification
- `verify-probe.sh <ksvc> <ns> <cold_cpu> <warm_cpu> [samples]` reproduces a
  cold + warm probe manually to check NIMBUS's numbers. **Stop NIMBUS first** so
  the online controller doesn't overwrite the ksvc CPU mid-test.

---

## When debugging a measurement anomaly
- Always read the raw per-sample CSVs under `results/<ts>/.../{cold,warm}/<cpu>m.csv`
  AND check pod CPU (`kubectl get pod ... -o jsonpath='{...resources}'`) — most
  "weird" results were the pod running at a different CPU than the label.
- A flat RT curve across CPU levels = samples all ran at the same (boost) CPU.
- Sample 1 ≫ rest = cold start or goroutine backlog, not the workload.
