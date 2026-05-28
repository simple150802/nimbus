<p align="center">
  <img src="./images/logo.svg" alt="NIMBUS" width="180"/>
</p>

<h1 align="center">NIMBUS</h1>

<p align="center">
  <b>N</b>ative <b>I</b>terative <b>M</b>easurement for <b>B</b>oost &amp; <b>U</b>tilization <b>S</b>izing
</p>

<p align="center"><i>Two-phase CPU auto-profiling for Knative services.</i></p>

---

NIMBUS is a Kubernetes controller that **auto-profiles** the optimal CPU limit for a Knative service. You declare a **node pool** (`spec.placement.nodeSelector`) and a single CPU **ceiling** (`spec.resourcePolicy.containerPolicies[0].cpuBudget`); NIMBUS measures **one representative node** from that pool with a binary search that bisects **downward** from that ceiling — there is no operator-supplied lower bound — and derives two values:

- **starting-phase CPU** — used during cold start, programmed via a `StartupCPUBoost` CR.
- **running-phase CPU** — used at steady state, written into the Knative service spec.

The representative's values are persisted to `.status.perNode` and treated as the profile for the whole pool (the pool is assumed homogeneous — see [Node-pool placement contract](#node-pool-placement-contract)). On a controller restart the search is skipped and the recorded values are re-applied. At apply time NIMBUS writes the pool `nodeSelector` plus the running-phase CPU onto **every** ksvc it manages, and records the per-ksvc outcome in `.status.applied`.

---

## What this is (and isn't) — handoff context

**You're being given two repos that work together:**

```
ken/
├── nimbus/                  ← THIS repo. The auto-profiling controller.
│   └── (this README)
└── kube-startup-cpu-boost/   ← The NIMBUS-specific fork of the upstream Google
                                project. NIMBUS depends on it for the actual
                                CPU-boost runtime mutation. Has its own README
                                + build.sh.
                                Clone from:
                                https://github.com/kenphunggg/kube-startup-cpu-boost.git
```

**What works today** (implementable, deployable, runnable):

- Offline auto-profiling: node-pool discovery (`spec.placement.nodeSelector`) → binary search on one representative node, samples persisted to `.status.perNode`.
- Node-pool placement: NIMBUS overwrites every managed ksvc's `nodeSelector` with the pool selector and re-asserts it on Nimbus edits (`watch.Modified`) and late-created ksvcs.
- Atomic apply: `nodeSelector` + running CPU written in one patch per ksvc; per-ksvc outcome recorded to `.status.applied`.
- Per-ksvc `StartupCPUBoost` CR creation (labeled `nimbus.io/owned-by=<nimbus>`).
- Sample export to disk (`spec.export.dir` → `results/<timestamp>/` with raw CSVs + `meta.json` + per-node `result.json`).
- Sample preload from a previous export (`spec.preMeasured.loadFromDir`) — skips re-measurement.
- Per-Nimbus online opt-out (`spec.online.enabled: false`) — offline still runs (profile + bootstrap apply + boost CR), polling waterfall and `/decide` short-circuit. Useful for measurement-only workflows and thesis A/B baselines.

**What works partially** (in-process, runnable; the Knative-side fork is the missing piece):

- The **online** decision engine: synchronous `POST /decide` HTTP endpoint, 3-tier waterfall (`c_opt` pool-wide → `c_min` pool-wide → `best_fit` pinned), EWMA burst detector, 70 %-per-node headroom budget, polling self-healer that re-asserts the same waterfall every 2 s.
- The steady-state warm CPU is locked to `c_opt_warm` on every tier — the thesis scope is cold-start optimization, so only the boost CR's cpu varies per tier.
- **What's still missing**: the Knative KPA fork at `scaler.go::applyScale` that synchronously calls `/decide` on `0→1` cold-start. Until it lands, the polling loop is the only consumer of the waterfall and the burst detector has no event source (mode stays NORMAL).

**What's out of scope** (and won't be added):

- Production routing, multi-tenancy, in-place CPU upgrade via `/resize`, automatic `containerConcurrency` mutation.

---

## Prerequisites

| Requirement | How to satisfy |
|---|---|
| Kubernetes ≥ 1.27 with `InPlacePodVerticalScaling` feature gate (≥ 1.33 has it on by default) | `kubectl version` to check; see your k8s installer's docs for the feature gate. |
| Knative Serving installed | https://knative.dev/docs/install/. Pin the Knative version and keep it **frozen** across experiment runs. |
| At least one node labeled with the pool key NIMBUS will manage | `kubectl label node <worker> nimbus.io/pool=serverless` for every node you want NIMBUS to schedule controlled ksvcs onto. **Label only nodes of equal computing power into one pool** — NIMBUS measures one representative and applies it pool-wide (see "Node-pool placement contract"). The label key/value must match `spec.placement.nodeSelector` on the Nimbus CR (Step 5). |
| A container registry the cluster can pull from | Examples: a public Docker Hub repo, a local kind-registry, an internal GCR. The bundled build script targets `docker.io/lazyken/kube-startup-cpu-boost:dev` — change this if you don't own that namespace. |
| Go ≥ 1.22 on your dev machine | `go version` to check. NIMBUS is run via `go run ./cmd` for dev. |
| `kubectl` configured against the target cluster | `kubectl get nodes` should list your workers. |
| `docker` on your dev machine + login to the registry | For building the kube-startup-cpu-boost fork image. |
| `jq` (optional, for the `headroom_cap.sh` test fixture) | Most distros: `apt install jq` / `brew install jq`. |

---

## End-to-end deployment

Run these once, in order. Each step prints what success looks like.

### Step 0 — Prepare the cluster

```bash
# Confirm cluster is reachable and has worker nodes
kubectl get nodes -o wide
# → expect at least 1 worker with InPlacePodVerticalScaling enabled

# Confirm Knative is installed and Ready
kubectl get pods -n knative-serving
# → expect activator, autoscaler, controller, etc. all Running

# (Optional, recommended) — create the target namespace where ksvcs will live
kubectl create namespace serverless

# Label every node that should be part of the NIMBUS-managed pool. The
# Nimbus CR in Step 5 declares this exact key/value in
# spec.placement.nodeSelector; offline measures one representative match
# and apply writes the same selector onto every controlled ksvc.
kubectl label node worker-1 nimbus.io/pool=serverless
kubectl label node worker-2 nimbus.io/pool=serverless   # repeat per pool node
kubectl get nodes -l nimbus.io/pool=serverless
# → expect at least one node listed; the alphabetically-first becomes the
#   measurement representative.
```

### Step 1 — Build and deploy the kube-startup-cpu-boost fork

NIMBUS depends on this controller to actually mutate pod CPU at boost time and revert at Ready. **Don't use the upstream Google release — use the fork** because:

- The fork's `apicondition.go` watches Pod ksvc-revision-status changes via Pod watch (needed for NIMBUS's revert-event flow).
- The fork builds with an image tag that matches NIMBUS's expectations (`apiCondition` is wired to NIMBUS-built URLs).

```bash
# Clone the NIMBUS-specific fork next to this repo (skip if you already have it).
# Do NOT substitute the upstream Google release — see the two reasons above.
git clone https://github.com/kenphunggg/kube-startup-cpu-boost.git ../kube-startup-cpu-boost
cd ../kube-startup-cpu-boost

# Build + push the controller image (edit DOCKER_REPO in build.sh first
# if you don't own docker.io/lazyken/*).
./build.sh

# Apply the controller (its build.sh applies the right manifests).
# If you skipped build.sh, manifests are in kube-startup-cpu-boost/config/.
kubectl get pods -n kube-startup-cpu-boost-system
# → expect kube-startup-cpu-boost-controller-manager-* Running

# CRITICAL: make sure the boost controller preserves CPU limits on revert.
# NIMBUS writes runningCpu into ksvc.spec; if the controller strips limits
# when reverting, that value is lost. Set:
kubectl set env deployment/kube-startup-cpu-boost-controller-manager \
  -n kube-startup-cpu-boost-system \
  REMOVE_LIMITS=false
kubectl rollout status -n kube-startup-cpu-boost-system \
  deployment/kube-startup-cpu-boost-controller-manager
```

See [`../kube-startup-cpu-boost/README.md`](../kube-startup-cpu-boost/README.md) for the fork's own fork-specific notes.

### Step 2 — Install the Nimbus CRD

```bash
cd ../nimbus
kubectl apply -f config/crd.yaml
kubectl get crd nimbuses.lazyken.io
# → NAME                       CREATED AT
#   nimbuses.lazyken.io        <timestamp>
```

### Step 3 — Deploy a sample workload (or your own ksvcs)

The bundled samples are `measure-yolo-{001,002,003}` — three pre-created ksvcs in the `serverless` namespace that NIMBUS will profile and (eventually) place. They must have `containerConcurrency: 1`, `max-scale: "1"`, `min-scale: "0"` so each pod serves one request at a time.

```bash
kubectl apply -f config/sampleapp_001.yaml
kubectl apply -f config/sampleapp_002.yaml
kubectl apply -f config/sampleapp_003.yaml

kubectl get ksvc -n serverless
# → measure-yolo-001 ... True   <url>
#   measure-yolo-002 ... True   <url>
#   measure-yolo-003 ... True   <url>

# If you swap apps, update both the manifest and the apiCondition
# path/response in the Nimbus CR to match the new app's HTTP contract.
```

### Step 4 — Start the NIMBUS controller (offline phase)

```bash
# Foreground — logs to stdout
go run ./cmd

# You'll see:
#   Hello from LAZYken! Starting NIMBUS...
#   Watcher started: Listening for Nimbus events across ALL namespaces...
#   Worker started: Direct linked-list monitoring with RLock...
```

In production this would be a Deployment; for thesis dev it runs from a terminal.

### Step 5 — Apply a Nimbus CR (kicks off the binary search)

```bash
kubectl apply -f config/my-boost-export.yaml

# In the controller terminal, expect a sequence like:
#   [nodes] serverless/boost-001 pool=[worker-1 worker-2] representative=worker-1 selector=map[nimbus.io/pool:serverless]
#   [nodes] BinarySearch on representative=worker-1
#   runBinarySearch gating on metric=p95, cpuBudget=1000m, slo=1500ms
#   [COLD] probe starting — cpu=300m ns=serverless
#   [COLD] sample 1/3: cpu=300m rt=4.521s
#   ...
#   [COLD] phase complete on node=worker-1: c_opt=700m (p95=1180ms) | c_min=300m (slo=1500ms) | samples=7
#   [WARM] phase complete on node=worker-1: c_opt=200m (p95=42ms) | c_min=150m (slo=50ms) | samples=8
#   [node=worker-1] cold c_opt=700m c_min=300m | warm c_opt=200m c_min=150m
#   [set] ksvc spec -> ns=serverless ksvc=measure-yolo-001 selector=map[nimbus.io/pool:serverless] cpu=200m
#   [set] StartupCPUBoost -> ns=serverless name=boost-001-measure-yolo-001 ksvc=measure-yolo-001 limits=700m
#   Nimbus apply status persisted: serverless/boost-001 applied=3 entries

# When the search finishes, inspect per-node converged values:
kubectl get nimbus boost-001 -n serverless -o yaml
```

A typical run with `coldSamples=3, warmSamples=10` on a single worker takes **5–15 minutes** wall-clock per ksvc (cold phase dominates). Three ksvcs in series: ~30–60 minutes.

### Step 6 — Reuse a finished run (skip re-measurement)

Once `results/<timestamp>/` exists from Step 5, you can reload it instead of remeasuring:

```bash
# Edit config/my-boost-preload.yaml — set spec.preMeasured.loadFromDir
# to the actual timestamp directory:
$EDITOR config/my-boost-preload.yaml

kubectl delete nimbus boost-001 -n serverless   # clear .status; preMeasured needs a clean slate
kubectl apply -f config/my-boost-preload.yaml

# Controller log will say:
#   Skipping binary search — all <N> candidate node(s) saturated: serverless/boost-001
```

Preload is intentionally strict for thesis experiments: use the same `spec.metric`
and `spec.acceptableResponseTime` values that produced the exported run. The
loaded `cMinStarting` / `cMinRunning` values are SLO-derived snapshots, so changing
the SLO in the preload manifest makes those imported `c_min` values semantically
stale. Re-measure when changing the SLO or metric.

---

## Verifying things work

Five quick checks after Step 5 completes:

```bash
# 1. Per-node profile is populated
kubectl get nimbus boost-001 -n serverless -o jsonpath='{.status.perNode}' | jq

# 2. One StartupCPUBoost CR per target ksvc (named <nimbus>-<ksvc>)
kubectl get startupcpuboost -n serverless --show-labels
# → boost-001-measure-yolo-001  ... nimbus.io/owned-by=boost-001,nimbus.io/ksvc=measure-yolo-001
#   boost-001-measure-yolo-002  ...
#   boost-001-measure-yolo-003  ...

# 3. ksvc spec now carries the running-phase CPU NIMBUS chose
kubectl get ksvc measure-yolo-001 -n serverless -o jsonpath='{.spec.template.spec.containers[0].resources.limits.cpu}'
# → e.g. 200m

# 4. Every controlled ksvc carries the Nimbus pool selector — not the
# original selector the user may have written into the manifest.
kubectl get ksvc -n serverless -o jsonpath='{range .items[*]}{.metadata.name}{": "}{.spec.template.spec.nodeSelector}{"\n"}{end}'
# → measure-yolo-001: map[nimbus.io/pool:serverless]
#   measure-yolo-002: map[nimbus.io/pool:serverless]
#   measure-yolo-003: map[nimbus.io/pool:serverless]

# 5. .status.applied gives the per-ksvc apply outcome from the most
# recent reconcile (one entry per ksvc in selector.values[]). An
# applyError field on a row means the apiserver rejected one of
# NIMBUS's patches for that ksvc.
kubectl get nimbus boost-001 -n serverless -o jsonpath='{.status.applied}' | jq
# → { "measure-yolo-001": { "nodeSelector": {"nimbus.io/pool":"serverless"},
#                           "startingCpu": "700m", "runningCpu": "200m" },
#     ... }
```

If 1 is missing: the search didn't finish, or the controller crashed — check stdout.
If 2 is missing: the boost-CR upsert failed — check controller log for `[set] StartupCPUBoost` errors.
If 3 / 4 is missing: `ApplyKsvcSpec` failed — usually a missing RBAC permission or the ksvc was deleted. Cross-check 5 for the per-ksvc error.
If 5 is empty: the Nimbus didn't reach the apply loop. Re-check the worker log for `Skipping binary search …` (fast path) or `STEP PROCESSING:` (slow path) before that point.

---

## Node-pool placement contract

NIMBUS treats the Nimbus CR as the **single source of truth** for where its controlled ksvcs run. The mechanism has four moving parts:

1. **Operator labels nodes** with the pool key/value (`kubectl label node … nimbus.io/pool=serverless`).
2. **Nimbus CR declares the same selector** in `spec.placement.nodeSelector`. This field is required — the CRD rejects an apply that omits it.
3. **Discovery resolves the pool** to Ready+schedulable nodes, sorts by name, and picks the first match as the *measurement representative*. The whole pool gets logged for traceability; only one node is profiled.
4. **Apply writes the selector verbatim onto every ksvc** in `selector.matchExpressions[0].values`. The patch is JSON-patch `add` on `/spec/template/spec/nodeSelector`, so **any user-set keys on the ksvc are overwritten**. Combined with the CPU patch in one apiserver call (`ApplyKsvcSpec`), the ksvc never sees an intermediate state.

> ⚠️ **Pool-homogeneity contract — your responsibility.** A pool must contain only nodes of **equal computing power** (same CPU model/generation and allocatable). NIMBUS measures *one* representative node (step 3) and applies that CPU profile to the whole pool — it does **not** verify the nodes are actually equivalent. If you label mismatched nodes into one pool, the unmeasured ones are silently mis-sized: when the representative is the fastest node, slower nodes miss the SLO. Label only equal-compute nodes into a pool and confirm this before applying. If you have two hardware classes, give them two pool labels and two Nimbus CRs.

Three additional behaviours support the "Nimbus owns nodeSelector" invariant:

| Trigger | Behaviour |
|---|---|
| You `kubectl apply` a **new ksvc** whose name is already in `values[]` of a completed Nimbus | The `StartKsvcWatcher` patches the new ksvc with both the pool selector and the running CPU. |
| You edit the Nimbus (add a ksvc to `values[]`, rotate `placement.nodeSelector`, change CPU budget) | `watch.Modified` is routed through `Upsert` — the Nimbus is removed from the completed set, fields are replaced in place, and the next worker tick re-applies against the new spec. |
| `ApplyKsvcSpec` returns an error for some ksvc | The boost CR for that ksvc is **not** written; the error string is persisted under `.status.applied[ksvc].applyError` so operators can grep for failures. |

**Hostname pin during measurement.** The representative ksvc is briefly pinned to `kubernetes.io/hostname=<representative>` via merge-patch so the binary search lands on a deterministic node. The pin is removed by a deferred `UnpinKsvc` before the apply loop runs, leaving only the pool selector. No controlled ksvc carries `kubernetes.io/hostname` after a successful tick.

**What's *not* enforced.** NIMBUS only watches ksvc `Added` events. If a third party `kubectl edit`s a controlled ksvc's `nodeSelector` after NIMBUS applied it, NIMBUS won't notice — the invariant is "true after every NIMBUS reconcile", not "continuously enforced".

---

## Custom resource

| | |
|---|---|
| Group / Version | `lazyken.io/v1alpha1` |
| Kind | `Nimbus` (`nb`) |
| Scope | Namespaced |

Sample manifests in [config/](config/):

- [`config/my-boost-export.yaml`](config/my-boost-export.yaml) — runs the binary search; writes raw samples to `../results/<timestamp>/`.
- [`config/my-boost-preload.yaml`](config/my-boost-preload.yaml) — loads a previously-exported run; skips the binary search.

Minimal shape:

```yaml
apiVersion: lazyken.io/v1alpha1
kind: Nimbus
metadata:
  name: boost-001
  namespace: serverless
selector:
  matchExpressions:
    - key: serving.knative.dev/service
      operator: In
      values: ["measure-yolo-001", "measure-yolo-002", "measure-yolo-003"]
spec:
  # Required. Nimbus-owned node pool. Must match the label applied to the
  # pool nodes in Step 0. Offline measures one Ready+schedulable match;
  # apply writes this selector verbatim onto every ksvc in values[] above,
  # overwriting any user-set keys.
  placement:
    nodeSelector:
      nimbus.io/pool: serverless
  metric: p95              # avg | p90 | p95 (default p95) — gate metric for convergence
  resourcePolicy:
    containerPolicies:
      - containerName: user-container
        # Economic ceiling — the max CPU per ksvc NIMBUS will ever assign.
        # The binary search anchors here and bisects DOWNWARD to find c_opt
        # (latency-plateau edge) and c_min (smallest CPU meeting the SLO).
        # No operator-supplied lower bound: the algorithm's resolution
        # threshold (100m) + a hardcoded safety floor (50m) terminate it.
        # Standard k8s CPU quantity ("2", "2000m", "1.5").
        cpuBudget: "2000m"
  durationPolicy:
    # Cold-phase gate. The boost CR's apiCondition is built from this path,
    # so the upstream kube-startup-cpu-boost webhook polls the same URL.
    # Gate passes when the body contains `response`.
    coldApiCondition:
      path: "/status"
      response: "READY"
    # Warm-phase gate. Must hit a real workload endpoint so the binary
    # search measures CPU-sensitive inference time, not flag-read latency.
    # Gate passes when actual code == statusCode AND (if bodyContains is
    # set) the body contains the substring.
    warmApiCondition:
      path: "/detect/local"
      statusCode: 200
      bodyContains: "\"success\":true"
  # Optional. Latency budget consumed by the online stage to derive c_min
  # from the offline sample lists. Offline profiling records it but does
  # not use it to steer binary search.
  # acceptableResponseTime:
  #   cold: 1500
  #   warm: 50
  # Optional. Defaults: coldSamples=1, warmSamples=10. For an SLO-grade run,
  # use coldSamples >= 5 and warmSamples >= 20 so p95 is meaningful.
  measurement:
    coldSamples: 3
    warmSamples: 10
  # Optional. Stream per-probe raw samples to disk.
  export:
    dir: "../results"
  # Optional. Load a previously-exported run instead of measuring.
  # preMeasured:
  #   loadFromDir: "../results/2026-05-15T10-56-11"
  # Optional. Per-Nimbus online opt-out. Default: online enabled.
  # When false, the polling reconciler skips this Nimbus and /decide
  # returns passthrough. Offline still measures + applies the boost CR.
  # online:
  #   enabled: false
```

The authoritative field reference is the CRD schema in [config/crd.yaml](config/crd.yaml) — every field has a description block there.

---

## Offline-only mode

A Nimbus carrying `spec.online.enabled: false` runs **offline only**: the binary search, profile persistence, and bootstrap apply (pool selector + `c_opt_warm` + `StartupCPUBoost` CR at `c_opt_cold`) all happen as usual, but the polling waterfall and `/decide` short-circuit. Useful for:

- **Measurement workflows** — capture the profile to `results/<timestamp>/`, then leave the ksvc alone (no every-2-s touch).
- **Thesis A/B baselines** — compare "measured + online-managed" vs "measured only" without forking the controller.
- **Quiet steady state** — once the profile is stable, avoid further reconcile noise.

To enable, set the flag in the manifest:

```yaml
spec:
  online:
    enabled: false
```

What changes when the flag is `false`:

| Path | Behaviour |
|---|---|
| Offline (binary search, `.status.perNode`, `.status.applied`, boost CR) | **unchanged** — runs as usual |
| Polling reconciler | **skips** this Nimbus (no `.status.online` write); logs `event=skip_offline_only` once on entry |
| `/decide` HTTP | feeds the burst detector (cluster-wide rate must stay accurate), then returns `passthrough`; KPA proceeds with the existing spec |
| Cold-start boost via `kube-startup-cpu-boost` | **works** — the boost CR offline wrote is still consumed by the webhook |
| Burst detector | still observes cold-starts on this Nimbus's ksvcs (so OTHER online-enabled Nimbuses' waterfalls react correctly) |

Verification:

```bash
# After applying with online.enabled=false:
# 1. status.perNode should still be populated by offline:
kubectl get nimbus boost-001 -n serverless -o jsonpath='{.status.perNode}' | jq
# 2. status.applied should record what offline wrote onto each ksvc:
kubectl get nimbus boost-001 -n serverless -o jsonpath='{.status.applied}' | jq
# 3. status.online should be ABSENT (the polling reconciler skips):
kubectl get nimbus boost-001 -n serverless -o jsonpath='{.status.online}'
# → (empty)
```

If you flip an already-online-enabled Nimbus to `online.enabled: false`, the previous `.status.online` rows linger (the reconciler stops writing but doesn't clear). Manual clear:

```bash
kubectl -n serverless patch nimbus boost-001 --subresource=status --type=merge \
  -p '{"status":{"online":null}}'
```

The default is `online.enabled: true`, so omitting the field (or omitting the whole `spec.online` block) keeps the full waterfall + `/decide` behaviour.

---

## Reset between runs

```bash
# Re-apply the CRD if you changed config/crd.yaml; otherwise status writes are pruned silently.
kubectl apply -f config/crd.yaml

# Wipe memoized per-node results so the next apply re-measures.
kubectl -n serverless patch nimbus boost-001 --subresource=status --type=merge \
  -p '{"status":{"perNode":null}}'

# Remove ksvc CPU floors so the boost webhook isn't capped from below.
for ksvc in measure-yolo-001 measure-yolo-002 measure-yolo-003; do
  kubectl -n serverless patch ksvc "$ksvc" --type=json \
    -p='[{"op":"remove","path":"/spec/template/spec/containers/0/resources/limits"}]'
done

# Remove any leftover hostname pin from an interrupted run.
for ksvc in measure-yolo-001 measure-yolo-002 measure-yolo-003; do
  kubectl -n serverless patch ksvc "$ksvc" --type=merge \
    -p='{"spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":null}}}}}'
done

# Drop NIMBUS-managed StartupCPUBoost CRs.
kubectl -n serverless delete startupcpuboost \
  -l app.kubernetes.io/managed-by=nimbus,nimbus.io/owned-by=boost-001 \
  --ignore-not-found

# (Optional) Clear the per-ksvc apply record so .status.applied reflects a fresh run.
kubectl -n serverless patch nimbus boost-001 --subresource=status --type=merge \
  -p '{"status":{"applied":null}}'
```

To force re-measurement of one specific node, clear that node only:

```bash
kubectl -n serverless patch nimbus boost-001 --subresource=status --type=merge \
  -p '{"status":{"perNode":{"worker":{"startingCpu":"","runningCpu":""}}}}'
```

---

## Reading the controller output

All output is colored, tagged stdout; no metrics or structured logs yet. Tags follow `[PHASE]` or `[action]` conventions. The lines you'll see most often:

| Tag | Meaning |
|---|---|
| `[nodes] <ns>/<name> pool=[...] representative=<n> selector=map[...]` | Discovered pool + chosen representative node. |
| `[nodes] BinarySearch on representative=<n>` | Start of measurement on the representative. |
| `runBinarySearch gating on metric=<m>, cpuBudget=..., slo=...ms` | Echo of `spec.metric`, cpuBudget, and SLO budget at the start of each per-phase bisect. |
| `[COLD] sample n/N: cpu=... rt=...` | Cold-start sample complete. |
| `[WARM] sample n/N: cpu=... rt=...` | Warm sample complete. |
| `[COLD] phase complete on node=...: c_opt=... (p95=...ms) \| c_min=... (slo=...ms) \| samples=N` | Cold phase converged — `c_opt` is the latency-plateau edge; `c_min` is the smallest probed CPU meeting the SLO (reads `unset (no slo)` / `infeasible (slo=...)` when applicable). |
| `[WARM] phase complete on node=...: c_opt=... (p95=...ms) \| c_min=... (slo=...ms) \| samples=N` | Warm-phase mirror. |
| `[node=...] cold c_opt=... c_min=... \| warm c_opt=... c_min=...` | Per-node digest after both phases. |
| `[set] ksvc pin -> ns=... ksvc=... node=...` | Measurement-time hostname pin (composes with the pool selector). |
| `[set] ksvc unpin -> ns=... ksvc=...` | Hostname pin removed after the search — only the pool selector remains. |
| `[set] ksvc nodeSelector -> ns=... ksvc=... selector=map[...]` | Pool selector written onto the measured ksvc before the search starts. |
| `[set] ksvc spec -> ns=... ksvc=... selector=map[...] cpu=...` | One-shot atomic apply: nodeSelector + requests.cpu + limits.cpu in a single JSON patch. Fires once per ksvc in `values[]` per tick. |
| `[set] StartupCPUBoost -> ...` | Boost CR upserted at apply time (one per ksvc). |
| `Nimbus apply status persisted: ns/name applied=N entries` | After the apply loop — N rows written to `.status.applied`. |
| `Propagating RunningCPU to new ksvc: ns/name -> <cpu>` | Late-arriving ksvc matched a completed Nimbus; pool selector + CPU re-applied via the side-channel. |
| `Skipping binary search — all <N> candidate node(s) saturated` | Status had every node already; no search ran. |

---

## Handoff checklist — what to make sure you have

| | |
|---|---|
| ✅ **Cluster access** | `kubectl config current-context` points at the right cluster. |
| ✅ **Container registry write** | `docker push <registry>/<image>:<tag>` succeeds. |
| ✅ **`kube-startup-cpu-boost` fork is in your account** | Clone [`https://github.com/kenphunggg/kube-startup-cpu-boost.git`](https://github.com/kenphunggg/kube-startup-cpu-boost.git). You'll need push access (or your own fork of it) to publish image updates. Do not substitute the upstream Google release. |
| ✅ **NIMBUS source** | This repo. |
| ✅ **Knative version pinned** | Record the Knative release tag you deploy and keep it fixed across experiment runs. |
| ✅ **Sample images for `measure-yolo-{001,002,003}`** | The ksvcs reference container images. Make sure those exist and are pullable. Check `config/sampleapp_00*.yaml`. |

---

## Scope and known limitations

- Only **Knative services** are supported targets; selector key is treated as a literal ksvc-name list (`operator: In` only).
- One ksvc, one container policy: the controller indexes `[0]` for both `matchExpressions` and `containerPolicies` and expects the container to be named `user-container`.
- `durationPolicy` supports `coldApiCondition` (body-substring gate) + `warmApiCondition` (statusCode + optional bodyContains). The upstream `fixed` and `podCondition` policies are not honored.
- Warm probes have no per-sample retry budget yet (cold probes do, with a 3-attempt stuck-recovery budget).
- Online assignment for the pre-created ksvc list is **designed but not implemented**. Current apply behavior still collapses per-node values to a single cluster-wide max.
- Per-pod in-place CPU updates via the k8s 1.33 `/resize` subresource are deferred.
- No metrics, no tests, no RBAC manifests, empty Dockerfile — the intended environment is a local dev cluster.
