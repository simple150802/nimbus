<p align="center">
  <img src="./images/logo.svg" alt="NIMBUS" width="180"/>
</p>

<h1 align="center">NIMBUS</h1>

<p align="center">
  <b>N</b>ative <b>I</b>terative <b>M</b>easurement for <b>B</b>oost &amp; <b>U</b>tilization <b>S</b>izing
</p>

<p align="center"><i>Two-phase CPU auto-profiling for Knative services.</i></p>

---

A Kubernetes controller that **automatically profiles** the optimal CPU limit for Knative services using a two-phase binary search. For every workload, on every candidate node, it discovers two values:

- a **starting-phase CPU** — the limit applied during cold start, programmed via a `StartupCPUBoost` CR;
- a **running-phase CPU** — the limit applied at steady state, written directly into the Knative service spec.

The search runs once per candidate node (any node the ksvc's `nodeSelector` + `nodeAffinity` + tolerations permit). Per-node results are persisted to `.status.perNode` so subsequent controller restarts skip the (expensive) search and re-apply the known-good values. At apply time the per-node values are collapsed to a single ksvc-wide CPU limit using the **max** strategy (slowest node's value as cluster-wide floor — no node starves at startup).


---

## Background — why this exists

[Google's `kube-startup-cpu-boost`](https://github.com/google/kube-startup-cpu-boost) is a Kubernetes controller that temporarily increases CPU requests/limits during a container's startup phase and reverts to a baseline once it's ready. It works, but two limitations are problematic for production-style workloads:

- **Hardcoded thresholds.** The boost CPU value is set by hand per workload; a bad guess costs latency or money.
- **Coarse end-of-startup signal.** The reset to baseline is driven mostly by a fixed wait, which lags the moment the app is actually ready.

NIMBUS improves on both:

- **Active-polling `durationPolicy`** — instead of a passive timer, the controller polls a user-defined HTTP endpoint until the body matches an expected substring. The boost is reverted the instant the application is ready. ([upstream PR](https://github.com/kenphunggg/kube-startup-cpu-boost.git))
- **Dynamic Range Strategy via binary search** — the user gives a `[min, max]` CPU range; the controller probes that range with a binary search and picks the smallest value that still serves requests within an acceptable response-time band, separately for the startup and running phases.

---

## How the binary search works

Per Nimbus, the controller:

1. **Discovers candidate nodes** — intersects Ready+!unschedulable nodes with the target ksvc's `nodeSelector`, required `nodeAffinity`, and tolerations vs. taints.
2. **Loops over candidates**: for each node not already saturated in `.status.perNode`, pin the ksvc to that node (one extra `kubernetes.io/hostname` key on `spec.template.spec.nodeSelector`, AND-composing with the user's existing constraints) and run the two-phase search.
3. **Unpins** the ksvc once at the end (single deferred unpin — each iteration's pin overwrites the previous).
4. **Persists** every per-node converged pair to `.status.perNode`, then **collapses** to `MaxStartingCpu` / `MaxRunningCpu` for the cluster-wide ksvc apply.

Each per-node search runs the same two phases:

### Starting phase

- `low := spec.resourcePolicy.containerPolicies[0].resourceRange.limits.min`
- `high := spec.resourcePolicy.containerPolicies[0].resourceRange.limits.max`
- Each probe creates a `StartupCPUBoost` CR at the candidate CPU, force-deletes the existing pod, waits for scale-to-zero, triggers `coldSamples` cold-start requests against `durationPolicy.apiCondition.url`, and reports the **p95** of those response times. (The shipping code currently reports the *mean*; switching to p95 is a planned tightening of the SLO claim — see [algorithm.md §3.3](algorithm.md#33-the-probes--n-samples-p95-aggregation).)
- The midpoint is treated as the new `high` (or `low` if response time improved by > 10%) until `high - low ≤ 100m`.
- The converged `high` is written to `.status.perNode[<node>].startingCpu`.

### Running phase

- `low := spec.min - 50m`, `high := <this-node>.startingCpu`.
- Probes use the warm path: patch the ksvc to the candidate CPU, wait for the new revision to scale to zero, trigger `warmSamples` warm requests, and report the **p95** of those response times. (Currently mean, see note above.)
- Same convergence rule (`high - low ≤ 100m`).
- The converged `high` is written to `.status.perNode[<node>].runningCpu`.

A single ksvc is pinned via `autoscaling.knative.dev/max-scale=1` for the duration of the search so every measurement reflects exactly one pod; the cap is removed when the search returns. Two-layer saturation: a node is "saturated" when both phases finished for it; the Nimbus is "complete" when every candidate is saturated. Partial completion (e.g., search aborted mid-loop) is persisted, and the next reconcile resumes from the unsaturated set.

The flow chart for the algorithm:

![Flow chart](./images/algorithm.png)

---

## Custom Resource: `Nimbus`

| | |
|---|---|
| Group | `lazyken.io` |
| Version | `v1alpha1` |
| Kind | `Nimbus` |
| Plural | `nimbuses` |
| Short name | `nb` |
| Scope | Namespaced |

Two ready-to-apply sample manifests live under [config/](config/):

- [`config/my-boost-export.yaml`](config/my-boost-export.yaml) — runs the binary search and writes per-probe samples to `../results/<timestamp>/`
- [`config/my-boost-preload.yaml`](config/my-boost-preload.yaml) — loads a previously-exported run and skips the binary search

The shared shape:

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
      values: ["measure-yolo"]
spec:
  # measurement is optional. Defaults: coldSamples=1, warmSamples=10.
  # Cold samples are expensive — each forces a scale-to-zero before a
  # fresh cold start. For an SLO-claim experiment use coldSamples ≥ 5
  # and warmSamples ≥ 20 (see algorithm.md §3.7).
  measurement:
    coldSamples: 1
    warmSamples: 10
  resourcePolicy:
    containerPolicies:
      - containerName: user-container
        resourceRange:
          limits:
            min: "200m"
            max: "2"
  durationPolicy:
    apiCondition:
      url: "http://measure-yolo.serverless.svc.cluster.local/status"
      response: "READY"
```

### Field reference

- **`selector.matchExpressions`** — the controller currently only honors the first entry, only the `In` operator, and treats `values` as a **literal list of Knative service names**, not as a label selector in the general Kubernetes sense. The CRD enforces this with an `enum: [In]` constraint.
- **`spec.resourcePolicy.containerPolicies[0]`** — only the first container policy is read; `containerName` must match the user-container name inside the Knative pod (typically `user-container`).
- **`spec.resourcePolicy.containerPolicies[0].resourceRange.limits.{min,max}`** — search bounds. Standard Kubernetes CPU quantities (`"200m"`, `"2"`, `"1500m"`).
- **`spec.measurement.coldSamples`** — number of cold-start samples per probe (default 1, minimum 1). Each sample = one full scale-to-zero + cold-start cycle, ~30–90 s wall-clock. **Recommended:** 5 (target: 10) for an SLO-claim experiment; the bare floor is 3, below which "p95" is just the single measurement and any kubelet hiccup defines the curve. See [algorithm.md §3.7](algorithm.md#37-how-many-samples-per-probe).
- **`spec.measurement.warmSamples`** — number of warm-request samples per probe (default 10). Each sample is ~10 s. **Recommended:** 20 (target: 30) — at `N < 20`, "p95" mathematically reduces to "max of N", so this is the smallest `N` at which the percentile is meaningful.
- **`spec.durationPolicy.apiCondition.{url,response}`** — only `apiCondition` is implemented (the upstream project's `fixed` and `podCondition` policies are not honored). The controller GETs `url` until the body contains `response`.
- **`status.perNode`** — a map keyed by node name. Each entry holds:
  - `startingCpu` / `runningCpu` — the converged values used at apply time (collapsed to the cluster-wide max).
  - `coldRtSamples` / `warmRtSamples` — every `(cpu, rtMillis)` probe point the binary search visited for that node, ordered by ascending CPU. Consumed by the online stage to fit `RT(x)` without re-probing; safe to ignore for now.

  The map populates as the per-node loop progresses; once every candidate node has both `startingCpu` and `runningCpu` non-empty, the Nimbus is "complete" and a controller restart re-applies the values without re-probing. Clear with `kubectl patch ... --subresource=status` to force a re-search.

`kubectl get nimbus` shows the count of profiled nodes:

```
NAME        NODES                                                    AGE
boost-001   map[master:map[startingCpu:706m runningCpu:219m] ...]    2m
```

(The raw map renders awkwardly in the printer column. Use `-o yaml` for a clean read of `.status.perNode`.)

### Exporting per-probe samples

Optional: set `spec.export.dir` to have the controller write raw response-time samples to disk as the binary search runs.

```yaml
spec:
  export:
    dir: "./results"                # or an absolute path like "/var/nimbus/results"
  resourcePolicy: { ... }
  durationPolicy: { ... }
```

Output tree (one timestamped subdirectory per search run):

```
results/2026-05-08T14-23-11/
├── meta.json                         # Nimbus spec snapshot + candidate nodes
├── master/
│   ├── cold/{300m.csv, 650m.csv, 706m.csv}   # one row per raw sample
│   ├── warm/{250m.csv, 219m.csv}
│   └── result.json                   # converged {startingCpu, runningCpu}
└── worker/
    └── ...
```

Each `<cpu>.csv` is `index,rt_millis` only — three rows when `measurement.coldSamples: 3`, appended monotonically if the search re-probes the same CPU. Node / phase / cpu are encoded in the path, not the row. Load into pandas:

```python
import pandas as pd, glob
df = pd.concat(pd.read_csv(f).assign(file=f) for f in glob.glob("results/*/master/cold/*.csv"))
```

Omit `spec.export` to disable export entirely (legacy behaviour, no files written).

---

## Quick start (local cluster)

Prerequisites:
- A Kubernetes cluster with [Knative Serving](https://knative.dev) installed.
- [`kube-startup-cpu-boost`](https://github.com/google/kube-startup-cpu-boost) deployed (the upstream mutating webhook is what NIMBUS drives).
- The boost controller configured to **preserve CPU limits on revert** — see step 0 below.
- A target Knative service in scope of the `selector.matchExpressions[0].values` list, exposing the `apiCondition.url` endpoint.

```bash
# 0. Ensure kube-startup-cpu-boost preserves the CPU limit when the boost
# expires (instead of stripping it entirely). NIMBUS writes the
# running-phase value into the ksvc spec; if the boost controller removes
# limits on revert, NIMBUS's running-phase value is lost and pods come
# back unbounded after the boost window.
kubectl set env deployment/kube-startup-cpu-boost-controller-manager \
  -n kube-startup-cpu-boost-system \
  REMOVE_LIMITS=false

# 1. Install the Nimbus CRD
kubectl apply -f config/crd.yaml

# 2. Deploy a sample app (or your own) in the serverless namespace
kubectl apply -f config/sampleapp.yaml

# 3. Start the controller
go run ./cmd

# 4. In another terminal, apply a Nimbus that runs the binary search and
#    exports samples to ../results/<timestamp>/
kubectl apply -f config/my-boost-export.yaml

# 5. Watch the search progress in the controller terminal
# 6. When the search converges, per-node finalized values are visible:
kubectl get nimbus -n serverless -o yaml | grep -A20 'status:'

# 7. (Optional) Re-use the exported run: delete the Nimbus, point the
#    preload manifest's loadFromDir at the resulting results/<ts>/, and
#    re-apply. The controller skips the binary search and applies the
#    loaded CPU values directly.
kubectl delete nimbus boost-001 -n serverless
$EDITOR config/my-boost-preload.yaml         # set loadFromDir
kubectl apply -f config/my-boost-preload.yaml
```

To re-run the search after completion, clear the status:

```bash
kubectl patch nimbus boost-001 -n serverless --subresource=status --type=merge \
  -p '{"status":{"perNode":null}}'
```

To force a re-search of just one node (after, say, a kernel upgrade):

```bash
# Re-running just `worker`:
kubectl patch nimbus boost-001 -n serverless --subresource=status --type=merge \
  -p '{"status":{"perNode":{"worker":{"startingCpu":"","runningCpu":""}}}}'
```

---

## Reading the logs

The controller writes colored, tagged output via `api/logging`:

| Tag | Meaning |
|---|---|
| `[COLD] probe starting — cpu=...` | A new starting-phase candidate is being measured. |
| `[COLD] curl GET <url>` | A cold-start probe HTTP request. |
| `[COLD] sample n/N: cpu=... rt=...` | One sample completed. |
| `[COLD] cool-down 10s for endpoint propagation` | Forced delay between samples so kubelet/Knative endpoints catch up. |
| `[COLD] probe stuck after 2m0s, deleting pod and retrying` | Auto-recovery: a cold probe exceeded its deadline; the pod is force-deleted and the sample is retried (up to 3 attempts). |
| `[WARM] ...` | Mirror of the above for the running-phase probes. |
| `[set] ksvc cpu limit -> ns=... ksvc=... cpu=...` | A ksvc spec mutation. |
| `[set] ksvc maxScale=1 -> ...` / `[set] ksvc maxScale=<unset> -> ...` | The 1-pod cap is set at the start of the search and removed when it returns. |
| `[set] StartupCPUBoost -> ...` | The intermediate boost CR was upserted. |
| `[<phase>][monitor] pod=... cpuLimit=...` | Monitor goroutine reports the user-container's effective limit, only during the trigger window. |
| `[nodes] <ns>/<name> candidates: [...]` | Discovered candidate-node list at the top of each search. |
| `[nodes] BinarySearch on node=<n>` | Start of one per-node iteration in the multi-node loop. |
| `[nodes] node=<n> already saturated ... — skipping` | Inner-skip: this node was completed in a previous run; loop moves on. |
| `[node=<n>] starting CPU: ... | running CPU: ...` | Per-node summary at end of `BinarySearch`. |
| `[set] ksvc pin -> ns=... ksvc=... node=...` | `PinKsvcToNode` adds the per-loop hostname constraint. |
| `[set] ksvc unpin -> ns=... ksvc=...` | `UnpinKsvc` (deferred, fires once after the loop). |
| `Skipping binary search — all <N> candidate node(s) saturated: ns/name` | Outer fast path: every candidate already has saturated values. |
| `Nimbus status persisted: ns/name perNode=<N> entries` | `WriteNimbusStatus` after a slow-path completion (full or partial). |
| `Propagating RunningCPU to new ksvc: ... -> <max>` | A late-created ksvc matched a completed Nimbus's selector and was patched to `MaxRunningCpu`. |

---

## Limitations / scope

- Only **Knative services** are supported as targets (the selector key is hardcoded to `serving.knative.dev/service` semantics).
- A Nimbus CRD currently programs **one ksvc, one container policy** — the controller indexes `[0]` for both `matchExpressions` and `containerPolicies` and assumes the container is named `user-container`.
- The `durationPolicy` only supports `apiCondition`. The `fixed` and `podCondition` policies present in the upstream `StartupCPUBoost` CRD are not honored here.
- Cold probes can take a long time at low CPU values; the search auto-aborts an individual probe after 2 minutes and retries up to 3 times. Warm probes have no per-sample retry budget yet — a flaky network kills the whole search.
- **Multi-node measurement** lands per-node values into `.status.perNode`, but at apply time the per-node values are collapsed to the cluster-wide max for a single ksvc CPU limit. Per-pod, per-node CPU via k8s 1.33's `/resize` subresource is designed but not implemented; see [online_plan.md](online_plan.md) §3.4 + Phase F'.
- No metrics / tracing — observability is print-only at the moment.
- The intended environment is a **local dev cluster**; the project doesn't ship RBAC manifests, a populated Dockerfile, or CI.
