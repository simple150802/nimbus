<p align="center">
  <img src="./images/logo.svg" alt="NIMBUS" width="180"/>
</p>

<h1 align="center">NIMBUS</h1>

<p align="center">
  <b>N</b>ative <b>I</b>terative <b>M</b>easurement for <b>B</b>oost &amp; <b>U</b>tilization <b>S</b>izing
</p>

<p align="center"><i>Two-phase CPU auto-profiling for Knative services.</i></p>

---

A Kubernetes controller that **automatically profiles** the optimal CPU limit for Knative services using a two-phase binary search. For every workload it discovers two values:

- a **starting-phase CPU** — the limit applied during cold start, programmed via a `StartupCPUBoost` CR;
- a **running-phase CPU** — the limit applied at steady state, written directly into the Knative service spec.

Both values are persisted to the controller's CRD `.status` subresource so subsequent restarts of the controller skip the (expensive) search and just re-apply the known-good values.


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

The search runs in two phases per Nimbus CRD:

### Starting phase

- `low := spec.resourcePolicy.containerPolicies[0].resourceRange.limits.min`
- `high := spec.resourcePolicy.containerPolicies[0].resourceRange.limits.max`
- Each probe creates a `StartupCPUBoost` CR at the candidate CPU, force-deletes the existing pod, waits for scale-to-zero, and measures the cold-start response time of `durationPolicy.apiCondition.url`.
- The midpoint is treated as the new `high` (or `low` if response time improved by > 10%) until `high - low ≤ 100m`.
- The converged `high` becomes `status.startingCpu`.

### Running phase

- `low := spec.min - 50m`, `high := status.startingCpu`.
- Probes use the warm path: patch the ksvc to the candidate CPU, wait for the new revision to scale to zero, trigger N warm requests, average the response times.
- Same convergence rule (`high - low ≤ 100m`).
- The converged `high` becomes `status.runningCpu`.

A single ksvc is pinned via `autoscaling.knative.dev/max-scale=1` for the duration of the search so every measurement reflects exactly one pod; the cap is removed when the search returns.

The flow chart for the algorithm:

![Flow chart](./images/algorithm.png)

---

## Custom Resource: `Nimbus`

| | |
|---|---|
| Group | `lazyken.io` |
| Version | `v1alpha1` |
| Kind | `Nimbus` |
| Plural | `nimbus` |
| Short name | `car` |
| Scope | Namespaced |

Sample manifest (also in [config/my-boost.yaml](config/my-boost.yaml)):

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
  # fresh cold start.
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
- **`spec.measurement.coldSamples`** — number of cold-start samples per probe (default 1, minimum 1). Each sample = one full scale-to-zero + cold-start cycle.
- **`spec.measurement.warmSamples`** — number of warm-request samples per probe (default 10). Cheap.
- **`spec.durationPolicy.apiCondition.{url,response}`** — only `apiCondition` is implemented (the upstream project's `fixed` and `podCondition` policies are not honored). The controller GETs `url` until the body contains `response`.
- **`status.startingCpu`** / **`status.runningCpu`** — written by the controller after a successful search. Their presence acts as a memoization key: subsequent reconciles skip the search and re-apply these values. Clear them with `kubectl patch ... --subresource=status` to force a re-search.

`kubectl get nimbus` shows finalized values directly:

```
NAME        STARTING CPU   RUNNING CPU   AGE
boost-001   706m           219m          2m
```

---

## Quick start (local cluster)

Prerequisites:
- A Kubernetes cluster with [Knative Serving](https://knative.dev) installed.
- [`kube-startup-cpu-boost`](https://github.com/google/kube-startup-cpu-boost) deployed (the upstream mutating webhook is what NIMBUS drives).
- A target Knative service in scope of the `selector.matchExpressions[0].values` list, exposing the `apiCondition.url` endpoint.

```bash
# 1. Install the Nimbus CRD
kubectl apply -f config/crd.yaml

# 2. Deploy a sample app (or your own) in the serverless namespace
kubectl apply -f config/sampleapp.yaml

# 3. Start the controller
go run ./cmd

# 4. In another terminal, apply a Nimbus
kubectl apply -f config/my-boost.yaml

# 5. Watch the search progress in the controller terminal
# 6. When the search converges, finalized values are visible:
kubectl get nimbus -n serverless
```

To re-run the search after completion, clear the status:

```bash
kubectl patch nimbus boost-001 -n serverless --subresource=status --type=merge \
  -p '{"status":{"startingCpu":"","runningCpu":""}}'
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
| `Skipping binary search — Nimbus already completed: ...` | Fast path: `.status` already carries finalized values. |
| `Propagating RunningCPU to new ksvc: ...` | A late-created ksvc matched a completed Nimbus's selector and was patched to its `runningCpu`. |

---

## Limitations / scope

- Only **Knative services** are supported as targets (the selector key is hardcoded to `serving.knative.dev/service` semantics).
- A Nimbus CRD currently programs **one ksvc, one container policy** — the controller indexes `[0]` for both `matchExpressions` and `containerPolicies` and assumes the container is named `user-container`.
- The `durationPolicy` only supports `apiCondition`. The `fixed` and `podCondition` policies present in the upstream `StartupCPUBoost` CRD are not honored here.
- Cold probes can take a long time at low CPU values; the search auto-aborts an individual probe after 2 minutes and retries up to 3 times, then surfaces the failure so the worker can re-attempt the whole Nimbus on the next tick.
- No metrics / tracing — observability is print-only at the moment.
- The intended environment is a **local dev cluster**; the project doesn't ship RBAC manifests, a populated Dockerfile, or CI.
