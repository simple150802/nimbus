<p align="center">
  <img src="./images/logo.svg" alt="NIMBUS" width="180"/>
</p>

<h1 align="center">NIMBUS</h1>

<p align="center">
  <b>N</b>ative <b>I</b>terative <b>M</b>easurement for <b>B</b>oost &amp; <b>U</b>tilization <b>S</b>izing
</p>

<p align="center"><i>Two-phase CPU auto-profiling for Knative services.</i></p>

---

NIMBUS is a Kubernetes controller that **auto-profiles** the optimal CPU limit for a Knative service. It runs a binary search per candidate node over a `[min, max]` CPU range and derives two values per node:

- **starting-phase CPU** — used during cold start, programmed via a `StartupCPUBoost` CR.
- **running-phase CPU** — used at steady state, written into the Knative service spec.

Per-node values are persisted to `.status.perNode`; on a controller restart the search is skipped and the recorded values are re-applied. The cluster-wide ksvc limit is the **max** of per-node values (so the slowest node still meets the latency target).

---

## Custom resource

| | |
|---|---|
| Group / Version | `lazyken.io/v1alpha1` |
| Kind | `Nimbus` (`nb`) |
| Scope | Namespaced |

Sample manifests in [config/](config/):

- [`config/my-boost-export.yaml`](config/my-boost-export.yaml) — runs the binary search; optionally writes raw samples to `../results/<timestamp>/`.
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
      values: ["measure-yolo"]
spec:
  metric: p95              # avg | p90 | p95 (default p95) — gate metric for convergence
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
```

The authoritative field reference is the CRD schema in [config/crd.yaml](config/crd.yaml) — every field has a description block there.

---

## Quick start (local cluster)

Prerequisites:

- A Kubernetes cluster with [Knative Serving](https://knative.dev) installed.
- [`kube-startup-cpu-boost`](https://github.com/google/kube-startup-cpu-boost) deployed.
- The boost controller patched to **preserve CPU limits on revert** ([fork](https://github.com/kenphunggg/kube-startup-cpu-boost.git)). Without this, NIMBUS's running-phase CPU is stripped from the ksvc when the boost expires.
- A target Knative service in the namespace, with `apiCondition.url` reachable from inside the cluster.

```bash
# 1. Install the Nimbus CRD
kubectl apply -f config/crd.yaml

# 2. Deploy a sample app (or your own) in the namespace
kubectl apply -f config/sampleapp_001.yaml

# 3. Start the controller (foreground; logs to stdout)
go run ./cmd

# 4. In another terminal, run the binary search
kubectl apply -f config/my-boost-export.yaml

# 5. Inspect per-node converged values once the search finishes
kubectl get nimbus boost-001 -n serverless -o yaml
```

To re-use a finished run instead of re-measuring, point `preMeasured.loadFromDir` at the produced `results/<timestamp>/` and apply [`config/my-boost-preload.yaml`](config/my-boost-preload.yaml).

## Reset between runs

```bash
# Re-apply the CRD if you changed config/crd.yaml; otherwise status writes are pruned silently.
kubectl apply -f config/crd.yaml

# Wipe memoized per-node results so the next apply re-measures.
kubectl -n serverless patch nimbus boost-001 --subresource=status --type=merge \
  -p '{"status":{"perNode":null}}'

# Remove the ksvc CPU floor so the boost webhook isn't capped from below.
kubectl -n serverless patch ksvc measure-yolo --type=json \
  -p='[{"op":"remove","path":"/spec/template/spec/containers/0/resources/limits"}]'

# Remove any leftover hostname pin from an interrupted run.
kubectl -n serverless patch ksvc measure-yolo --type=merge \
  -p='{"spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":null}}}}}'

# Drop the StartupCPUBoost CR.
kubectl -n serverless delete startupcpuboost boost-001 --ignore-not-found
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
| `[nodes] candidates: [...]` | Discovered candidate nodes for the ksvc. |
| `[nodes] BinarySearch on node=<n>` | Start of one per-node iteration. |
| `Binary search gating on metric=<m>` | Echo of `spec.metric` (or its default) at search start. |
| `[COLD] sample n/N: cpu=... rt=...` | Cold-start sample complete. |
| `[WARM] sample n/N: cpu=... rt=...` | Warm sample complete. |
| `Binary Search Complete! The optimal CPU limit is: ...` | One phase converged. |
| `[set] StartupCPUBoost -> ...` | Boost CR upserted at apply time. |
| `[set] ksvc cpu limit -> ...` | Running-phase CPU patched into the ksvc. |
| `Skipping binary search — all <N> candidate node(s) saturated` | Status had every node already; no search ran. |

---

## Scope and known limitations

- Only **Knative services** are supported targets; selector key is treated as a literal ksvc-name list (`operator: In` only).
- One ksvc, one container policy: the controller indexes `[0]` for both `matchExpressions` and `containerPolicies` and expects the container to be named `user-container`.
- `durationPolicy` only supports `apiCondition`. The upstream `fixed` and `podCondition` policies are not honored.
- Warm probes have no per-sample retry budget yet (cold probes do, with a 3-attempt stuck-recovery budget).
- Per-pod, per-node CPU via the k8s 1.33 `/resize` subresource is **designed but not implemented** — the controller collapses per-node values to a single cluster-wide max at apply time.
- No metrics, no tests, no RBAC manifests, empty Dockerfile — the intended environment is a local dev cluster.
