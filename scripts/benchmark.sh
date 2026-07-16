#!/bin/bash
# benchmark.sh — Measure cold + warm p95 RT at increasing CPU levels per service.
#
# Option A measurement model (mirrors NIMBUS exactly — see api/algorithm):
#   • COLD: inject the target CPU via a StartupCPUBoost CR (the upstream webhook
#     mutates each fresh pod at creation), NOT by patching the ksvc spec. This is
#     what api/algorithm/probe_cold.go does (CreateStartupCPUBoost). The ksvc
#     spec is never touched by the cold phase.
#   • WARM: bring a pod up fast at WARM_INIT_CPU, then IN-PLACE RESIZE it to the
#     target CPU via the `resize` subresource (mirrors probe_warm.go). Boost is
#     deliberately NOT used for warm — the boost controller reverts CPU async and
#     the pod scales to zero first (the warm-phase saga in CLAUDE.md).
#
# WHY the rewrite: the old version patched the ksvc requests/limits per level.
# If ANY StartupCPUBoost CR (e.g. a leftover premeasured/NIMBUS boost) targeted
# the ksvc, the webhook overrode that patch and every pod cold-started at the
# boost CPU — so "100m" and "600m" both ran at the same CPU and the curve came
# out as noise (inverted). This version (a) uses the boost CR as the single CPU
# source of truth, (b) VERIFIES every pod's actual CPU and aborts on mismatch,
# and (c) refuses to run while a conflicting boost CR is present.
#
# Effect on stored values:
#   • .status.perNode (the saved sweep c_opt/c_min) is NEVER touched.
#   • The ksvc spec CPU is captured at start and restored on exit.
#   • A conflicting premeasured boost CR must be removed (PURGE_BOOSTS=1) to
#     measure correctly; re-apply preload afterwards to restore it (that only
#     recreates the boost CR + sets ksvc CPU — it does NOT re-run the sweep).
#
# IMPORTANT: stop NIMBUS before running, else the online controller re-patches
# the ksvc / boost CR mid-measurement (same rule as verify-probe.sh).
#
# Usage:
#   ./scripts/benchmark.sh
#   SERVICES="measure-yolo-001" PURGE_BOOSTS=1 ./scripts/benchmark.sh
#
# Output: results/benchmark_<timestamp>/

set -euo pipefail

# ─── Configuration (override via env) ────────────────────────────────────────
NAMESPACE="${NAMESPACE:-serverless}"
CONTAINER_NAME="${CONTAINER_NAME:-user-container}"   # Knative user container name
WARM_SAMPLES="${WARM_SAMPLES:-20}"
COLD_SAMPLES="${COLD_SAMPLES:-20}"      # bimodal cold RT (page cache) needs ≥20 for stable p95
KNEE_THRESHOLD="${KNEE_THRESHOLD:-5}"   # percent; improvement < 5% → on plateau
KNEE_CONFIRM="${KNEE_CONFIRM:-5}"       # extra steps after knee before stopping
CPU_START="${CPU_START:-50}"            # milli-CPU, first probe point
CPU_STEP="${CPU_STEP:-50}"              # milli-CPU increment per step
CPU_MAX="${CPU_MAX:-2000}"              # hard ceiling; stop the sweep here even if the knee
                                        # never confirms (e.g. warm keeps skipping) so the
                                        # loop can't run forever
WARM_INIT_CPU="${WARM_INIT_CPU:-1000m}" # CPU the warm pod initialises at before in-place resize
COLD_METHOD="${COLD_METHOD:-boost}"     # cold measurement: boost (StartupCPUBoost CR, mirrors
                                        # NIMBUS) or patch (direct ksvc CPU, no boost webhook
                                        # overhead — matches true cold-start; use for fast apps
                                        # like JVM/Go where boost's ~fixed overhead dominates)
COLD_BASE_CPU="${COLD_BASE_CPU:-10m}"   # ksvc base CPU during cold probes (boost method only); MUST be ≤ every cold
                                        # target so the boost only ever raises UP to the target
                                        # (kube-startup-cpu-boost never lowers below the base — a
                                        # higher base left by the warm phase would pin the pod there)
COLD_PATH="${COLD_PATH:-/status}"       # readiness path (cold gate + boost poll URL)
COLD_RESPONSE="${COLD_RESPONSE:-READY}" # readiness body substring
# MAX_*_TIMEOUT, when set, OVERRIDE the per-service timeout fields below (empty =
# use the entry's value). Lets `MAX_WARM_TIMEOUT=300 ...` raise the cap for LLM.
COLD_TIMEOUT_OVERRIDE="${MAX_COLD_TIMEOUT:-}"  # seconds per cold-start attempt
WARM_TIMEOUT_OVERRIDE="${MAX_WARM_TIMEOUT:-}"  # seconds per warm request
INTER_WARM_SLEEP="${INTER_WARM_SLEEP:-2}"    # seconds between warm samples / cgroup settle
INTER_COLD_SLEEP="${INTER_COLD_SLEEP:-10}"   # seconds between cold samples
SCALE_ZERO_WAIT="${SCALE_ZERO_WAIT:-180}"    # max seconds to wait for scale-to-zero
                                             # (warm patches a new revision whose pod must
                                             # idle out the scale-to-zero grace; 120s can be short)
INFEASIBLE_RATIO="${INFEASIBLE_RATIO:-50}"   # % of timeouts → mark level infeasible
BOOST_APIVERSION="${BOOST_APIVERSION:-autoscaling.x-k8s.io/v1alpha1}"
PURGE_BOOSTS="${PURGE_BOOSTS:-0}"       # 1 = delete conflicting boost CRs (else abort service)
RESULTS_DIR="${RESULTS_DIR:-./results/benchmark_$(date +%Y%m%dT%H%M%S)}"

# ─── Service definitions ──────────────────────────────────────────────────────
# Format: "ksvc|warm_path|warm_body_contains|warm_timeout_s|cold_timeout_s[|cold_path|cold_response]"
# cold_path / cold_response are OPTIONAL — default to COLD_PATH (/status) and
# COLD_RESPONSE (READY). LLM needs its own (/loading-stats, ready) so it carries them.
declare -a DEFAULT_SERVICES=(
    "io-probe-001|/detect/local|\"success\":true|30|60"
    "sha256-001|/detect/local|\"success\":true|60|120"
    "insignface-001|/detect/local|\"success\":true|60|120"
    "measure-yolo-001|/detect/local|\"success\":true|60|120"
    "jvm-probe-001|/detect/local|\"success\":true|60|120"
    "measure-llm-001|/text2text?prompt=hi|reply|300|300|/loading-stats|ready"
)

# Allow SERVICES env var to filter (space-separated ksvc names)
if [[ -n "${SERVICES:-}" ]]; then
    declare -a ACTIVE_SERVICES=()
    for entry in "${DEFAULT_SERVICES[@]}"; do
        ksvc="${entry%%|*}"
        for wanted in $SERVICES; do
            [[ "$ksvc" == "$wanted" ]] && ACTIVE_SERVICES+=("$entry") && break
        done
    done
else
    ACTIVE_SERVICES=("${DEFAULT_SERVICES[@]}")
fi

# ─── Helpers ─────────────────────────────────────────────────────────────────
log()  { echo "[$(date '+%H:%M:%S')] $*"; }
info() { echo "[$(date '+%H:%M:%S')] INFO  $*"; }
warn() { echo "[$(date '+%H:%M:%S')] WARN  $*" >&2; }
die()  { echo "[$(date '+%H:%M:%S')] FATAL $*" >&2; exit 1; }

# Normalise a k8s CPU quantity to integer millicores: "500m"->500, "1"->1000.
to_milli() {
    local v=$1
    if [[ "$v" == *m ]]; then echo "${v%m}"; else awk -v x="$v" 'BEGIN{printf "%d", x*1000}'; fi
}

bench_boost_name() { echo "nimbus-bench-$1"; }

# ─── State restored on exit ───────────────────────────────────────────────────
# ORIG_CPU[ksvc] = the ksvc spec CPU captured before we touched it. cleanup()
# restores it and removes our bench boost CR so the cluster is left as found.
declare -A ORIG_CPU
cleanup() {
    local ksvc
    for ksvc in "${!ORIG_CPU[@]}"; do
        if [[ -n "${ORIG_CPU[$ksvc]}" ]]; then
            patch_ksvc_cpu "$ksvc" "${ORIG_CPU[$ksvc]}" && \
                info "restored ksvc $ksvc → ${ORIG_CPU[$ksvc]}"
        fi
        kubectl delete startupcpuboost "$(bench_boost_name "$ksvc")" \
            -n "$NAMESPACE" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT

# ─── CPU control (Option A) ───────────────────────────────────────────────────

# apply_bench_boost — upsert our StartupCPUBoost CR so every FRESH pod for this
# ksvc is mutated to ${cpu_milli}m at creation (cold-start CPU). Distinct name
# (nimbus-bench-*) so it never collides with a NIMBUS/premeasured boost CR.
apply_bench_boost() {
    local ksvc=$1 cpu_milli=$2 cold_path=${3:-$COLD_PATH} cold_response=${4:-$COLD_RESPONSE}
    local url="http://${ksvc}.${NAMESPACE}.svc.cluster.local${cold_path}"
    # Structure mirrors config/kube-startup-cpu-boost.yaml exactly: `selector`
    # is a TOP-LEVEL field (sibling of spec), NOT spec.selector. Getting this
    # wrong makes the CRD reject the apply. Failure is surfaced (not swallowed)
    # so a schema error can't silently kill the run.
    local err
    if ! err=$(kubectl apply -f - 2>&1 <<EOF
apiVersion: ${BOOST_APIVERSION}
kind: StartupCPUBoost
metadata:
  name: $(bench_boost_name "$ksvc")
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/managed-by: nimbus-benchmark
selector:
  matchExpressions:
    - key: serving.knative.dev/service
      operator: In
      values: ["${ksvc}"]
spec:
  resourcePolicy:
    containerPolicies:
      - containerName: ${CONTAINER_NAME}
        fixedResources:
          requests: "${cpu_milli}m"
          limits: "${cpu_milli}m"
  durationPolicy:
    apiCondition:
      url: "${url}"
      response: "${cold_response}"
EOF
    ); then
        die "$ksvc: kubectl apply StartupCPUBoost @ ${cpu_milli}m failed: ${err}"
    fi
}

delete_bench_boost() {
    kubectl delete startupcpuboost "$(bench_boost_name "$1")" \
        -n "$NAMESPACE" >/dev/null 2>&1 || true
}

# patch_ksvc_cpu — set the ksvc spec requests==limits==cpu (cpu carries a unit,
# e.g. "1000m"). Used only for warm fast-init and for restore-on-exit.
patch_ksvc_cpu() {
    local ksvc=$1 cpu=$2
    kubectl patch ksvc "$ksvc" -n "$NAMESPACE" --type=json -p \
        "[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/limits/cpu\",\"value\":\"${cpu}\"},
          {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/requests/cpu\",\"value\":\"${cpu}\"}]" \
        >/dev/null 2>&1
}

capture_ksvc_cpu() {
    kubectl get ksvc "$1" -n "$NAMESPACE" \
        -o jsonpath='{.spec.template.spec.containers[0].resources.requests.cpu}' 2>/dev/null
}

# serving_pod — name of the pod that ACTUALLY serves traffic: a pod on the ksvc's
# latest-ready revision. Using this instead of `.items[0]` (which sorts by name
# and can pick a leftover OLD-revision pod) is what makes resize/verify target the
# right pod when a service transitions revisions without cleanly scaling to zero.
serving_pod() {
    local ksvc=$1 rev p
    rev=$(kubectl get ksvc "$ksvc" -n "$NAMESPACE" -o jsonpath='{.status.latestReadyRevisionName}' 2>/dev/null)
    if [[ -n "$rev" ]]; then
        p=$(kubectl get pod -n "$NAMESPACE" \
            -l "serving.knative.dev/service=$ksvc,serving.knative.dev/revision=$rev" \
            -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    fi
    [[ -z "$p" ]] && p=$(kubectl get pod -n "$NAMESPACE" -l "serving.knative.dev/service=$ksvc" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    echo "$p"
}

# resize_pod_cpu — in-place resize the SERVING pod to ${cpu_milli}m via the
# `resize` subresource (no new revision, no scale-to-zero). Mirrors PatchPodCpu.
resize_pod_cpu() {
    local ksvc=$1 cpu_milli=$2 pod
    pod=$(serving_pod "$ksvc")
    [[ -z "$pod" ]] && return 1
    kubectl patch pod "$pod" -n "$NAMESPACE" --subresource resize --type=json -p \
        "[{\"op\":\"replace\",\"path\":\"/spec/containers/0/resources/requests/cpu\",\"value\":\"${cpu_milli}m\"},
          {\"op\":\"replace\",\"path\":\"/spec/containers/0/resources/limits/cpu\",\"value\":\"${cpu_milli}m\"}]" \
        >/dev/null 2>&1
}

# verify_pod_cpu — read the live pod's ACTUAL request CPU and compare to target.
# Echoes the observed millicores; returns 0 on match, 1 on mismatch / no pod.
# This is the guard the old script lacked: it catches "pod ran at a different
# CPU than the label" — the single most common cause of bogus measurements.
verify_pod_cpu() {
    local ksvc=$1 want_milli=$2 got
    got=$(kubectl get pod -n "$NAMESPACE" -l "serving.knative.dev/service=$ksvc" \
        -o jsonpath="{.items[0].spec.containers[?(@.name=='${CONTAINER_NAME}')].resources.requests.cpu}" 2>/dev/null)
    [[ -z "$got" ]] && { echo "NOPOD"; return 1; }
    local got_milli; got_milli=$(to_milli "$got")
    [[ "$got_milli" == "$want_milli" ]] && { echo "$got_milli"; return 0; }
    echo "$got_milli"; return 1
}

# pod_applied_cpu — the kubelet-APPLIED request cpu (status.containerStatuses),
# i.e. what the container is ACTUALLY running at. Differs from spec during an
# in-place resize: the pod spec updates instantly but the kubelet applies the new
# cgroup asynchronously, so a spec-based check can read "resized to 200m" while
# the container is still throttled at WARM_INIT. Empty if the cluster doesn't
# report per-container status resources.
pod_applied_cpu() {
    local pod; pod=$(serving_pod "$1")
    [[ -z "$pod" ]] && return
    kubectl get pod "$pod" -n "$NAMESPACE" \
        -o jsonpath="{.status.containerStatuses[?(@.name=='${CONTAINER_NAME}')].resources.requests.cpu}" 2>/dev/null
}

# wait_pod_cpu_applied — poll until the APPLIED cpu == target (the resize really
# took effect), up to ~25s. Echoes the last observed millicores; returns 0 on
# match. Falls back to the spec-based verify_pod_cpu if the cluster never reports
# applied resources. This is what makes a warm sample trustworthy: it guarantees
# the pod is truly throttled to the target CPU before timing, so a low-CPU level
# can't be measured fast because the resize hadn't landed yet.
wait_pod_cpu_applied() {
    local ksvc=$1 want_milli=$2 tries=0 got got_milli=""
    while (( tries < 25 )); do
        got=$(pod_applied_cpu "$ksvc")
        if [[ -n "$got" ]]; then
            got_milli=$(to_milli "$got")
            [[ "$got_milli" == "$want_milli" ]] && { echo "$got_milli"; return 0; }
        fi
        sleep 1; tries=$((tries + 1))
    done
    if [[ -z "$got" ]]; then          # applied never reported → fall back to spec
        verify_pod_cpu "$ksvc" "$want_milli"; return $?
    fi
    echo "${got_milli:-NOPOD}"; return 1
}

# detect_conflicting_boosts — list any NIMBUS/premeasured StartupCPUBoost CR
# targeting this ksvc (its webhook would override our CPU). Matches by the
# `nimbus.io/ksvc` label NIMBUS stamps on every boost CR it creates
# (api/kubeapi/startup_cpu_boost.go) — robust vs the CR's exact selector path.
# Our own bench CR carries `managed-by: nimbus-benchmark` (no nimbus.io/ksvc
# label) so it is never matched. Always returns 0 so `set -e` can't kill the run.
detect_conflicting_boosts() {
    local ksvc=$1 out
    out=$(kubectl get startupcpuboost -n "$NAMESPACE" -l "nimbus.io/ksvc=$ksvc" \
        -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null) || return 0
    [[ -n "$out" ]] && echo "$out"
    return 0
}

# preflight_service — verify the ksvc exists and kubectl is reachable before we
# touch anything. Prints a clear reason and returns 1 (skip) instead of letting
# set -e kill the run silently.
preflight_service() {
    local ksvc=$1
    if ! kubectl get ksvc "$ksvc" -n "$NAMESPACE" >/dev/null 2>&1; then
        warn "SKIP $ksvc: ksvc not found in namespace '$NAMESPACE' (check kubectl context / service name)"
        return 1
    fi
    return 0
}

force_delete_pods() {
    local ksvc=$1
    kubectl delete pod -n "$NAMESPACE" \
        -l "serving.knative.dev/service=$ksvc" \
        --grace-period=0 --force >/dev/null 2>&1 || true
}

# wait_ksvc_latest_ready — poll until the ksvc's latest CREATED revision is also
# its latest READY revision, i.e. the CPU we just patched is what Knative will
# route new traffic to. Without this, triggering right after a patch can scale up
# the PREVIOUS revision (e.g. the leftover cold base=10m), so a warm sample runs
# at the wrong CPU. Returns 0 once converged, 1 on timeout.
# Optional 2nd arg `pre` = the latestCreatedRevisionName captured BEFORE a patch.
# When given, require a NEW revision (created != pre) that is ready — otherwise
# there's a race: right after a patch the reconciler hasn't created the new
# revision yet, so latestCreated still equals the OLD revision (which is already
# ready), the check passes instantly, and triggering scales up the OLD CPU.
wait_ksvc_latest_ready() {
    local ksvc=$1 pre=${2:-} tries=0 created ready
    while (( tries < 45 )); do
        created=$(kubectl get ksvc "$ksvc" -n "$NAMESPACE" -o jsonpath='{.status.latestCreatedRevisionName}' 2>/dev/null)
        ready=$(kubectl get ksvc "$ksvc" -n "$NAMESPACE" -o jsonpath='{.status.latestReadyRevisionName}' 2>/dev/null)
        if [[ -n "$created" && "$created" == "$ready" && ( -z "$pre" || "$created" != "$pre" ) ]]; then
            return 0
        fi
        sleep 2; tries=$((tries + 1))
    done
    return 1
}

wait_scale_to_zero() {
    local ksvc=$1
    local waited=0
    while [[ $waited -lt $SCALE_ZERO_WAIT ]]; do
        local count
        count=$(kubectl get pod -n "$NAMESPACE" \
            -l "serving.knative.dev/service=$ksvc" \
            --no-headers 2>/dev/null | wc -l)
        [[ "$count" -eq 0 ]] && return 0
        sleep 2; waited=$((waited + 2))
    done
    warn "scale-to-zero timeout for $ksvc after ${SCALE_ZERO_WAIT}s"
    return 1
}

pod_count() {
    kubectl get pod -n "$NAMESPACE" -l "serving.knative.dev/service=$1" \
        --no-headers 2>/dev/null | wc -l
}

# wait_truly_cold — guarantee the ksvc is at ZERO pods immediately before the
# cold timer starts. The active StartupCPUBoost CR polls /status, which drives
# the boost controller to spin a pod up during the cool-down; if such a pod is
# already mid-cold-start when time_cold fires, the measurement is understated by
# several seconds. So: delete → wait-zero → brief settle → RE-CHECK, and if a
# racing pod reappeared, evict + re-cool-down and repeat (mirrors NIMBUS's
# post-cool-down guard in probe_cold.go). Returns once 0 pods is stable.
wait_truly_cold() {
    local ksvc=$1 tries=0
    while (( tries < 6 )); do
        force_delete_pods "$ksvc"
        wait_scale_to_zero "$ksvc" || true
        sleep "$INTER_COLD_SLEEP"           # cool-down (endpoint propagation)
        # settle window: give a racing boost-poll a chance to reveal a pod
        sleep 3
        if [[ "$(pod_count "$ksvc")" -eq 0 ]]; then
            return 0
        fi
        warn "  cold guard: pod reappeared during cool-down (boost-poll race) — evicting and re-cooling"
        tries=$((tries + 1))
    done
    return 0
}

# Time a single warm request. Echoes milliseconds or "-1" on timeout.
time_warm() {
    local url=$1 contains=$2 timeout_s=$3
    local t_start t_end body
    t_start=$(date +%s%3N)
    body=$(curl -sf --max-time "$timeout_s" "$url" 2>/dev/null) || { echo "-1"; return; }
    if echo "$body" | grep -qF "$contains"; then
        t_end=$(date +%s%3N)
        echo $((t_end - t_start))
    else
        echo "-1"
    fi
}

# Time a cold start: timer starts immediately (before pod exists), then retries
# /status. IMPORTANT: no `-f`. During scale-up the activator returns HTTP 503
# ("no healthy upstream"); with `-f` curl treats that as an error and sleeps 2s,
# which on a SHORT cold-start (JVM ~2s) adds ~2s = ~2x inflation. NIMBUS's Go
# client treats 503 as a normal response and polls immediately — so we do too:
# only a genuine connection error (curl exit != 0, e.g. connection refused or the
# --max-time timeout) sleeps; a 503/NOT_READY body loops immediately.
# Echoes total milliseconds from first attempt to "READY", or "-1" on timeout.
time_cold() {
    local status_url=$1 timeout_s=$2 cold_response=${3:-$COLD_RESPONSE}
    local t_start t_end body curl_exit
    t_start=$(date +%s%3N)
    local deadline=$((t_start + timeout_s * 1000))

    while true; do
        local now
        now=$(date +%s%3N)
        [[ $now -ge $deadline ]] && { echo "-1"; return; }

        body=$(curl -s --max-time 5 "$status_url" 2>/dev/null)
        curl_exit=$?

        if [[ $curl_exit -ne 0 ]]; then
            sleep 2   # genuine connection error / timeout — mirrors probeRetryInterval
        elif [[ "$body" == *"$cold_response"* ]]; then
            t_end=$(date +%s%3N)
            echo $((t_end - t_start))
            return
        fi
        # NOT_READY response → loop immediately (no sleep), same as Nimbus triggerHttp
    done
}

# Compute stats from a CSV of integers. Echoes: avg,p50,p90,p95,timeout_count,total
compute_stats() {
    local csv_file=$1
    awk -F',' 'NR>1 {
        val=$2+0
        total++
        if (val < 0) { timeouts++; next }
        vals[++n] = val
        sum += val
    }
    END {
        if (n == 0) {
            print "TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT," total "," total
            exit
        }
        for (i=1; i<=n; i++) for (j=i+1; j<=n; j++)
            if (vals[i]>vals[j]) { t=vals[i]; vals[i]=vals[j]; vals[j]=t }
        p50 = vals[int(n*0.50+0.5)]
        p90 = vals[int(n*0.90+0.5)]
        p95 = vals[int(n*0.95+0.5)]
        avg = int(sum/n)
        print avg "," p50 "," p90 "," p95 "," (timeouts+0) "," total
    }' "$csv_file"
}

# Returns 0 if knee detected (improvement from prev to curr < threshold%).
is_knee() {
    local prev=$1 curr=$2
    [[ "$prev" == "TIMEOUT" || "$curr" == "TIMEOUT" ]] && return 1
    awk -v p="$prev" -v c="$curr" -v t="$KNEE_THRESHOLD" \
        'BEGIN { exit (((p-c)/p*100) < t) ? 0 : 1 }'
}

# ─── Per-CPU-level probe ──────────────────────────────────────────────────────

# probe_cold — dispatch to the configured cold-measurement method:
#   boost (default) — inject cpu via a StartupCPUBoost CR (mirrors NIMBUS's
#                     getResptCold). Adds the boost webhook's admission overhead
#                     (~fixed seconds), negligible for long cold-starts but ~2x on
#                     fast ones (JVM/Go).
#   patch           — set the ksvc CPU DIRECTLY to cpu (no boost). The pod cold-
#                     starts at cpu from its own revision → no boost overhead →
#                     matches the TRUE cold-start (verify-probe) and NIMBUS's real
#                     numbers on fast apps. Use this for JVM/Go cross-checks.
probe_cold() {
    local ksvc=$1 cpu_milli=$2 status_url=$3 cold_timeout=$4 out_csv=$5
    local cold_path=${6:-$COLD_PATH} cold_response=${7:-$COLD_RESPONSE}

    echo "index,rt_millis" > "$out_csv"
    if [[ "$COLD_METHOD" == "patch" ]]; then
        probe_cold_patch "$ksvc" "$cpu_milli" "$status_url" "$cold_timeout" "$out_csv" "$cold_response"
    else
        probe_cold_boost "$ksvc" "$cpu_milli" "$status_url" "$cold_timeout" "$out_csv" "$cold_path" "$cold_response"
    fi
    return 0
}

# probe_cold_boost — inject cpu via the bench boost CR, then take COLD_SAMPLES cold
# starts, VERIFYING each pod actually runs at cpu. Aborts on the first sample if
# the pod's CPU ≠ target (boost not taking effect).
probe_cold_boost() {
    local ksvc=$1 cpu_milli=$2 status_url=$3 cold_timeout=$4 out_csv=$5 cold_path=$6 cold_response=$7

    # Reset the ksvc base LOW so the boost always raises UP to cpu_milli. Without
    # this, the warm phase leaves the ksvc at WARM_INIT_CPU (e.g. 1000m); the next
    # cold level with a lower target can't be lowered by the boost and the pod runs
    # at the stale base. Matches NIMBUS setting the ksvc to ~10m during cold.
    patch_ksvc_cpu "$ksvc" "$COLD_BASE_CPU"
    apply_bench_boost "$ksvc" "$cpu_milli" "$cold_path" "$cold_response"
    sleep 2   # let the boost webhook register before the first pod is created
    info "  bench boost CR @ ${cpu_milli}m applied (ksvc base=${COLD_BASE_CPU}, boost injects ${cpu_milli}m)"

    for ((i=1; i<=COLD_SAMPLES; i++)); do
        wait_truly_cold "$ksvc"
        local rt
        rt=$(time_cold "$status_url" "$cold_timeout" "$cold_response")
        if [[ "$rt" != "-1" ]]; then
            local got
            if ! got=$(verify_pod_cpu "$ksvc" "$cpu_milli"); then
                delete_bench_boost "$ksvc"
                die "$ksvc cold @ ${cpu_milli}m: pod ran at ${got}m ≠ ${cpu_milli}m — StartupCPUBoost not taking effect (conflicting boost CR? NIMBUS still running? wrong container name?)."
            fi
        fi
        echo "$i,$rt" >> "$out_csv"
        info "  cold sample $i/$COLD_SAMPLES: ${rt}ms (cpu=${cpu_milli}m verified, boost)"
    done
    delete_bench_boost "$ksvc"
}

# probe_cold_patch — measure cold-start with the ksvc set DIRECTLY to cpu (no
# boost). The pod cold-starts at cpu from its own revision, so there's no boost-
# webhook overhead — this matches verify-probe / NIMBUS's true cold-start, and is
# the right method for fast-starting apps. VERIFIES the serving pod's APPLIED cpu.
probe_cold_patch() {
    local ksvc=$1 cpu_milli=$2 status_url=$3 cold_timeout=$4 out_csv=$5 cold_response=$6

    # Point the ksvc at the target and wait for that revision to be the routed one
    # (else a fresh pod could come up on the previous revision at the wrong CPU).
    local pre; pre=$(kubectl get ksvc "$ksvc" -n "$NAMESPACE" -o jsonpath='{.status.latestCreatedRevisionName}' 2>/dev/null)
    patch_ksvc_cpu "$ksvc" "${cpu_milli}m"
    wait_ksvc_latest_ready "$ksvc" "$pre" || warn "  cold(patch) @ ${cpu_milli}m: new revision not ready in time (continuing)"
    info "  cold(patch): ksvc set to ${cpu_milli}m (direct, no boost)"

    for ((i=1; i<=COLD_SAMPLES; i++)); do
        # Fresh cold start: remove pods, let it scale to zero, cool down.
        force_delete_pods "$ksvc"
        wait_scale_to_zero "$ksvc" || true
        sleep "$INTER_COLD_SLEEP"

        local rt
        rt=$(time_cold "$status_url" "$cold_timeout" "$cold_response")
        if [[ "$rt" != "-1" ]]; then
            local got
            if ! got=$(wait_pod_cpu_applied "$ksvc" "$cpu_milli"); then
                die "$ksvc cold(patch) @ ${cpu_milli}m: serving pod at ${got}m ≠ ${cpu_milli}m — ksvc patch/route not applied. Refusing bogus data."
            fi
        fi
        echo "$i,$rt" >> "$out_csv"
        info "  cold sample $i/$COLD_SAMPLES: ${rt}ms (cpu=${cpu_milli}m verified, patch)"
    done
}

# warm_bring_up — mirror NIMBUS: pin the ksvc to ONE stable WARM_INIT revision
# and bring up a SINGLE pod at it. The whole warm sweep then only RESIZES this
# pod (subresource) per level — the ksvc is never re-patched, so there is no
# per-level revision churn (the root cause of the scale-to-zero fights and the
# multi-pod routing bug). Returns 0 with a single pod up at WARM_INIT, else 1.
warm_bring_up() {
    local ksvc=$1 status_url=$2 warm_timeout=$3 cold_response=$4
    local want; want=$(to_milli "$WARM_INIT_CPU")
    # Capture the current revision BEFORE patching so we wait for the NEW one to
    # be ready (not the old cold-base=10m revision) before triggering.
    local pre; pre=$(kubectl get ksvc "$ksvc" -n "$NAMESPACE" -o jsonpath='{.status.latestCreatedRevisionName}' 2>/dev/null)
    patch_ksvc_cpu "$ksvc" "$WARM_INIT_CPU"
    wait_ksvc_latest_ready "$ksvc" "$pre" || warn "  warm: WARM_INIT revision not ready in time (continuing)"
    # Diagnostics: what the ksvc actually looks like after the patch.
    local ksvc_cpu created ready
    ksvc_cpu=$(capture_ksvc_cpu "$ksvc")
    created=$(kubectl get ksvc "$ksvc" -n "$NAMESPACE" -o jsonpath='{.status.latestCreatedRevisionName}' 2>/dev/null)
    ready=$(kubectl get ksvc "$ksvc" -n "$NAMESPACE" -o jsonpath='{.status.latestReadyRevisionName}' 2>/dev/null)
    info "  warm: after patch → ksvc.cpu=${ksvc_cpu} preRev=${pre} created=${created} ready=${ready}"
    local up got _att
    for _att in 1 2 3; do
        force_delete_pods "$ksvc"
        up=$(time_cold "$status_url" "$warm_timeout" "$cold_response")
        [[ "$up" == "-1" ]] && { warn "  warm: pod never READY at WARM_INIT (attempt $_att)"; continue; }
        if got=$(wait_pod_cpu_applied "$ksvc" "$want"); then
            info "  warm: single pod up at WARM_INIT=${WARM_INIT_CPU} (stable revision)"
            return 0
        fi
        warn "  warm: pod came up at ${got}m (stale revision) — retry $_att"
    done
    return 1
}

# probe_warm_resized — measure warm at cpu_milli by IN-PLACE resizing the already
# running warm pod (brought up once by warm_bring_up) to cpu_milli, waiting for
# the kubelet to actually apply it, then WARM_SAMPLES timed requests. No ksvc
# patch, no new pod, no scale-to-zero — exactly one pod serves throughout, so
# every request hits the correct CPU (this is what NIMBUS's searchCMinWarm does).
probe_warm_resized() {
    local ksvc=$1 cpu_milli=$2 warm_url=$3 contains=$4 warm_timeout=$5 out_csv=$6

    echo "index,rt_millis" > "$out_csv"

    if ! resize_pod_cpu "$ksvc" "$cpu_milli"; then
        warn "  warm @ ${cpu_milli}m: in-place resize failed (pod gone?) — skipping"; return 0
    fi
    local got
    if ! got=$(wait_pod_cpu_applied "$ksvc" "$cpu_milli"); then
        warn "  warm @ ${cpu_milli}m: applied CPU=${got}m ≠ ${cpu_milli}m — skipping"; return 0
    fi
    sleep "$INTER_WARM_SLEEP"   # cgroup settle
    info "  warm pod resized → ${cpu_milli}m (applied+verified)"

    # Warmup discard — flush the goroutine backlog from the resize.
    curl -sf --max-time "$warm_timeout" "$warm_url" >/dev/null 2>&1 || true
    sleep "$INTER_WARM_SLEEP"

    for ((i=1; i<=WARM_SAMPLES; i++)); do
        local rt
        rt=$(time_warm "$warm_url" "$contains" "$warm_timeout")
        echo "$i,$rt" >> "$out_csv"
        [[ "$rt" != "-1" ]] && info "  warm sample $i/$WARM_SAMPLES: ${rt}ms" \
                              || warn "  warm sample $i/$WARM_SAMPLES: TIMEOUT"
        [[ $i -lt $WARM_SAMPLES ]] && sleep "$INTER_WARM_SLEEP"
    done
    return 0
}

# ─── Per-service benchmark loop ───────────────────────────────────────────────
benchmark_service() {
    local entry=$1
    local ksvc warm_path warm_contains warm_timeout cold_timeout cold_path cold_response
    IFS='|' read -r ksvc warm_path warm_contains warm_timeout cold_timeout cold_path cold_response <<< "$entry"
    cold_path="${cold_path:-$COLD_PATH}"
    cold_response="${cold_response:-$COLD_RESPONSE}"
    # MAX_*_TIMEOUT env, when set, overrides the entry's per-request timeouts.
    cold_timeout="${COLD_TIMEOUT_OVERRIDE:-$cold_timeout}"
    warm_timeout="${WARM_TIMEOUT_OVERRIDE:-$warm_timeout}"

    preflight_service "$ksvc" || return 0

    # Guard: a conflicting boost CR would override our CPU (the classic bug).
    local conflicts; conflicts=$(detect_conflicting_boosts "$ksvc")
    if [[ -n "$conflicts" ]]; then
        if [[ "$PURGE_BOOSTS" == "1" ]]; then
            warn "$ksvc: deleting conflicting StartupCPUBoost CR(s): $(echo $conflicts) — re-apply preload afterwards to restore c_opt_cold"
            local c; for c in $conflicts; do
                kubectl delete startupcpuboost "$c" -n "$NAMESPACE" >/dev/null 2>&1 || true
            done
        else
            warn "SKIP $ksvc: existing StartupCPUBoost CR(s) will override CPU: $(echo $conflicts)"
            warn "  → stop NIMBUS, then set PURGE_BOOSTS=1 to delete them (re-apply preload after), or delete manually."
            return 0
        fi
    fi

    # Capture the ksvc spec CPU so cleanup() restores it (we never leave it changed).
    if [[ -z "${ORIG_CPU[$ksvc]:-}" ]]; then
        ORIG_CPU[$ksvc]="$(capture_ksvc_cpu "$ksvc")"
        info "$ksvc: original ksvc cpu = ${ORIG_CPU[$ksvc]:-<none>} (restored on exit)"
    fi

    local svc_dir="$RESULTS_DIR/$ksvc"
    mkdir -p "$svc_dir/cold" "$svc_dir/warm"

    local base_url="http://${ksvc}.${NAMESPACE}.svc.cluster.local"
    local status_url="${base_url}${cold_path}"
    local warm_url="${base_url}${warm_path}"

    local cold_summary="$svc_dir/cold_summary.csv"
    local warm_summary="$svc_dir/warm_summary.csv"
    echo "cpu_milli,avg,p50,p90,p95,timeouts,total,knee,infeasible" > "$cold_summary"
    echo "cpu_milli,avg,p50,p90,p95,timeouts,total,knee,infeasible" > "$warm_summary"

    # ============================ COLD PHASE ============================
    # Boost-injected, a FRESH cold-start per level (ksvc base pinned low so the
    # boost always raises UP to the target). Runs to its own knee.
    log "=== $ksvc: COLD phase (boost-injected + verified) ==="
    local cpu=$CPU_START prev_cold_p95="TIMEOUT" cold_knee_steps=0 cold_done=false
    while true; do
        info "$ksvc cold @ ${cpu}m"
        local cold_csv="$svc_dir/cold/${cpu}m.csv"
        log "  → cold probe (${COLD_SAMPLES} samples)"
        probe_cold "$ksvc" "$cpu" "$status_url" "$cold_timeout" "$cold_csv" "$cold_path" "$cold_response"
        local cold_stats; cold_stats=$(compute_stats "$cold_csv")
        local cold_avg cold_p50 cold_p90 cold_p95 cold_to cold_total
        IFS=',' read -r cold_avg cold_p50 cold_p90 cold_p95 cold_to cold_total <<< "$cold_stats"

        local cold_infeasible=false cold_knee_flag=false to_pct=0
        [[ "$cold_total" -gt 0 ]] && to_pct=$(( cold_to * 100 / cold_total ))
        [[ $to_pct -ge $INFEASIBLE_RATIO ]] && cold_infeasible=true
        if [[ "$cold_infeasible" == false ]]; then
            if is_knee "$prev_cold_p95" "$cold_p95" 2>/dev/null; then
                cold_knee_steps=$((cold_knee_steps + 1)); cold_knee_flag=true
                [[ $cold_knee_steps -ge $KNEE_CONFIRM ]] && cold_done=true
            else cold_knee_steps=0; fi
        fi
        echo "${cpu},${cold_avg},${cold_p50},${cold_p90},${cold_p95},${cold_to},${cold_total},${cold_knee_flag},${cold_infeasible}" >> "$cold_summary"
        log "  cold p95=${cold_p95}ms  timeouts=${cold_to}/${cold_total}  knee=${cold_knee_flag}"
        prev_cold_p95="$cold_p95"
        [[ "$cold_done" == true ]] && { log "  cold knee confirmed at ${cpu}m"; break; }
        cpu=$((cpu + CPU_STEP))
        [[ "$cpu" -gt "$CPU_MAX" ]] && { log "  cold reached CPU_MAX=${CPU_MAX}m"; break; }
    done

    # ============================ WARM PHASE ============================
    # NIMBUS-style: bring up ONE pod at a STABLE WARM_INIT revision, then only
    # in-place RESIZE it per level — the ksvc is never re-patched, so there is no
    # per-level revision churn (no scale-to-zero fights, no multi-pod routing).
    # Exactly one pod serves every request at the resized CPU.
    log "=== $ksvc: WARM phase (single pod @ WARM_INIT=${WARM_INIT_CPU}, in-place resize per level) ==="
    if warm_bring_up "$ksvc" "$status_url" "$warm_timeout" "$cold_response"; then
        local cpu=$CPU_START prev_warm_p95="TIMEOUT" warm_knee_steps=0 warm_done=false
        while true; do
            info "$ksvc warm @ ${cpu}m"
            local warm_csv="$svc_dir/warm/${cpu}m.csv"
            log "  → warm probe (resize + 1 warmup + ${WARM_SAMPLES} samples)"
            probe_warm_resized "$ksvc" "$cpu" "$warm_url" "$warm_contains" "$warm_timeout" "$warm_csv"
            local warm_stats; warm_stats=$(compute_stats "$warm_csv")
            local warm_avg warm_p50 warm_p90 warm_p95 warm_to warm_total
            IFS=',' read -r warm_avg warm_p50 warm_p90 warm_p95 warm_to warm_total <<< "$warm_stats"

            local warm_infeasible=false warm_knee_flag=false warm_to_pct=0
            [[ "$warm_total" -gt 0 ]] && warm_to_pct=$(( warm_to * 100 / warm_total ))
            [[ $warm_to_pct -ge $INFEASIBLE_RATIO ]] && warm_infeasible=true
            if [[ "$warm_infeasible" == false ]]; then
                if is_knee "$prev_warm_p95" "$warm_p95" 2>/dev/null; then
                    warm_knee_steps=$((warm_knee_steps + 1)); warm_knee_flag=true
                    [[ $warm_knee_steps -ge $KNEE_CONFIRM ]] && warm_done=true
                else warm_knee_steps=0; fi
            fi
            echo "${cpu},${warm_avg},${warm_p50},${warm_p90},${warm_p95},${warm_to},${warm_total},${warm_knee_flag},${warm_infeasible}" >> "$warm_summary"
            log "  warm p95=${warm_p95}ms  timeouts=${warm_to}/${warm_total}  knee=${warm_knee_flag}"
            prev_warm_p95="$warm_p95"
            [[ "$warm_done" == true ]] && { log "  warm knee confirmed at ${cpu}m"; break; }
            cpu=$((cpu + CPU_STEP))
            [[ "$cpu" -gt "$CPU_MAX" ]] && { log "  warm reached CPU_MAX=${CPU_MAX}m"; break; }
        done
    else
        warn "$ksvc: could not bring up a warm pod at WARM_INIT — warm phase skipped"
    fi

    log "=== $ksvc: done. Results → $svc_dir ==="
}

# ─── Merge all summaries into all_services.json ───────────────────────────────
build_json() {
    local out="$RESULTS_DIR/all_services.json"
    echo "{" > "$out"
    local first_svc=true
    for entry in "${ACTIVE_SERVICES[@]}"; do
        local ksvc="${entry%%|*}"
        local svc_dir="$RESULTS_DIR/$ksvc"
        [[ -d "$svc_dir" ]] || continue

        [[ "$first_svc" == false ]] && echo "," >> "$out"
        first_svc=false
        echo "  \"$ksvc\": {" >> "$out"

        for phase in cold warm; do
            local summary="$svc_dir/${phase}_summary.csv"
            [[ -f "$summary" ]] || continue
            echo "    \"$phase\": [" >> "$out"
            local first_row=true
            while IFS=',' read -r cpu avg p50 p90 p95 to total knee infeasible; do
                [[ "$cpu" == "cpu_milli" ]] && continue
                [[ "$first_row" == false ]] && echo "," >> "$out"
                first_row=false
                printf '      {"cpu":%s,"avg":"%s","p50":"%s","p90":"%s","p95":"%s","timeouts":%s,"knee":%s,"infeasible":%s}' \
                    "$cpu" "$avg" "$p50" "$p90" "$p95" "$to" \
                    "$([[ "$knee" == true ]] && echo true || echo false)" \
                    "$([[ "$infeasible" == true ]] && echo true || echo false)" >> "$out"
            done < "$summary"
            echo "" >> "$out"
            echo "    ]$([ $phase == cold ] && echo , || echo '')" >> "$out"
        done
        echo "  }" >> "$out"
    done
    echo "}" >> "$out"
    log "JSON summary → $out"
}

# ─── Main ─────────────────────────────────────────────────────────────────────
mkdir -p "$RESULTS_DIR"
log "Results dir: $RESULTS_DIR"
log "Services: $(for e in "${ACTIVE_SERVICES[@]}"; do echo -n "${e%%|*} "; done)"
log "Model: cold via COLD_METHOD=${COLD_METHOD} ($([[ "$COLD_METHOD" == patch ]] && echo 'direct ksvc patch, no boost' || echo 'StartupCPUBoost CR')), warm via in-place resize, every pod CPU verified"

for entry in "${ACTIVE_SERVICES[@]}"; do
    benchmark_service "$entry"
done

build_json

log "Benchmark complete. Results: $RESULTS_DIR"
log "Plot with: python3 results/plot_profiles.py --results-dir $RESULTS_DIR"
