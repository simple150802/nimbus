#!/usr/bin/env bash
# verify-probe.sh — manually reproduce NIMBUS's cold + warm measurement for one
# ksvc and print the response times, so you can confirm the numbers NIMBUS wrote
# into .status.perNode are correct.
#
# Run from the control-plane node (it has cluster DNS + pod-network access — the
# same path NIMBUS uses). Requires: kubectl, curl, bc.
#
# Usage:
#   ./verify-probe.sh <ksvc> <namespace> <cold_cpu> <warm_cpu> [samples]
# Example (sha256 boost-003: cold c_opt=75m, warm c_opt=975m):
#   ./verify-probe.sh sha256-001 serverless 75m 975m 5
set -euo pipefail

KSVC="${1:?need ksvc name}"
NS="${2:?need namespace}"
COLD_CPU="${3:?need cold cpu e.g. 75m}"
WARM_CPU="${4:?need warm cpu e.g. 975m}"
SAMPLES="${5:-5}"

URL_STATUS="http://${KSVC}.${NS}.svc.cluster.local/status"
URL_WARM="http://${KSVC}.${NS}.svc.cluster.local/detect/local"
LABEL="serving.knative.dev/service=${KSVC}"

now() { date +%s.%N; }
podname() { kubectl get pod -n "$NS" -l "$LABEL" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null; }

scale_to_zero() {
  echo "  [reset] deleting pods, waiting for scale-to-zero..."
  kubectl delete pod -n "$NS" -l "$LABEL" --force --grace-period=0 >/dev/null 2>&1 || true
  while [ -n "$(kubectl get pod -n "$NS" -l "$LABEL" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)" ]; do
    sleep 1
  done
  sleep 10   # endpoint propagation (mirrors NIMBUS interColdSampleSleep)
}

set_ksvc_cpu() {
  local cpu="$1"
  # JSON-patch the two cpu paths only. A merge patch on the containers LIST
  # replaces element [0] wholesale and drops required fields (image) → the
  # Knative webhook rejects it ("missing field spec...containers[0].image").
  kubectl patch ksvc "$KSVC" -n "$NS" --type=json -p \
    "[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/limits/cpu\",\"value\":\"${cpu}\"},{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/requests/cpu\",\"value\":\"${cpu}\"}]" >/dev/null
}

resize_pod_cpu() {   # in-place resize via the resize subresource (no restart)
  local cpu="$1" pod
  pod="$(podname)"
  kubectl patch pod "$pod" -n "$NS" --subresource=resize --type=json -p \
    "[{\"op\":\"replace\",\"path\":\"/spec/containers/0/resources/limits/cpu\",\"value\":\"${cpu}\"},{\"op\":\"replace\",\"path\":\"/spec/containers/0/resources/requests/cpu\",\"value\":\"${cpu}\"}]" >/dev/null
}

# ---------- COLD: time from pod-creation to /status=READY at COLD_CPU ----------
echo "=== COLD probe @ ${COLD_CPU} ==="
set_ksvc_cpu "$COLD_CPU"
scale_to_zero
start="$(now)"
until curl -s --max-time 120 "$URL_STATUS" | grep -q READY; do sleep 0.05; done
end="$(now)"
echo "  cold-start (pod→READY): $(echo "$end - $start" | bc)s   [NIMBUS startingRt to compare]"

# ---------- WARM: resize the live pod to WARM_CPU, then timed /detect/local ----
echo "=== WARM probe @ ${WARM_CPU} (pod already up at ${COLD_CPU}) ==="
resize_pod_cpu "$WARM_CPU"
sleep 2                                   # cgroup settle
curl -s --max-time 120 "$URL_WARM" >/dev/null   # warmup discard (flush backlog)
sleep 2
echo "  timed samples:"
for i in $(seq 1 "$SAMPLES"); do
  s="$(now)"; curl -s --max-time 120 "$URL_WARM" >/dev/null; e="$(now)"
  printf "    sample %d: %ss\n" "$i" "$(echo "$e - $s" | bc)"
  sleep 2
done
echo "  [compare against NIMBUS warmRtSamples @ ${WARM_CPU}]"
