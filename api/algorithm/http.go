package algorithm

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"nimbus/api/logging"
)

// httpProbeTimeout caps the per-request timeout of the HTTP client.
// Independent of probeTimeout (which caps the entire trigger-and-retry
// loop): each individual GET fails after this duration, then triggerHttp
// retries until the parent context's deadline fires.
const httpProbeTimeout = 5 * time.Second

// warmProbeTimeout is the FLOOR for the warm-phase per-request HTTP timeout.
// Warm gates hit a real workload endpoint whose per-request latency at low CPU
// can be tens of seconds. 90 s is fine for image/inference workloads, but
// minute-scale workloads (e.g. LLM token generation: ~60 s at budget, ~120 s at
// half budget) blow past it — the client times out before the response arrives
// and triggerHttpWithCodeBody's retry loop spins forever. warmRequestTimeout
// scales the actual per-request timeout to the warm SLO; this const is only the
// lower bound when no SLO is set.
const warmProbeTimeout = 90 * time.Second

// warmRequestTimeout returns the per-request HTTP client timeout for warm
// probes, scaled to the warm SLO: max(warmProbeTimeout, 2×SLO). A request that
// could plausibly meet the SLO always has room to complete (so the curve is
// measured, not timed out), while genuinely stuck pods are still bounded. A
// non-positive sloMillis (no warm SLO configured) falls back to the floor.
func warmRequestTimeout(sloMillis int64) time.Duration {
	if sloMillis <= 0 {
		return warmProbeTimeout
	}
	if t := time.Duration(2*sloMillis) * time.Millisecond; t > warmProbeTimeout {
		return t
	}
	return warmProbeTimeout
}

// triggerHttp repeatedly GETs targetURL until the response body contains
// expectedResponse. URL is constructed by the caller via
// kubeapi.BuildKsvcStatusURL — triggerHttp is pure HTTP-retry logic, no
// Knative-DNS knowledge. The phase tag ("COLD" / "WARM") is included in
// every log line so a scrolling log makes the active probe obvious.
// Honors ctx: cancellation aborts both in-flight requests and the
// inter-retry wait, so SIGINT (or a probeTimeout deadline) stops the
// probe immediately.
func triggerHttp(ctx context.Context, phase, targetURL, expectedResponse string) (time.Duration, error) {
	logging.Normal(fmt.Sprintf("[%s] curl GET %s (expect body~%q)", phase, targetURL, expectedResponse))

	client := &http.Client{Timeout: httpProbeTimeout}
	start := time.Now()

	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return 0, err
		}
		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			logging.Normal(fmt.Sprintf("[%s] pod not reachable yet, retrying...", phase))
			if err := sleepCtx(ctx, probeRetryInterval); err != nil {
				return 0, err
			}
			continue
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			logging.Warning(fmt.Sprintf("[%s] failed to read response body, retrying...", phase))
			if err := sleepCtx(ctx, probeRetryInterval); err != nil {
				return 0, err
			}
			continue
		}

		if strings.Contains(string(bodyBytes), expectedResponse) {
			duration := time.Since(start)
			logging.Success(fmt.Sprintf("[%s] expected response received in %s", phase, duration))
			return duration, nil
		}
	}
}

// triggerHttpWithCodeBody is the warm-phase variant of triggerHttp: gates
// on HTTP status code (must equal expectedCode) plus an optional body
// substring (bodyContains; "" disables the body check). Used by
// getResptWarm against the workload endpoint configured in
// spec.durationPolicy.warmApiCondition — body-substring gating alone is
// too brittle for endpoints whose response varies per request (e.g.
// inference detections).
//
// Mirrors triggerHttp's structure: same ctx-aware retry loop, same phase-tagged
// logging. perReqTimeout is the per-request HTTP client timeout — callers pass
// warmRequestTimeout(warmSLO) so minute-scale workloads aren't cut off mid-
// response. Cold path keeps using the original triggerHttp so it's unchanged.
func triggerHttpWithCodeBody(ctx context.Context, phase, targetURL string, expectedCode int, bodyContains string, perReqTimeout time.Duration) (time.Duration, error) {
	if bodyContains == "" {
		logging.Normal(fmt.Sprintf("[%s] curl GET %s (expect code=%d)", phase, targetURL, expectedCode))
	} else {
		logging.Normal(fmt.Sprintf("[%s] curl GET %s (expect code=%d, body~%q)", phase, targetURL, expectedCode, bodyContains))
	}

	client := &http.Client{Timeout: perReqTimeout}
	start := time.Now()

	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return 0, err
		}
		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			logging.Normal(fmt.Sprintf("[%s] pod not reachable yet, retrying...", phase))
			if err := sleepCtx(ctx, probeRetryInterval); err != nil {
				return 0, err
			}
			continue
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			logging.Warning(fmt.Sprintf("[%s] failed to read response body, retrying...", phase))
			if err := sleepCtx(ctx, probeRetryInterval); err != nil {
				return 0, err
			}
			continue
		}

		codeOk := resp.StatusCode == expectedCode
		bodyOk := bodyContains == "" || strings.Contains(string(bodyBytes), bodyContains)
		if codeOk && bodyOk {
			duration := time.Since(start)
			logging.Success(fmt.Sprintf("[%s] expected response received in %s", phase, duration))
			return duration, nil
		}
	}
}
