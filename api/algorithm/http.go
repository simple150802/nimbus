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

// warmProbeTimeout is the per-request timeout for the warm-phase HTTP
// probe (triggerHttpWithCodeBody). Warm-phase gates hit a real workload
// endpoint (e.g. /detect/local on the YOLO probe app) whose per-request
// latency at the search-floor CPU can be 20–40 s. The 5 s cold ceiling
// is far too tight: NIMBUS times out client-side before inference
// finishes, gunicorn keeps processing (logs 200), queue-proxy logs
// "context canceled" — and NIMBUS retries forever without ever getting
// a response. 90 s gives ~2× safety margin over realistic worst cases.
const warmProbeTimeout = 90 * time.Second

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
// Mirrors triggerHttp's structure: same per-request timeout, same
// ctx-aware retry loop, same phase-tagged logging. Cold path keeps using
// the original triggerHttp so its behavior is unchanged.
func triggerHttpWithCodeBody(ctx context.Context, phase, targetURL string, expectedCode int, bodyContains string) (time.Duration, error) {
	if bodyContains == "" {
		logging.Normal(fmt.Sprintf("[%s] curl GET %s (expect code=%d)", phase, targetURL, expectedCode))
	} else {
		logging.Normal(fmt.Sprintf("[%s] curl GET %s (expect code=%d, body~%q)", phase, targetURL, expectedCode, bodyContains))
	}

	client := &http.Client{Timeout: warmProbeTimeout}
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
