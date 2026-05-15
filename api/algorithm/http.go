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
