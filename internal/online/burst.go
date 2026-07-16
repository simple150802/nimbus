package online

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"nimbus/api/logging"
)

// ---------------------------------------------------------------------------
// Tunable parameters — edit config/burst.env (auto-loaded by main via
// LoadEnvFile) or export the env vars directly. Defaults match OVERVIEW §8.2.
//
//   NIMBUS_BURST_RESERVE_RATIO    free-CPU fraction held back in BURST   (0.30)
//   NIMBUS_BURST_THRESHOLD_RATE   events/sec that flips NORMAL->BURST    (1.0)
//   NIMBUS_BURST_THRESHOLD_DELTA  acceleration that flips early          (0.15)
//   NIMBUS_BURST_WINDOW           sliding window for the rate estimate   (10s)
//   NIMBUS_BURST_EWMA_ALPHA       smoothing for rate (velocity)          (0.30)
//   NIMBUS_BURST_EWMA_BETA        smoothing for rate-of-change (accel)   (0.20)
//   NIMBUS_BURST_DECAY_INTERVAL   decay-loop tick period                 (5s)
//   NIMBUS_BURST_DECAY_QUIET      quiet window before BURST->NORMAL      (30s)
//   NIMBUS_BURST_DECAY_FACTOR     fade per decay tick during silence     (0.5)
//
// (NIMBUS_BUDGET_PCT — the per-node serverless budget — is parsed in
// controller.go's budgetPct.)
// ---------------------------------------------------------------------------

// BurstConfig holds the burst-detector tunables. Defaults mirror OVERVIEW §8.2.
type BurstConfig struct {
	ReserveRatio   float64
	ThresholdRate  float64
	ThresholdDelta float64
	Window         time.Duration
	Alpha          float64
	Beta           float64
	DecayInterval  time.Duration
	DecayQuiet     time.Duration
	DecayFactor    float64 // fade multiplier per decay tick — INDEPENDENT of Alpha/Beta
}

// DefaultBurstConfig returns the §8.2 defaults with per-field env overrides.
func DefaultBurstConfig() BurstConfig {
	cfg := BurstConfig{
		ReserveRatio:   envFloat("NIMBUS_BURST_RESERVE_RATIO", 0.30),
		ThresholdRate:  envFloat("NIMBUS_BURST_THRESHOLD_RATE", 1.0),
		ThresholdDelta: envFloat("NIMBUS_BURST_THRESHOLD_DELTA", 0.15),
		Window:         envDuration("NIMBUS_BURST_WINDOW", 10*time.Second),
		Alpha:          envFloat("NIMBUS_BURST_EWMA_ALPHA", 0.30),
		Beta:           envFloat("NIMBUS_BURST_EWMA_BETA", 0.20),
		DecayInterval:  envDuration("NIMBUS_BURST_DECAY_INTERVAL", 5*time.Second),
		DecayQuiet:     envDuration("NIMBUS_BURST_DECAY_QUIET", 30*time.Second),
		DecayFactor:    envFloat("NIMBUS_BURST_DECAY_FACTOR", 0.5),
	}
	if cfg.Window <= 0 { // guard div-by-zero in the windowed-rate estimator
		cfg.Window = 10 * time.Second
	}
	if cfg.DecayFactor <= 0 || cfg.DecayFactor >= 1 { // must fade, but not instantly
		cfg.DecayFactor = 0.5
	}
	return cfg
}

// BurstMode is the detector's current state.
type BurstMode int

const (
	ModeNormal BurstMode = iota
	ModeBurst
)

func (m BurstMode) String() string {
	if m == ModeBurst {
		return "BURST"
	}
	return "NORMAL"
}

// BurstState is the single, cluster-wide cold-start-rate signal. The /decide
// handler feeds it via OnColdStartEvent (one event per cold-start RPC); a
// background decay goroutine (StartBurstDecay) returns it to NORMAL after a
// quiet window. The online reconciler reads it via Read() under RLock on
// every waterfall decision. Decoupled from any reconcile loop.
type BurstState struct {
	mu           sync.RWMutex
	cfg          BurstConfig
	mode         BurstMode
	events       []time.Time // cold-start arrivals within the last cfg.Window
	ewmaRate     float64     // smoothed events/sec (velocity)
	ewmaDelta    float64     // smoothed rate-of-change (acceleration)
	reserveRatio float64     // 0 in NORMAL, cfg.ReserveRatio in BURST
	lastEventAt  time.Time
	seeded       bool
}

// NewBurstState builds a NORMAL-mode detector with config from env/defaults.
func NewBurstState() *BurstState {
	return &BurstState{cfg: DefaultBurstConfig(), mode: ModeNormal}
}

// Read returns the current mode, reserve ratio, smoothed rate (velocity), and
// smoothed acceleration. O(1) under RLock — safe on every per-ksvc waterfall
// decision. The rate/deltaRate are also stamped onto status.online for
// experiment-CSV correlation.
func (b *BurstState) Read() (mode BurstMode, reserveRatio, rate, deltaRate float64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.mode, b.reserveRatio, b.ewmaRate, b.ewmaDelta
}

// OnColdStartEvent folds one observed cold-start arrival into the EWMA rate and
// acceleration, flipping NORMAL->BURST when either crosses its threshold.
// Called from the /decide handler.
//
// The instantaneous rate is a SLIDING-WINDOW COUNT — cold-starts seen in the
// last cfg.Window seconds ÷ window — not 1/inter-arrival. A single-sample 1/dt
// estimator overestimates the true arrival rate (Jensen: E[1/Δt] ≥ 1/E[Δt])
// and blows up for near-simultaneous events (dt→0), so one coincidental close
// pair could trip BURST. The windowed count is unbiased in events/sec and
// bounded, so the ThresholdRate keeps a clean "events/sec" meaning.
func (b *BurstState) OnColdStartEvent(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.seeded = true
	b.lastEventAt = now

	// Append this arrival and drop everything older than the window.
	b.events = append(b.events, now)
	cutoff := now.Add(-b.cfg.Window)
	drop := 0
	for drop < len(b.events) && b.events[drop].Before(cutoff) {
		drop++
	}
	b.events = b.events[drop:]
	inst := float64(len(b.events)) / b.cfg.Window.Seconds()

	prev := b.ewmaRate
	b.ewmaRate = b.cfg.Alpha*inst + (1-b.cfg.Alpha)*b.ewmaRate
	b.ewmaDelta = b.cfg.Beta*(b.ewmaRate-prev) + (1-b.cfg.Beta)*b.ewmaDelta

	if b.mode == ModeNormal && (b.ewmaRate > b.cfg.ThresholdRate || b.ewmaDelta > b.cfg.ThresholdDelta) {
		b.mode = ModeBurst
		b.reserveRatio = b.cfg.ReserveRatio
		logging.Info(fmt.Sprintf("[online][burst] event=mode_change mode=BURST rate=%.2f delta=%.2f reserve=%.2f",
			b.ewmaRate, b.ewmaDelta, b.reserveRatio))
	}
}

// decay runs on the decay ticker: during quiet it bleeds the smoothed rate
// toward zero (so it can fall below the threshold with no new events), then
// returns to NORMAL once the quiet window elapses and the rate is sub-threshold.
func (b *BurstState) decay(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.seeded {
		return
	}
	quiet := now.Sub(b.lastEventAt)
	if quiet >= b.cfg.DecayInterval {
		// Fade toward zero using a DEDICATED factor, not (1-Alpha)/(1-Beta), so
		// responsiveness (Alpha/Beta) and forget-speed (DecayFactor) tune apart.
		b.ewmaRate *= b.cfg.DecayFactor
		b.ewmaDelta *= b.cfg.DecayFactor
	}
	if b.mode == ModeBurst && quiet >= b.cfg.DecayQuiet && b.ewmaRate < b.cfg.ThresholdRate {
		b.mode = ModeNormal
		b.reserveRatio = 0
		logging.Info(fmt.Sprintf("[online][burst] event=mode_change mode=NORMAL rate=%.2f quiet_s=%.0f",
			b.ewmaRate, quiet.Seconds()))
	}
}

// StartBurstDecay runs the decay loop until ctx is cancelled. Launch as
// `go online.StartBurstDecay(ctx, bs)`. The cold-start event source is the
// /decide handler (OnColdStartEvent), not this goroutine.
func StartBurstDecay(ctx context.Context, bs *BurstState) {
	logging.Info(fmt.Sprintf("[online][burst] event=decay_start interval=%s quiet=%s reserve=%.2f threshold_rate=%.2f action=start",
		bs.cfg.DecayInterval, bs.cfg.DecayQuiet, bs.cfg.ReserveRatio, bs.cfg.ThresholdRate))

	ticker := time.NewTicker(bs.cfg.DecayInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logging.Info("[online][burst] event=decay_stop action=stop reason=context_cancelled")
			return
		case <-ticker.C:
			bs.decay(time.Now())
		}
	}
}

// LoadEnvFile seeds the process environment from a dotenv-style file (e.g.
// config/burst.env) so the burst/budget tunables can be edited in one place.
// Call it from main BEFORE the online goroutines read os.Getenv. Blank lines
// and '#' comments are ignored; a KEY already present in the environment is
// not overwritten (an explicit export wins over the file); a missing file is
// not an error (built-in defaults apply).
func LoadEnvFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			logging.Info(fmt.Sprintf("[online] event=config_load path=%s action=skip reason=not_found (built-in defaults apply)", path))
			return
		}
		logging.Warning(fmt.Sprintf("[online] event=config_load path=%s action=skip reason=%v", path, err))
		return
	}
	loaded := 0
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		key := strings.TrimSpace(line[:eq])
		val := strings.Trim(strings.TrimSpace(line[eq+1:]), `"'`)
		if key == "" {
			continue
		}
		if _, present := os.LookupEnv(key); present {
			continue
		}
		if err := os.Setenv(key, val); err != nil {
			logging.Warning(fmt.Sprintf("[online] event=config_load key=%s action=skip reason=%v", key, err))
			continue
		}
		loaded++
	}
	logging.Info(fmt.Sprintf("[online] event=config_load path=%s loaded=%d action=apply", path, loaded))
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		logging.Warning(fmt.Sprintf("[online][burst] invalid %s=%q — using default %v", key, v, def))
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		logging.Warning(fmt.Sprintf("[online][burst] invalid %s=%q — using default %v", key, v, def))
	}
	return def
}
