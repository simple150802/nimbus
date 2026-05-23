// Package preMeasured loads previously-exported run data (the format
// produced by internal/export) and translates it back into in-memory
// NodeResult entries the watcher can use to skip the binary search.
//
// The loader reads <dir>/<node>/result.json, which since the Option-A
// change carries both the converged CPU values AND the per-probe sample
// trail (coldRtSamples / warmRtSamples). The sample trail then flows
// through applyPreMeasured → WriteNimbusStatus into .status.perNode, so
// a preload-driven run leaves status with the same per-CPU sample list
// a fresh measurement would have produced. Raw per-sample CSVs under
// <node>/<phase>/<cpu>.csv are still NOT consumed — those exist for
// ad-hoc analysis (pandas / Jupyter) and aren't needed for the runtime
// path.
package preMeasured

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"nimbus/api/nimbusevent"
)

// ErrDirNotFound is returned by ReadRunDir when loadFromDir doesn't
// exist on the filesystem. The caller (watcher) treats this as a
// non-fatal warning and falls through to the search path.
var ErrDirNotFound = errors.New("preMeasured: loadFromDir does not exist")

// nodeResultFile mirrors the shape internal/export.WriteResult writes.
// Kept private so the export package's internal type stays the source
// of truth; this is a read-only mirror.
type nodeResultFile struct {
	Node                 string                    `json:"node"`
	StartingCpu          string                    `json:"startingCpu,omitempty"`
	StartingRt           *nimbusevent.RtStats      `json:"startingRt,omitempty"`
	CMinStarting         string                    `json:"cMinStarting,omitempty"`
	ColdRtSamples        []nimbusevent.SamplePoint `json:"coldRtSamples,omitempty"`
	RunningCpu           string                    `json:"runningCpu,omitempty"`
	RunningRt            *nimbusevent.RtStats      `json:"runningRt,omitempty"`
	CMinRunning          string                    `json:"cMinRunning,omitempty"`
	WarmRtSamples        []nimbusevent.SamplePoint `json:"warmRtSamples,omitempty"`
	ColdPhaseCompletedAt string                    `json:"cold_phase_completed_at,omitempty"`
	WarmPhaseCompletedAt string                    `json:"warm_phase_completed_at,omitempty"`
}

// ReadRunMetric returns the spec.metric value recorded in
// <loadFromDir>/meta.json. Empty string + nil err means "unknown" —
// either the file is missing, malformed, or the field wasn't set when
// the run was exported (pre-metric-field controllers). The caller uses
// this for an informational mismatch warning; it never blocks the load.
// Returns ErrDirNotFound if loadFromDir itself doesn't exist.
func ReadRunMetric(loadFromDir string) (string, error) {
	if loadFromDir == "" {
		return "", fmt.Errorf("loadFromDir is empty")
	}
	abs, err := filepath.Abs(loadFromDir)
	if err != nil {
		return "", fmt.Errorf("resolve absolute path: %w", err)
	}
	if _, err := os.Stat(abs); err != nil {
		if os.IsNotExist(err) {
			return "", ErrDirNotFound
		}
		return "", err
	}
	data, err := os.ReadFile(filepath.Join(abs, "meta.json"))
	if err != nil {
		return "", nil
	}
	// Anonymous shape: peel only the one field we care about so we
	// don't have to mirror the full runMeta struct from the export
	// package (which is private to that package and would create a
	// dependency cycle if exported).
	var meta struct {
		Nimbus struct {
			Spec struct {
				Metric string `json:"metric"`
			} `json:"spec"`
		} `json:"nimbus"`
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return "", nil
	}
	return meta.Nimbus.Spec.Metric, nil
}

// ReadRunDir scans loadFromDir for one subdirectory per node and reads
// each <node>/result.json. Returns a map keyed by node name with one
// NodeResult per node that had a parseable result.json. Missing or
// malformed files yield no entry for that node (logged by the caller).
//
// loadFromDir may be relative (resolved against the controller's cwd)
// or absolute. Returns ErrDirNotFound if the resolved path doesn't
// exist; any other I/O error is wrapped and returned.
func ReadRunDir(loadFromDir string) (map[string]*nimbusevent.NodeResult, error) {
	if loadFromDir == "" {
		return nil, fmt.Errorf("loadFromDir is empty")
	}
	abs, err := filepath.Abs(loadFromDir)
	if err != nil {
		return nil, fmt.Errorf("resolve absolute path: %w", err)
	}

	entries, err := os.ReadDir(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrDirNotFound
		}
		return nil, fmt.Errorf("list %s: %w", abs, err)
	}

	out := make(map[string]*nimbusevent.NodeResult)
	for _, e := range entries {
		if !e.IsDir() {
			continue // skip meta.json and any stray files
		}
		nodeName := e.Name()
		resultPath := filepath.Join(abs, nodeName, "result.json")

		data, err := os.ReadFile(resultPath)
		if err != nil {
			// Missing result.json for a node-shaped subdir: not loadable.
			// Caller decides whether to log; we just omit the entry.
			continue
		}

		var nrf nodeResultFile
		if err := json.Unmarshal(data, &nrf); err != nil {
			// Malformed JSON — same treatment: skip this node, let the
			// search re-derive it.
			continue
		}

		// A loaded node counts as saturated only when BOTH CPU values are
		// present. Partial result.json (only cold or only warm) is treated
		// as "not loadable" — the search will fill both phases.
		if nrf.StartingCpu == "" || nrf.RunningCpu == "" {
			continue
		}

		// Defensive copies of the sample slices so a future mutation
		// of out[node].ColdRtSamples doesn't reach back into the
		// JSON-decoded buffer.
		var cold, warm []nimbusevent.SamplePoint
		if len(nrf.ColdRtSamples) > 0 {
			cold = append([]nimbusevent.SamplePoint(nil), nrf.ColdRtSamples...)
		}
		if len(nrf.WarmRtSamples) > 0 {
			warm = append([]nimbusevent.SamplePoint(nil), nrf.WarmRtSamples...)
		}
		out[nodeName] = &nimbusevent.NodeResult{
			StartingCpu:       nrf.StartingCpu,
			StartingRt:        nrf.StartingRt,
			CMinStarting:      nrf.CMinStarting,
			ColdRtSamples:     cold,
			RunningCpu:        nrf.RunningCpu,
			RunningRt:         nrf.RunningRt,
			CMinRunning:       nrf.CMinRunning,
			WarmRtSamples:     warm,
			StartingSaturated: true,
			RunningSaturated:  true,
		}
	}

	return out, nil
}
