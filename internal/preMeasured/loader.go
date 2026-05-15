// Package preMeasured loads previously-exported run data (the format
// produced by internal/export) and translates it back into in-memory
// NodeResult entries the watcher can use to skip the binary search.
//
// Today the loader reads only <dir>/<node>/result.json — enough to mark
// a node "saturated" and trigger the fast-path skip. Raw per-sample CSVs
// under <node>/<phase>/<cpu>.csv are NOT consumed here: the controller's
// in-memory PerNodeResults.ColdRtSamples / WarmRtSamples are populated
// by the binary search itself, not by replay. If the online stage later
// needs the historical samples, extend ReadRunDir to walk the CSVs and
// aggregate per CPU.
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
	Node                 string               `json:"node"`
	StartingCpu          string               `json:"startingCpu,omitempty"`
	StartingRt           *nimbusevent.RtStats `json:"startingRt,omitempty"`
	RunningCpu           string               `json:"runningCpu,omitempty"`
	RunningRt            *nimbusevent.RtStats `json:"runningRt,omitempty"`
	ColdPhaseCompletedAt string               `json:"cold_phase_completed_at,omitempty"`
	WarmPhaseCompletedAt string               `json:"warm_phase_completed_at,omitempty"`
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

		out[nodeName] = &nimbusevent.NodeResult{
			StartingCpu:       nrf.StartingCpu,
			StartingRt:        nrf.StartingRt,
			RunningCpu:        nrf.RunningCpu,
			RunningRt:         nrf.RunningRt,
			StartingSaturated: true,
			RunningSaturated:  true,
			// ColdRtSamples / WarmRtSamples deliberately empty — see
			// package doc. Online stage that needs samples should be
			// extended here.
		}
	}

	return out, nil
}
