// Package export writes raw per-probe samples + run metadata to the
// filesystem when a Nimbus declares spec.export.dir. Layout:
//
//	<dir>/<timestamp>/
//	    meta.json                              (run-level metadata)
//	    <node>/
//	        cold/<cpu>.csv                     (one row per individual sample)
//	        warm/<cpu>.csv
//	        result.json                        (converged CPU values for this node)
//
// CSV format is intentionally minimal — one row per measurement, only an
// index and the response time in milliseconds. Everything else (node,
// phase, cpu) is encoded in the file path.
//
// All exported functions log warnings on error and return the error; the
// caller is expected to swallow errors so export failures never abort a
// binary search.
package export

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"nimbus/api/logging"
	"nimbus/api/nimbusevent"
)

// timestampLayout is the filesystem-safe ISO-like layout used for run dirs.
// Colons are replaced with hyphens so the path is portable across filesystems.
const timestampLayout = "2006-01-02T15-04-05"

// InitRunDir creates <baseDir>/<timestamp>/ and returns the absolute path
// to it. The caller stores this on NimbusEvent.ExportRoot for the duration
// of the run. baseDir may be relative (resolved against the controller's
// working directory) or absolute; '..' segments are allowed so the user
// can write outside cwd (intended for local-dev use; tighten this guard
// before deploying to a multi-tenant cluster). Returns ("", err) on any
// error — caller treats that as "export disabled for this run" and
// continues.
func InitRunDir(baseDir string, runStartedAt time.Time) (string, error) {
	if baseDir == "" {
		return "", fmt.Errorf("baseDir is empty")
	}

	abs, err := filepath.Abs(baseDir)
	if err != nil {
		return "", fmt.Errorf("resolve absolute path: %w", err)
	}

	runRoot := filepath.Join(abs, runStartedAt.UTC().Format(timestampLayout))
	if err := os.MkdirAll(runRoot, 0o755); err != nil {
		return "", fmt.Errorf("mkdir run dir: %w", err)
	}
	logging.Info(fmt.Sprintf("[export] run dir: %s", runRoot))
	return runRoot, nil
}

// AppendSample appends one CSV row (index, rt_millis) to
// <runRoot>/<node>/<phase>/<cpu>.csv. Creates parent directories on the
// first call. Writes the "index,rt_millis" header when the file is new.
// Index is monotonic per file — on append, the next index is one past the
// existing row count (1-based).
//
// The file is opened in append mode for one write and closed immediately
// (streaming write per Option B; no slice ever holds raw samples in RAM).
func AppendSample(runRoot, node, phase, cpu string, rtMillis int64) error {
	if runRoot == "" {
		return nil // export disabled — silent no-op
	}
	dir := filepath.Join(runRoot, node, phase)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	path := filepath.Join(dir, cpu+".csv")

	// Determine the next index by counting existing rows (excluding header).
	// Cheap: we open append-mode anyway, so we read once to find current size.
	idx, err := nextIndex(path)
	if err != nil {
		return fmt.Errorf("compute next index for %s: %w", path, err)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	// Header on first write only (idx == 1 means the file is new or empty).
	if idx == 1 {
		if _, err := f.WriteString("index,rt_millis\n"); err != nil {
			return fmt.Errorf("write header to %s: %w", path, err)
		}
	}
	if _, err := fmt.Fprintf(f, "%d,%d\n", idx, rtMillis); err != nil {
		return fmt.Errorf("write row to %s: %w", path, err)
	}
	return nil
}

// nextIndex returns 1-based next row index for the CSV at path. Returns 1
// when the file doesn't exist or is empty. Reads the file once to count
// newlines; for the row counts NIMBUS produces (~10s of rows per file)
// this is negligible.
func nextIndex(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 1, nil
		}
		return 0, err
	}
	// Header line ("index,rt_millis\n") is line 1. Data rows start at line 2.
	// Next data-row index = total newlines (treating last unterminated line as a row).
	if len(data) == 0 {
		return 1, nil
	}
	var newlines int64
	for _, b := range data {
		if b == '\n' {
			newlines++
		}
	}
	// newlines == 1 means just the header. Next data row = index 1.
	// newlines == 4 means header + 3 data rows. Next data row = index 4.
	if newlines == 0 {
		// File exists but no newline yet — treat as empty.
		return 1, nil
	}
	return newlines, nil
}

// runMeta is the shape of meta.json. Captured snapshot of the Nimbus spec
// (so the run is reproducible) plus the discovered candidate-node set.
type runMeta struct {
	StartedAt      string                  `json:"started_at"`
	Nimbus         nimbusMetadataAndSpec   `json:"nimbus"`
	CandidateNodes []string                `json:"candidate_nodes"`
}

type nimbusMetadataAndSpec struct {
	Name      string                 `json:"name"`
	Namespace string                 `json:"namespace"`
	Spec      nimbusevent.NimbusSpec `json:"spec"`
}

// WriteMeta writes <runRoot>/meta.json. Overwrites any existing file.
func WriteMeta(runRoot string, ev *nimbusevent.NimbusEvent, candidateNodes []string, startedAt time.Time) error {
	if runRoot == "" {
		return nil
	}
	meta := runMeta{
		StartedAt: startedAt.UTC().Format(time.RFC3339),
		Nimbus: nimbusMetadataAndSpec{
			Name:      ev.Metadata.Name,
			Namespace: ev.Metadata.Namespace,
			Spec:      ev.Spec,
		},
		CandidateNodes: append([]string(nil), candidateNodes...),
	}
	return writeJSON(filepath.Join(runRoot, "meta.json"), meta)
}

// nodeResultFile is the shape of <runRoot>/<node>/result.json.
type nodeResultFile struct {
	Node                 string               `json:"node"`
	StartingCpu          string               `json:"startingCpu,omitempty"`
	StartingRt           *nimbusevent.RtStats `json:"startingRt,omitempty"`
	RunningCpu           string               `json:"runningCpu,omitempty"`
	RunningRt            *nimbusevent.RtStats `json:"runningRt,omitempty"`
	ColdPhaseCompletedAt string               `json:"cold_phase_completed_at,omitempty"`
	WarmPhaseCompletedAt string               `json:"warm_phase_completed_at,omitempty"`
}

// WriteResult writes <runRoot>/<node>/result.json with the converged CPU
// values + saturated RT stats for this node. Overwrites any existing
// file. completedAt is the timestamp at which the per-node search
// finished.
func WriteResult(runRoot, node string, r *nimbusevent.NodeResult, completedAt time.Time) error {
	if runRoot == "" || r == nil {
		return nil
	}
	dir := filepath.Join(runRoot, node)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	completedIso := completedAt.UTC().Format(time.RFC3339)
	out := nodeResultFile{
		Node:        node,
		StartingCpu: r.StartingCpu,
		StartingRt:  r.StartingRt,
		RunningCpu:  r.RunningCpu,
		RunningRt:   r.RunningRt,
	}
	// Use the same completedAt for both phases — we write at end-of-node, not
	// end-of-phase. Refining this would require a second timestamp arg; not
	// worth the call-site complexity unless an experiment needs sub-phase timing.
	if r.StartingSaturated {
		out.ColdPhaseCompletedAt = completedIso
	}
	if r.RunningSaturated {
		out.WarmPhaseCompletedAt = completedIso
	}
	return writeJSON(filepath.Join(dir, "result.json"), out)
}

// writeJSON marshals v with 2-space indentation and writes it atomically
// (write to temp, rename). Atomicity matters when the file is being
// consumed by another process while NIMBUS is writing — partial reads
// are avoided.
func writeJSON(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	data = append(data, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmp, path, err)
	}
	return nil
}
