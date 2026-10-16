package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/sourcegraph/zoekt"
)

// attemptStage is the retryable phase an indexing attempt has reached.
type attemptStage string

const (
	// stageIndexing means the index subprocess has been started and it is
	// unknown whether a shard was published. A record left in this stage
	// signals an interrupted attempt that a restart has to take over.
	stageIndexing attemptStage = "indexing"

	// stageReportSuccess means a shard matching the pinned branch commits is
	// present on disk, but the success report to Sourcegraph is unconfirmed.
	stageReportSuccess attemptStage = "report_success"

	// stageReportFailure means the index subprocess failed, but the failure
	// report to Sourcegraph is unconfirmed.
	stageReportFailure attemptStage = "report_failure"
)

// attemptsDirName holds persistent attempt records. It is a hidden
// subdirectory of the index dir, so shard globbing and stale-shard cleanup
// never touch its files.
const attemptsDirName = ".indexserver.attempts"

// attemptRecordVersion is embedded in persistedAttempt so future format
// changes can reject older records.
const attemptRecordVersion = 1

// persistedAttempt is the on-disk record of one indexing attempt. It lets the
// server take over unconfirmed work after a restart and retry a failed status
// report without rebuilding or republishing the same shard version.
//
// The record is written before the corresponding phase commits and removed
// once the phase is confirmed. Callers always hold the repository indexMutex,
// so at most one writer per RepoID exists.
type persistedAttempt struct {
	// Version is the record format version.
	Version int `json:"version"`

	// RepoID and AttemptID identify the attempt that owns the record.
	RepoID    uint32 `json:"repo_id"`
	AttemptID int64  `json:"attempt_id"`

	// Stage is the retryable phase the attempt has reached.
	Stage attemptStage `json:"stage"`

	// Options pins the full repository configuration, including the branch
	// commits, of the attempt.
	Options IndexOptions `json:"options"`

	// IndexTimeUnix is the IndexTime read from the published shard. It is set
	// for stageReportSuccess so a retried report does not republish anything.
	IndexTimeUnix int64 `json:"index_time_unix,omitempty"`

	// FailureMessage is the truncated index subprocess error. It is set for
	// stageReportFailure so a retried failure report does not rerun the build.
	FailureMessage string `json:"failure_message,omitempty"`

	// UpdatedAt is the last time the record was written.
	UpdatedAt time.Time `json:"updated_at"`
}

// attemptStore persists one JSON record per repository with atomic
// tmp-file-plus-rename writes.
type attemptStore struct {
	dir string
}

func newAttemptStore(indexDir string) *attemptStore {
	return &attemptStore{dir: filepath.Join(indexDir, attemptsDirName)}
}

func (s *attemptStore) path(repoID uint32) string {
	return filepath.Join(s.dir, strconv.FormatUint(uint64(repoID), 10)+".json")
}

// Save atomically writes a for repoID.
func (s *attemptStore) Save(a *persistedAttempt) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}

	a.Version = attemptRecordVersion
	a.UpdatedAt = time.Now()

	dst := s.path(a.RepoID)
	tmp, err := jsonMarshalTmpFile(a, dst)
	if err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

// Load returns the record for repoID. A missing record yields (nil, nil); a
// corrupt record is discarded and also yields (nil, nil).
func (s *attemptStore) Load(repoID uint32) (*persistedAttempt, error) {
	b, err := os.ReadFile(s.path(repoID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	a, err := decodeAttemptRecord(b)
	if err != nil {
		errorLog.Printf("discarding corrupt attempt record for repo %d: %s", repoID, err)
		if rmErr := os.Remove(s.path(repoID)); rmErr != nil && !os.IsNotExist(rmErr) {
			errorLog.Printf("removing corrupt attempt record for repo %d: %s", repoID, rmErr)
		}
		return nil, nil
	}
	return a, nil
}

func decodeAttemptRecord(b []byte) (*persistedAttempt, error) {
	var a persistedAttempt
	if err := json.Unmarshal(b, &a); err != nil {
		return nil, err
	}
	if a.Version != attemptRecordVersion {
		return nil, nil
	}
	return &a, nil
}

// List returns all readable attempt records. Corrupt records are discarded.
func (s *attemptStore) List() ([]*persistedAttempt, error) {
	files, err := filepath.Glob(filepath.Join(s.dir, "*.json"))
	if err != nil {
		return nil, err
	}

	var records []*persistedAttempt
	for _, fn := range files {
		b, err := os.ReadFile(fn)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		a, err := decodeAttemptRecord(b)
		if err != nil || a == nil {
			errorLog.Printf("discarding corrupt attempt record %s: %v", fn, err)
			if rmErr := os.Remove(fn); rmErr != nil && !os.IsNotExist(rmErr) {
				errorLog.Printf("removing corrupt attempt record %s: %s", fn, rmErr)
			}
			continue
		}
		records = append(records, a)
	}
	return records, nil
}

// Delete removes the record for repoID. A missing record is not an error.
func (s *attemptStore) Delete(repoID uint32) error {
	if err := os.Remove(s.path(repoID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// DeleteIfMatch removes the record for repoID only when it belongs to
// attemptID. It is used to ensure a superseded attempt cannot delete a record
// written by the attempt that replaced it.
func (s *attemptStore) DeleteIfMatch(repoID uint32, attemptID int64) error {
	a, err := s.Load(repoID)
	if err != nil || a == nil {
		return err
	}
	if a.AttemptID != attemptID {
		return nil
	}
	return s.Delete(repoID)
}

// DeleteTenant removes all records belonging to tenantID.
func (s *attemptStore) DeleteTenant(tenantID int) error {
	records, err := s.List()
	if err != nil {
		return err
	}
	var errs error
	for _, a := range records {
		if a.Options.TenantID != tenantID {
			continue
		}
		if err := s.Delete(a.RepoID); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

// branchesEqual reports whether want and got describe the same branch
// versions, compared by branch name. Order is irrelevant.
func branchesEqual(want, got []zoekt.RepositoryBranch) bool {
	if len(want) != len(got) {
		return false
	}
	versions := make(map[string]string, len(got))
	for _, b := range got {
		versions[b.Name] = b.Version
	}
	for _, b := range want {
		if v, ok := versions[b.Name]; !ok || v != b.Version {
			return false
		}
	}
	return true
}
