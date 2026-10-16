package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/sourcegraph/log/logtest"
	"github.com/stretchr/testify/require"

	"github.com/sourcegraph/zoekt"
	configv1 "github.com/sourcegraph/zoekt/cmd/zoekt-sourcegraph-indexserver/grpc/protos/sourcegraph/zoekt/configuration/v1"
)

func testBranches(version string) []zoekt.RepositoryBranch {
	return []zoekt.RepositoryBranch{{Name: "HEAD", Version: version}}
}

func testAttemptOpts(id uint32, name, version string) IndexOptions {
	return IndexOptions{
		RepoID:   id,
		Name:     name,
		CloneURL: "http://api.test/.internal/git/" + name,
		Branches: testBranches(version),
		TenantID: 1,
	}
}

func TestBranchesEqual(t *testing.T) {
	head := func(v string) []zoekt.RepositoryBranch {
		return testBranches(v)
	}
	dev := func(v string) []zoekt.RepositoryBranch {
		return []zoekt.RepositoryBranch{
			{Name: "dev", Version: v},
			{Name: "HEAD", Version: "head-" + v},
		}
	}

	for _, tc := range []struct {
		name string
		a, b []zoekt.RepositoryBranch
		want bool
	}{
		{"both empty", nil, nil, true},
		{"same", head("aaa"), head("aaa"), true},
		{"different version", head("aaa"), head("bbb"), false},
		{"different length", head("aaa"), dev("aaa"), false},
		{"order independent", dev("ccc"), []zoekt.RepositoryBranch{
			{Name: "HEAD", Version: "head-ccc"},
			{Name: "dev", Version: "ccc"},
		}, true},
		{"name changed", head("aaa"), []zoekt.RepositoryBranch{{Name: "main", Version: "aaa"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, branchesEqual(tc.a, tc.b))
		})
	}
}

func TestAttemptStore(t *testing.T) {
	store := newAttemptStore(t.TempDir())

	got, err := store.Load(42)
	require.NoError(t, err)
	require.Nil(t, got)

	rec := &persistedAttempt{
		RepoID:        42,
		AttemptID:     7,
		Stage:         stageReportSuccess,
		Options:       testAttemptOpts(42, "test/repo", "aaa"),
		IndexTimeUnix: 123,
	}
	require.NoError(t, store.Save(rec))

	got, err = store.Load(42)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, attemptRecordVersion, got.Version)
	require.Equal(t, int64(7), got.AttemptID)
	require.Equal(t, stageReportSuccess, got.Stage)
	require.Equal(t, int64(123), got.IndexTimeUnix)
	if d := cmp.Diff(rec.Options, got.Options); d != "" {
		t.Fatalf("options mismatch (-want +got):\n%s", d)
	}
	require.False(t, got.UpdatedAt.IsZero())

	// A different attempt must not be able to remove the record.
	require.NoError(t, store.DeleteIfMatch(42, 8))
	got, err = store.Load(42)
	require.NoError(t, err)
	require.NotNil(t, got, "record must survive DeleteIfMatch with a different attempt ID")

	// The owning attempt can remove it.
	require.NoError(t, store.DeleteIfMatch(42, 7))
	got, err = store.Load(42)
	require.NoError(t, err)
	require.Nil(t, got)

	// Delete is idempotent.
	require.NoError(t, store.Delete(42))

	// List and per-tenant deletion.
	require.NoError(t, store.Save(&persistedAttempt{RepoID: 1, Options: IndexOptions{RepoID: 1, TenantID: 1}, Stage: stageIndexing}))
	require.NoError(t, store.Save(&persistedAttempt{RepoID: 2, Options: IndexOptions{RepoID: 2, TenantID: 2}, Stage: stageIndexing}))

	records, err := store.List()
	require.NoError(t, err)
	require.Len(t, records, 2)

	require.NoError(t, store.DeleteTenant(1))
	records, err = store.List()
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, uint32(2), records[0].RepoID)

	// Corrupt records are discarded rather than failing List.
	require.NoError(t, os.WriteFile(store.path(3), []byte("{not json"), 0o600))
	records, err = store.List()
	require.NoError(t, err)
	require.Len(t, records, 1)
	if _, err := os.Stat(store.path(3)); !os.IsNotExist(err) {
		t.Fatalf("corrupt record should have been removed, stat err: %v", err)
	}
}

func TestQueueAttemptGeneration(t *testing.T) {
	q := NewQueue(0, 0, logtest.Scoped(t))

	v1 := testAttemptOpts(1, "test/repo", "v1")
	q.AddOrUpdate(v1)
	item1, ok := q.Pop()
	require.True(t, ok)
	require.True(t, q.attemptValid(item1.attempt))

	// New configuration arrives while the first attempt is running.
	v2 := testAttemptOpts(1, "test/repo", "v2")
	q.AddOrUpdate(v2)
	require.False(t, q.attemptValid(item1.attempt), "the old attempt must be invalidated")

	// A late success (or failure) must not be committed to the newer state.
	require.False(t, q.Complete(item1.attempt, indexStateSuccess))
	require.False(t, q.Complete(item1.attempt, indexStateFail))

	item2, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, "v2", item2.Opts.Branches[0].Version)
	require.True(t, q.attemptValid(item2.attempt))

	// Completing the current attempt applies and drains the queue.
	require.True(t, q.Complete(item2.attempt, indexStateSuccess))
	_, ok = q.Pop()
	require.False(t, ok)
}

func TestQueueCompleteFailureBackoff(t *testing.T) {
	// A stale failure must not install backoff; a real failure does.
	q := NewQueue(time.Millisecond, 10*time.Millisecond, logtest.Scoped(t))

	stale := testAttemptOpts(1, "repo-1", "a")
	q.AddOrUpdate(stale)
	item1, _ := q.Pop()
	q.AddOrUpdate(testAttemptOpts(1, "repo-1", "b")) // invalidates item1
	require.False(t, q.Complete(item1.attempt, indexStateFail))

	// No backoff was installed for the newer configuration: Bump is allowed
	// immediately (well before the 1ms backoff window).
	require.Empty(t, q.Bump([]uint32{1}))
	_, ok := q.Pop()
	require.True(t, ok, "stale failure must not back off the current configuration")

	// A genuine failure applies the backoff; immediate re-enqueue is blocked.
	fail := testAttemptOpts(2, "repo-2", "a")
	q.AddOrUpdate(fail)
	item2, _ := q.Pop()
	require.True(t, q.Complete(item2.attempt, indexStateFail))
	q.Bump([]uint32{2})
	_, ok = q.Pop()
	require.False(t, ok, "backoff must block immediate re-enqueue after a real failure")

	time.Sleep(5 * time.Millisecond)
	require.Empty(t, q.Bump([]uint32{2}))
	_, ok = q.Pop()
	require.True(t, ok)
}

func TestQueueRecoverAndRemoveHook(t *testing.T) {
	q := NewQueue(0, 0, logtest.Scoped(t))

	var removed []uint32
	q.onRemove = func(id uint32) { removed = append(removed, id) }

	q.AddOrUpdate(testAttemptOpts(1, "repo-1", "a"))
	q.AddOrUpdate(testAttemptOpts(2, "repo-2", "a"))

	require.Equal(t, []uint32{1}, q.MaybeRemoveMissing([]uint32{2}))
	require.Equal(t, []uint32{1}, removed)

	// Recovery enqueues persisted work unconditionally.
	q.Recover(testAttemptOpts(3, "repo-3", "a"))
	var recovered []uint32
	for {
		item, ok := q.Pop()
		if !ok {
			break
		}
		recovered = append(recovered, item.Opts.RepoID)
	}
	require.ElementsMatch(t, []uint32{2, 3}, recovered)
}

// attemptHarness is a hermetic server setup: external commands and metadata
// lookups are stubbed, Sourcegraph records status reports.
type attemptHarness struct {
	t      *testing.T
	s      *Server
	q      *Queue
	sg     *recordingSourcegraph
	opts   IndexOptions
	cmds   []string
	indexN int

	// indexErr is returned by the stubbed zoekt-git-index subprocess.
	indexErr error
	// diskBranches describes the shard visible on disk. nil means no shard.
	diskBranches []zoekt.RepositoryBranch
	// diskIndexTime is the IndexTime of the shard visible on disk.
	diskIndexTime time.Time
	// duringIndex runs inside the stubbed zoekt-git-index subprocess.
	duringIndex func()
}

func newAttemptHarness(t *testing.T, opts IndexOptions) *attemptHarness {
	t.Helper()

	sg := &recordingSourcegraph{opts: opts}
	s := &Server{
		Sourcegraph:      sg,
		IndexDir:         t.TempDir(),
		CPUCount:         1,
		IndexConcurrency: 1,
		logger:           logtest.Scoped(t),
		timeout:          time.Minute,
	}
	q := NewQueue(time.Millisecond, 10*time.Millisecond, logtest.Scoped(t))
	s.queue = q

	h := &attemptHarness{t: t, s: s, q: q, sg: sg, opts: opts, diskIndexTime: time.Unix(123, 0)}

	s.runCmdOverride = func(cmd *exec.Cmd) error {
		h.cmds = append(h.cmds, cmd.Args[0])
		if cmd.Args[0] == "zoekt-git-index" {
			h.indexN++
			if h.duringIndex != nil {
				h.duringIndex()
			}
			if h.indexErr != nil {
				return h.indexErr
			}
		}
		return nil
	}
	s.metadataOverride = func(args *indexArgs) (*zoekt.Repository, *zoekt.IndexMetadata, bool, error) {
		if h.diskBranches == nil {
			return nil, nil, false, nil
		}
		return &zoekt.Repository{
			ID:       args.RepoID,
			Name:     args.Name,
			TenantID: args.TenantID,
			Branches: h.diskBranches,
		}, &zoekt.IndexMetadata{
			IndexTime: h.diskIndexTime,
		}, true, nil
	}

	return h
}

func (h *attemptHarness) store() *attemptStore {
	return h.s.attemptStoreFor()
}

// pop enqueues the current opts (if not already queued), pops an attempt and
// builds args pinned to it.
func (h *attemptHarness) pop() *indexArgs {
	h.q.AddOrUpdate(h.opts)
	item, ok := h.q.Pop()
	require.True(h.t, ok)
	args := h.s.indexArgs(item.Opts)
	args.Attempt = item.attempt
	return args
}

// requeue pops the item again after it left the heap (e.g. via Bump).
func (h *attemptHarness) requeue() *indexArgs {
	require.Empty(h.t, h.q.Bump([]uint32{h.opts.RepoID}))
	item, ok := h.q.Pop()
	require.True(h.t, ok)
	args := h.s.indexArgs(item.Opts)
	args.Attempt = item.attempt
	return args
}

func (h *attemptHarness) run(args *indexArgs) (indexState, error) {
	return h.s.index(context.Background(), args)
}

func (h *attemptHarness) requireNoRecord() {
	h.t.Helper()
	rec, err := h.store().Load(h.opts.RepoID)
	require.NoError(h.t, err)
	require.Nil(h.t, rec, "attempt record should not exist")
}

func (h *attemptHarness) requireRecord(stage attemptStage) *persistedAttempt {
	h.t.Helper()
	rec, err := h.store().Load(h.opts.RepoID)
	require.NoError(h.t, err)
	require.NotNil(h.t, rec)
	require.Equal(h.t, stage, rec.Stage)
	return rec
}

func (h *attemptHarness) requireStatus(n int, state configv1.UpdateIndexStatusRequest_Repository_State, version string) {
	h.t.Helper()
	require.Len(h.t, h.sg.updates, n)
	last := h.sg.updates[n-1]
	require.Len(h.t, last, 1)
	require.Equal(h.t, h.opts.RepoID, last[0].RepoID)
	require.Equal(h.t, state, last[0].State)
	require.Equal(h.t, version, last[0].Branches[0].Version)
}

func TestIndexAttemptSuccess(t *testing.T) {
	h := newAttemptHarness(t, testAttemptOpts(42, "test/repo", "aaa"))
	h.diskBranches = testBranches("aaa")

	args := h.pop()
	state, err := h.run(args)
	require.NoError(t, err)
	require.Equal(t, indexStateSuccess, state)

	// One build, one success report, record removed afterwards.
	require.Equal(t, 1, h.indexN)
	h.requireStatus(1, configv1.UpdateIndexStatusRequest_Repository_STATE_SUCCESS, "aaa")
	h.requireNoRecord()

	// The queue accepts the result and the repository is now indexed.
	require.True(t, h.q.Complete(args.Attempt, indexStateSuccess))
	_, ok := h.q.Pop()
	require.False(t, ok)
}

func TestIndexAttemptSuccessReportRetriedWithoutRebuild(t *testing.T) {
	h := newAttemptHarness(t, testAttemptOpts(42, "test/repo", "aaa"))
	h.diskBranches = testBranches("aaa")

	args := h.pop()

	// First run: shard built, but the status API times out (response lost).
	h.sg.updateErr = errors.New("status api timeout")
	state, err := h.run(args)
	require.NoError(t, err)
	require.Equal(t, indexStateSuccess, state)
	require.Equal(t, 1, h.indexN)
	h.requireRecord(stageReportSuccess)

	// Queue accepts the local success (disk is current), then re-enqueues on
	// the next sync.
	require.True(t, h.q.Complete(args.Attempt, indexStateSuccess))

	// Retry: reconcile against the existing shard and only resend the report.
	h.sg.updateErr = nil
	args2 := h.requeue()
	state, err = h.run(args2)
	require.NoError(t, err)
	require.Equal(t, indexStateSuccess, state)
	require.Equal(t, 1, h.indexN, "retry must not rebuild the same version")
	h.requireStatus(2, configv1.UpdateIndexStatusRequest_Repository_STATE_SUCCESS, "aaa")
	h.requireNoRecord()
}

func TestIndexAttemptFailureReportRetriedWithoutRebuild(t *testing.T) {
	h := newAttemptHarness(t, testAttemptOpts(42, "test/repo", "aaa"))
	// No shard on disk: the build genuinely failed.
	h.indexErr = errors.New("zoekt-git-index exploded")

	args := h.pop()

	// The failure status report itself cannot be delivered either.
	h.sg.updateErr = errors.New("status api timeout")
	state, err := h.run(args)
	require.Error(t, err)
	require.Equal(t, indexStateFail, state)
	require.Equal(t, 1, h.indexN)
	rec := h.requireRecord(stageReportFailure)
	require.Contains(t, rec.FailureMessage, "zoekt-git-index exploded")
	h.requireStatus(1, configv1.UpdateIndexStatusRequest_Repository_STATE_FAILURE, "aaa")

	require.True(t, h.q.Complete(args.Attempt, indexStateFail))

	// Retry after backoff: the failure report is resent without rerunning the
	// index subprocess.
	time.Sleep(5 * time.Millisecond)
	h.sg.updateErr = nil
	args2 := h.requeue()
	before := h.indexN
	state, err = h.run(args2)
	require.Error(t, err)
	require.Equal(t, indexStateFail, state)
	require.Equal(t, before, h.indexN, "retry must resend the report, not rerun the build")
	h.requireStatus(2, configv1.UpdateIndexStatusRequest_Repository_STATE_FAILURE, "aaa")
	h.requireNoRecord()
}

func TestIndexAttemptFailureWithShardUpgradesToSuccess(t *testing.T) {
	h := newAttemptHarness(t, testAttemptOpts(42, "test/repo", "aaa"))
	h.indexErr = errors.New("exit status 1")
	// Despite the non-zero subprocess exit, the wanted shard is on disk (the
	// success response was effectively lost).
	h.diskBranches = testBranches("aaa")

	args := h.pop()
	state, err := h.run(args)
	require.NoError(t, err)
	require.Equal(t, indexStateSuccess, state)

	require.Equal(t, 1, h.indexN)
	h.requireStatus(1, configv1.UpdateIndexStatusRequest_Repository_STATE_SUCCESS, "aaa")
	for _, batch := range h.sg.updates {
		require.NotEqual(t, configv1.UpdateIndexStatusRequest_Repository_STATE_FAILURE, batch[0].State,
			"a shard already matching the pinned commits must never be reported as failure")
	}
	h.requireNoRecord()
}

func TestIndexAttemptStaleBeforePull(t *testing.T) {
	h := newAttemptHarness(t, testAttemptOpts(42, "test/repo", "aaa"))

	args := h.pop()
	// Newer configuration arrives before the attempt does any work.
	h.opts = testAttemptOpts(42, "test/repo", "bbb")
	h.q.AddOrUpdate(h.opts)

	state, err := h.run(args)
	require.NoError(t, err)
	require.Equal(t, indexStateStale, state)

	require.Empty(t, h.cmds, "a stale attempt must not pull or index")
	require.Empty(t, h.sg.updates, "a stale attempt must not report status")
	h.requireNoRecord()
	require.False(t, h.q.Complete(args.Attempt, indexStateSuccess))

	// The newer configuration remains queued as unindexed.
	item, ok := h.q.Pop()
	require.True(t, ok)
	require.Equal(t, "bbb", item.Opts.Branches[0].Version)
}

func TestIndexAttemptStaleDuringSubprocess(t *testing.T) {
	h := newAttemptHarness(t, testAttemptOpts(42, "test/repo", "aaa"))
	h.diskBranches = testBranches("aaa")

	args := h.pop()
	// Configuration changes while the index subprocess is publishing.
	h.duringIndex = func() {
		h.q.AddOrUpdate(testAttemptOpts(42, "test/repo", "bbb"))
	}

	state, err := h.run(args)
	require.NoError(t, err)
	require.Equal(t, indexStateStale, state)

	require.Equal(t, 1, h.indexN)
	require.Empty(t, h.sg.updates, "the late success of an old attempt must not be announced")
	h.requireNoRecord()
	require.False(t, h.q.Complete(args.Attempt, indexStateSuccess))

	item, ok := h.q.Pop()
	require.True(t, ok)
	require.Equal(t, "bbb", item.Opts.Branches[0].Version)
}

func TestIndexAttemptSupersededRecordIsDiscarded(t *testing.T) {
	h := newAttemptHarness(t, testAttemptOpts(42, "test/repo", "bbb"))

	// Persisted record for an older configuration; "publishing" the current
	// build makes the wanted shard appear on disk.
	require.NoError(t, h.store().Save(&persistedAttempt{
		RepoID:    42,
		AttemptID: 99,
		Stage:     stageReportSuccess,
		Options:   testAttemptOpts(42, "test/repo", "aaa"),
	}))
	h.duringIndex = func() { h.diskBranches = testBranches("bbb") }

	args := h.pop()
	state, err := h.run(args)
	require.NoError(t, err)
	require.Equal(t, indexStateSuccess, state)

	require.Equal(t, 1, h.indexN, "the superseded version must not be confirmed from the record")
	h.requireStatus(1, configv1.UpdateIndexStatusRequest_Repository_STATE_SUCCESS, "bbb")
	h.requireNoRecord()
}

func TestIndexAttemptRestartRecovery(t *testing.T) {
	t.Run("unconfirmed success is confirmed without rebuild", func(t *testing.T) {
		h := newAttemptHarness(t, testAttemptOpts(42, "test/repo", "aaa"))
		h.diskBranches = testBranches("aaa")

		require.NoError(t, h.store().Save(&persistedAttempt{
			RepoID:        42,
			AttemptID:     99,
			Stage:         stageReportSuccess,
			Options:       testAttemptOpts(42, "test/repo", "aaa"),
			IndexTimeUnix: 123,
		}))

		// Simulate startup recovery.
		rec, err := h.store().Load(42)
		require.NoError(t, err)
		h.q.Recover(rec.Options)
		item, ok := h.q.Pop()
		require.True(t, ok)
		args := h.s.indexArgs(item.Opts)
		args.Attempt = item.attempt

		state, err := h.run(args)
		require.NoError(t, err)
		require.Equal(t, indexStateSuccess, state)
		require.Equal(t, 0, h.indexN, "recovery must not republish a version on disk")
		h.requireStatus(1, configv1.UpdateIndexStatusRequest_Repository_STATE_SUCCESS, "aaa")
		require.Equal(t, int64(123), h.sg.updates[0][0].IndexTimeUnix)
		h.requireNoRecord()
	})

	t.Run("interrupted build with shard present is confirmed", func(t *testing.T) {
		h := newAttemptHarness(t, testAttemptOpts(42, "test/repo", "aaa"))
		h.diskBranches = testBranches("aaa")

		require.NoError(t, h.store().Save(&persistedAttempt{
			RepoID:    42,
			AttemptID: 5,
			Stage:     stageIndexing,
			Options:   testAttemptOpts(42, "test/repo", "aaa"),
		}))
		h.q.Recover(testAttemptOpts(42, "test/repo", "aaa"))
		item, _ := h.q.Pop()
		args := h.s.indexArgs(item.Opts)
		args.Attempt = item.attempt

		state, err := h.run(args)
		require.NoError(t, err)
		require.Equal(t, indexStateSuccess, state)
		require.Equal(t, 0, h.indexN)
		h.requireStatus(1, configv1.UpdateIndexStatusRequest_Repository_STATE_SUCCESS, "aaa")
		h.requireNoRecord()
	})

	t.Run("interrupted build without shard rebuilds", func(t *testing.T) {
		h := newAttemptHarness(t, testAttemptOpts(42, "test/repo", "aaa"))
		// The subprocess finishes the shard this time.
		h.duringIndex = func() { h.diskBranches = testBranches("aaa") }

		require.NoError(t, h.store().Save(&persistedAttempt{
			RepoID:    42,
			AttemptID: 5,
			Stage:     stageIndexing,
			Options:   testAttemptOpts(42, "test/repo", "aaa"),
		}))
		h.q.Recover(testAttemptOpts(42, "test/repo", "aaa"))
		item, _ := h.q.Pop()
		args := h.s.indexArgs(item.Opts)
		args.Attempt = item.attempt

		state, err := h.run(args)
		require.NoError(t, err)
		require.Equal(t, indexStateSuccess, state)
		require.Equal(t, 1, h.indexN, "recovery without a shard must obey the normal incremental build")
		h.requireStatus(1, configv1.UpdateIndexStatusRequest_Repository_STATE_SUCCESS, "aaa")
		h.requireNoRecord()
	})

	t.Run("recoverAttempts enqueues valid records", func(t *testing.T) {
		h := newAttemptHarness(t, testAttemptOpts(42, "test/repo", "aaa"))
		require.NoError(t, h.store().Save(&persistedAttempt{
			RepoID:    42,
			Stage:     stageReportSuccess,
			Options:   testAttemptOpts(42, "test/repo", "aaa"),
			AttemptID: 1,
		}))
		require.NoError(t, h.store().Save(&persistedAttempt{
			RepoID:    43,
			Stage:     stageIndexing,
			Options:   testAttemptOpts(43, "other/repo", "ccc"),
			AttemptID: 2,
		}))

		h.s.recoverAttempts()
		require.Equal(t, 2, h.q.Len())
	})
}

func TestIndexAttemptForceBypassesRecordedFailure(t *testing.T) {
	h := newAttemptHarness(t, testAttemptOpts(42, "test/repo", "aaa"))
	// The build finishes and publishes this time.
	h.duringIndex = func() { h.diskBranches = testBranches("aaa") }

	require.NoError(t, h.store().Save(&persistedAttempt{
		RepoID:         42,
		AttemptID:      99,
		Stage:          stageReportFailure,
		Options:        testAttemptOpts(42, "test/repo", "aaa"),
		FailureMessage: "old failure",
	}))

	// Manual force reindex: no queue token, Incremental disabled.
	args := h.s.indexArgs(h.opts)
	args.Incremental = false
	require.Nil(t, args.Attempt)

	state, err := h.run(args)
	require.NoError(t, err)
	require.Equal(t, indexStateSuccess, state)

	require.Equal(t, 1, h.indexN, "force reindex must run a fresh build despite an unconfirmed failure record")
	h.requireStatus(1, configv1.UpdateIndexStatusRequest_Repository_STATE_SUCCESS, "aaa")
	h.requireNoRecord()
}

func TestIndexAttemptEmptyBranchesKeepsBehavior(t *testing.T) {
	opts := IndexOptions{RepoID: 42, Name: "empty/repo", TenantID: 1}
	h := newAttemptHarness(t, opts)

	// A record left by a previous configuration with branches must be dropped
	// when current input has no branches.
	require.NoError(t, h.store().Save(&persistedAttempt{
		RepoID:    42,
		AttemptID: 9,
		Stage:     stageReportSuccess,
		Options:   testAttemptOpts(42, "empty/repo", "aaa"),
	}))

	args := h.pop()
	state, err := h.run(args)
	require.NoError(t, err)
	require.Equal(t, indexStateEmpty, state)

	require.Empty(t, h.sg.updates, "empty shards keep the original behavior (no status report)")
	require.Equal(t, 0, h.indexN)
	h.requireNoRecord()

	// An actual empty shard was published.
	matches, err := filepath.Glob(filepath.Join(h.s.IndexDir, "*.zoekt"))
	require.NoError(t, err)
	require.NotEmpty(t, matches)
}
