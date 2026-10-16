package search

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sourcegraph/zoekt"
	"github.com/sourcegraph/zoekt/index"
)

// writeResumeShardFile builds a one-repo shard with one document containing
// content and writes it as <name>.zoekt.
func writeResumeShardFile(t *testing.T, dir, repo, content string) {
	t.Helper()
	b := testShardBuilder(t, &zoekt.Repository{ID: repoID(repo), Name: repo}, index.Document{
		Name:    "doc.txt",
		Content: []byte(content),
	})
	var buf bytes.Buffer
	if err := b.Write(&buf); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, repo+".zoekt"), buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}

func repoID(name string) uint32 {
	switch name {
	case "repo-a":
		return 101
	case "repo-b":
		return 102
	default:
		return 999
	}
}

// eventContents returns the matched line contents emitted for file-bearing
// events.
func eventContents(events []*zoekt.SearchResult) []string {
	var out []string
	for _, e := range events {
		for _, f := range e.Files {
			for _, lm := range f.LineMatches {
				out = append(out, string(lm.Line))
			}
			for _, cm := range f.ChunkMatches {
				out = append(out, string(cm.Content))
			}
		}
	}
	return out
}

// TestResumeIsolatedFromHotReload replaces a shard (new generation, different
// content) and removes another while the client holds a resume token. The
// resumed stream must read the pinned old generation exclusively.
func TestResumeIsolatedFromHotReload(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	dir := t.TempDir()
	writeResumeShardFile(t, dir, "repo-a", "needle alpha")
	writeResumeShardFile(t, dir, "repo-b", "needle beta")

	var opens atomic.Int32
	searcher, err := NewDirectorySearcherWithOptions(dir, DirectorySearcherOptions{
		IndexFileOpener: func(f *os.File) (index.IndexFile, error) {
			opens.Add(1)
			return index.NewIndexFile(f)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(searcher.Close)
	rs := searcher.(zoekt.ResumableStreamer)

	q := mustParseQuery(t, "needle")
	opts := &zoekt.SearchOptions{}

	// Reference uninterrupted run, captured before the hot reload.
	ref := &eventSender{}
	if err := searcher.StreamSearch(context.Background(), q, opts, ref); err != nil {
		t.Fatal(err)
	}
	want := eventContents(ref.events)
	if len(want) != 2 || !containsAll(want, "needle alpha", "needle beta") {
		t.Fatalf("reference contents: %v", want)
	}

	// First resumable run, stopped after the first confirmed batch.
	ctx, cancel := context.WithCancel(context.Background())
	first := &eventSender{ctx: ctx, cancel: cancel, tokenBatches: 1}
	_, err = rs.StreamSearchResume(ctx, q, opts, &zoekt.StreamResumeRequest{}, first)
	if err != nil && err != context.Canceled {
		t.Fatalf("start: %v", err)
	}
	if len(first.tokens) != 1 {
		t.Fatalf("start tokens: got %d", len(first.tokens))
	}
	tok := first.tokens[0]

	// Hot reload: replace repo-a generation and remove repo-b entirely.
	writeResumeShardFile(t, dir, "repo-a", "needle gamma")
	if err := os.Remove(filepath.Join(dir, "repo-b.zoekt")); err != nil {
		t.Fatal(err)
	}
	deadline := testDeadline(t, 10*time.Second)
	if !waitForPredicate(deadline, 10*time.Millisecond, func() bool {
		return opens.Load() >= 3 // initial 2 + at least one reload
	}) {
		t.Fatalf("watcher did not reload: opens=%d", opens.Load())
	}
	// Give the batch reload a moment to process the drop as well.
	time.Sleep(100 * time.Millisecond)

	// The current shard set must serve the NEW generation now.
	current := &eventSender{}
	if err := searcher.StreamSearch(context.Background(), q, opts, current); err != nil {
		t.Fatal(err)
	}
	if containsAll(eventContents(current.events), "needle alpha", "needle beta") {
		t.Fatalf("legacy stream still served the old generation: %v", eventContents(current.events))
	}

	// Resume against the token: reads stay confined to the snapshot's pinned
	// generation, so the re-searched shard still returns old content even
	// though its file was replaced, and the deleted shard remains readable.
	second := &eventSender{}
	abandoned, err := rs.StreamSearchResume(context.Background(), q, opts,
		&zoekt.StreamResumeRequest{Token: tok}, second)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if abandoned {
		t.Fatal("resume reported abandoned")
	}
	got := append(eventContents(first.events), eventContents(second.events)...)
	if len(got) != len(want) || !sameMultiset(got, want) {
		t.Fatalf("resumed contents: got %v, want %v", got, want)
	}
	if !containsAll(got, "needle alpha", "needle beta") {
		t.Fatalf("resume lost pinned shards: %v", got)
	}
	if strings.Contains(strings.Join(got, "|"), "gamma") {
		t.Fatalf("resume leaked the new generation: %v", got)
	}
}

func containsAll(haystack []string, needles ...string) bool {
	for _, n := range needles {
		found := false
		for _, h := range haystack {
			if strings.Contains(h, n) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func sameMultiset(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := map[string]int{}
	for _, s := range a {
		counts[s]++
	}
	for _, s := range b {
		counts[s]--
		if counts[s] < 0 {
			return false
		}
	}
	return true
}
