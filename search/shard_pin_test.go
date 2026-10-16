package search

import (
	"fmt"
	"sync"
	"testing"

	"go.uber.org/atomic"

	"github.com/sourcegraph/zoekt"
	"github.com/sourcegraph/zoekt/index"
)

// closeCountingSearcher wraps a Searcher and counts Close calls.
type closeCountingSearcher struct {
	zoekt.Searcher
	closes atomic.Int32
}

func (c *closeCountingSearcher) Close() {
	c.closes.Add(1)
	if c.Searcher != nil {
		c.Searcher.Close()
	}
}

// newCountingSearcher returns a real index searcher wrapped to count closes.
func newCountingSearcher(t *testing.T, n int) zoekt.Searcher {
	t.Helper()
	r := &zoekt.Repository{ID: uint32(n), Name: fmt.Sprintf("test-repository-%d", n)}
	b := testShardBuilder(t, r, index.Document{
		Name:    "filename.go",
		Content: []byte("needle needle needle"),
	})
	return &closeCountingSearcher{Searcher: searcherForTest(t, b)}
}

func closerOf(r *rankedShard) *closeCountingSearcher {
	return r.Searcher.(*closeCountingSearcher)
}

func TestRankedShardPinLifecycle(t *testing.T) {
	ss := newShardedSearcher(1)

	ss.replace(map[string]zoekt.Searcher{"k": newCountingSearcher(t, 0)})
	r1 := ss.getLoaded().shards[0]

	// A pin keeps the old generation open across a replace.
	if !r1.pin() {
		t.Fatal("pin of in-service shard failed")
	}

	ss.replace(map[string]zoekt.Searcher{"k": newCountingSearcher(t, 1)})
	r2 := ss.getLoaded().shards[0]

	if closerOf(r1).closes.Load() != 0 {
		t.Fatalf("pinned old shard closed before unpin: %d", closerOf(r1).closes.Load())
	}
	if closerOf(r2).closes.Load() != 0 {
		t.Fatalf("new shard closed unexpectedly: %d", closerOf(r2).closes.Load())
	}

	r1.unpin()
	if closerOf(r1).closes.Load() != 1 {
		t.Fatalf("old shard closes after unpin: got %d, want 1", closerOf(r1).closes.Load())
	}

	// A retired shard cannot be pinned anymore; the failed pin must not unbalance
	// the counter or close the shard a second time.
	if r1.pin() {
		t.Fatal("pin on retired shard succeeded")
	}
	if got := closerOf(r1).closes.Load(); got != 1 {
		t.Fatalf("double close: got %d, want 1", got)
	}

	// Unpinned replacement closes the old shard immediately.
	ss.replace(map[string]zoekt.Searcher{"k": newCountingSearcher(t, 2)})
	r3 := ss.getLoaded().shards[0]
	if closerOf(r2).closes.Load() != 1 {
		t.Fatalf("unpinned old shard closes immediately: got %d, want 1", closerOf(r2).closes.Load())
	}

	// Dropping the last key retires the shard.
	ss.replace(map[string]zoekt.Searcher{"k": nil})
	if closerOf(r3).closes.Load() != 1 {
		t.Fatalf("dropped shard closes immediately: got %d, want 1", closerOf(r3).closes.Load())
	}
}

func TestRankedShardPinConcurrent(t *testing.T) {
	r := &rankedShard{Searcher: &closeCountingSearcher{}}
	r.retire()

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r.pin() {
				r.unpin()
			}
		}()
	}
	wg.Wait()

	if got := closerOf(r).closes.Load(); got != 1 {
		t.Fatalf("retired shard closes exactly once: got %d, want 1", got)
	}
}
