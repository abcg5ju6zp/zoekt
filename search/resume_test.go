package search

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"testing"

	"github.com/sourcegraph/zoekt"
	"github.com/sourcegraph/zoekt/index"
)

// eventSender records events up to tokenBatches confirmed batches, then
// simulates a disconnect: it blocks the search loop (so later shards are not
// dispatched) and drops subsequent events until the context is canceled.
type eventSender struct {
	events       []*zoekt.SearchResult
	ctx          context.Context
	cancel       context.CancelFunc
	tokenBatches int

	tokens       [][]byte
	disconnected bool
}

func (s *eventSender) Send(e *zoekt.SearchResult) {
	if s.disconnected {
		// The client is gone; late in-flight events are discarded and must
		// not be part of the client-side confirmed prefix.
		return
	}
	cp := *e
	s.events = append(s.events, &cp)
	if len(e.ResumeToken) > 0 {
		s.tokens = append(s.tokens, e.ResumeToken)
		if len(s.tokens) == s.tokenBatches {
			// This batch was the last confirmed one: freeze the client view
			// and break the connection.
			s.disconnected = true
			go s.cancel()
			<-s.ctx.Done()
		}
	}
}

func fileNames(events []*zoekt.SearchResult) []string {
	var out []string
	for _, e := range events {
		for _, f := range e.Files {
			out = append(out, fmt.Sprintf("%s/%s", f.Repository, f.FileName))
		}
	}
	return out
}

func resumeTestShards(t *testing.T) (*shardedSearcher, []string) {
	t.Helper()
	ss := newShardedSearcher(1)
	t.Cleanup(ss.Close)

	var keys []string
	for i, name := range []string{"repo-a", "repo-b", "repo-c", "repo-d"} {
		r := &zoekt.Repository{ID: uint32(i + 1), Name: name}
		b, err := index.NewShardBuilder(r)
		if err != nil {
			t.Fatal(err)
		}
		for j := range 2 {
			doc := index.Document{
				Name:    fmt.Sprintf("%s/file-%d.go", name, j),
				Content: []byte("needle needle needle"),
			}
			if err := b.Add(doc); err != nil {
				t.Fatal(err)
			}
		}
		key := fmt.Sprintf("key-%s", name)
		keys = append(keys, key)
		ss.replace(map[string]zoekt.Searcher{key: searcherForTest(t, b)})
	}
	return ss, keys
}

func TestStreamSearchResumeExactness(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	ss, _ := resumeTestShards(t)
	q := mustParseQuery(t, "needle")
	opts := &zoekt.SearchOptions{}

	// Reference: uninterrupted legacy stream.
	ref := &eventSender{}
	if err := ss.StreamSearch(context.Background(), q, opts, ref); err != nil {
		t.Fatal(err)
	}
	wantFiles := fileNames(ref.events)
	for _, e := range ref.events {
		if len(e.ResumeToken) > 0 {
			t.Fatal("legacy StreamSearch must not emit resume tokens")
		}
	}

	for _, k := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("resume after %d batches", k), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			first := &eventSender{ctx: ctx, cancel: cancel, tokenBatches: k}
			_, err := ss.StreamSearchResume(ctx, q, opts, &zoekt.StreamResumeRequest{}, first)
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("start: %v", err)
			}
			if len(first.tokens) != k {
				t.Fatalf("start: got %d tokens, want %d", len(first.tokens), k)
			}

			tok := first.tokens[k-1]
			second := &eventSender{}
			abandoned, err := ss.StreamSearchResume(context.Background(), q, opts,
				&zoekt.StreamResumeRequest{Token: tok}, second)
			if err != nil {
				t.Fatalf("resume: %v", err)
			}
			if abandoned {
				t.Fatal("resume reported abandoned")
			}

			got := append(fileNames(first.events), fileNames(second.events)...)
			if diff := slicesCmp(got, wantFiles); diff != "" {
				t.Fatalf("files differ from uninterrupted stream (-got +want):\n%s", diff)
			}

			// The resumed call must not repeat confirmed shards.
			confirmed := map[string]bool{}
			for _, e := range first.events {
				if len(e.ResumeToken) > 0 {
					for _, f := range e.Files {
						confirmed[f.Repository] = true
					}
				}
			}
			for _, e := range second.events {
				for _, f := range e.Files {
					if confirmed[f.Repository] {
						t.Fatalf("resume re-emitted confirmed shard %s", f.Repository)
					}
				}
			}

			// The initial stats-only event is not replayed on resume.
			for _, e := range second.events {
				if len(e.Files) == 0 && len(e.ResumeToken) == 0 && e.Wait > 0 {
					t.Fatal("resume replayed the initial stats-only event")
				}
			}
		})
	}
}

func slicesCmp(a, b []string) string {
	if slices.Equal(a, b) {
		return ""
	}
	return fmt.Sprintf("got %v\nwant %v", a, b)
}

func TestStreamSearchResumeTotalMatchLimit(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	ss, _ := resumeTestShards(t)
	q := mustParseQuery(t, "needle")
	// Each shard contains 6 matches (3 per file, 2 files). Allow two shards'
	// worth before stopping.
	opts := &zoekt.SearchOptions{TotalMaxMatchCount: 10}

	ctx, cancel := context.WithCancel(context.Background())
	first := &eventSender{ctx: ctx, cancel: cancel, tokenBatches: 1}
	_, err := ss.StreamSearchResume(ctx, q, opts, &zoekt.StreamResumeRequest{}, first)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("start: %v", err)
	}

	ref := &eventSender{}
	if err := ss.StreamSearch(context.Background(), q, opts, ref); err != nil {
		t.Fatal(err)
	}
	wantFiles := fileNames(ref.events)

	second := &eventSender{}
	_, err = ss.StreamSearchResume(context.Background(), q, opts,
		&zoekt.StreamResumeRequest{Token: first.tokens[0]}, second)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	got := append(fileNames(first.events), fileNames(second.events)...)
	if diff := slicesCmp(got, wantFiles); diff != "" {
		t.Fatalf("files differ from uninterrupted limited stream:\n%s", diff)
	}
}

func TestStreamSearchResumeAbandon(t *testing.T) {
	ss, _ := resumeTestShards(t)
	q := mustParseQuery(t, "needle")
	opts := &zoekt.SearchOptions{}

	ctx, cancel := context.WithCancel(context.Background())
	first := &eventSender{ctx: ctx, cancel: cancel, tokenBatches: 1}
	_, err := ss.StreamSearchResume(ctx, q, opts, &zoekt.StreamResumeRequest{}, first)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("start: %v", err)
	}
	tok := first.tokens[0]

	abandoned, err := ss.StreamSearchResume(context.Background(), q, opts,
		&zoekt.StreamResumeRequest{Token: tok, Abandon: true}, &eventSender{})
	if err != nil {
		t.Fatalf("abandon: %v", err)
	}
	if !abandoned {
		t.Fatal("abandon not acknowledged")
	}
	_, err = ss.StreamSearchResume(context.Background(), q, opts,
		&zoekt.StreamResumeRequest{Token: tok}, &eventSender{})
	var rerr *zoekt.StreamResumeError
	if !asResumeError(err, &rerr) || rerr.Code != zoekt.StreamResumeAbandoned {
		t.Fatalf("reuse after abandon: got %v, want abandoned", err)
	}
}

func TestStreamSearchResumeDisplayLimit(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	ss, _ := resumeTestShards(t)
	q := mustParseQuery(t, "needle")
	// 4 shards with 2 files each; keep the best 3 files.
	opts := &zoekt.SearchOptions{MaxDocDisplayCount: 3}

	ctx, cancel := context.WithCancel(context.Background())
	first := &eventSender{ctx: ctx, cancel: cancel, tokenBatches: 1}
	_, err := ss.StreamSearchResume(ctx, q, opts, &zoekt.StreamResumeRequest{}, first)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("start: %v", err)
	}

	ref := &eventSender{}
	if err := ss.StreamSearch(context.Background(), q, opts, ref); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	wantFiles := fileNames(ref.events)

	second := &eventSender{}
	_, err = ss.StreamSearchResume(context.Background(), q, opts,
		&zoekt.StreamResumeRequest{Token: first.tokens[0]}, second)
	// Display truncation cancels the internal context, not the caller's.
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("resume: %v", err)
	}
	got := append(fileNames(first.events), fileNames(second.events)...)
	if diff := slicesCmp(got, wantFiles); diff != "" {
		t.Fatalf("display-limited files differ from uninterrupted stream:\n%s", diff)
	}
}

func asResumeError(err error, target **zoekt.StreamResumeError) bool {
	for err != nil {
		if e, ok := err.(*zoekt.StreamResumeError); ok {
			*target = e
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
