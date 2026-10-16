package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/sourcegraph/zoekt"
	webserverv1 "github.com/sourcegraph/zoekt/grpc/protos/zoekt/webserver/v1"
	"github.com/sourcegraph/zoekt/index"
	mockSearcher "github.com/sourcegraph/zoekt/internal/mockSearcher"
	"github.com/sourcegraph/zoekt/query"
	searchpkg "github.com/sourcegraph/zoekt/search"
)

// resumableBackend is a zoekt.ResumableStreamer mock for protocol tests.
type resumableBackend struct {
	resumeCalls atomic.Int32
	streamCalls atomic.Int32
	lastReq     atomic.Pointer[zoekt.StreamResumeRequest]

	terminal *zoekt.StreamResumeError
	events   []*zoekt.SearchResult
}

func (b *resumableBackend) Search(context.Context, query.Q, *zoekt.SearchOptions) (*zoekt.SearchResult, error) {
	return nil, nil
}
func (b *resumableBackend) List(context.Context, query.Q, *zoekt.ListOptions) (*zoekt.RepoList, error) {
	return nil, nil
}
func (b *resumableBackend) Close()         {}
func (b *resumableBackend) String() string { return "resumableBackend" }
func (b *resumableBackend) StreamSearch(ctx context.Context, q query.Q, opts *zoekt.SearchOptions, sender zoekt.Sender) error {
	b.streamCalls.Add(1)
	for _, e := range b.events {
		sender.Send(e)
	}
	return nil
}

func (b *resumableBackend) StreamSearchResume(
	_ context.Context,
	_ query.Q,
	_ *zoekt.SearchOptions,
	req *zoekt.StreamResumeRequest,
	sender zoekt.Sender,
) (bool, error) {
	b.resumeCalls.Add(1)
	b.lastReq.Store(req)
	if b.terminal != nil {
		return false, b.terminal
	}
	if req != nil && req.Abandon {
		return true, nil
	}
	for _, e := range b.events {
		sender.Send(e)
	}
	return false, nil
}

// legacyOnly hides StreamSearchResume so the server takes the legacy
// (non-snapshot) path.
type legacyOnly struct {
	zoekt.Streamer
}

func (l legacyOnly) StreamSearch(ctx context.Context, q query.Q, opts *zoekt.SearchOptions, sender zoekt.Sender) error {
	return l.Streamer.StreamSearch(ctx, q, opts, sender)
}

func resumeTestServer(t *testing.T, backend zoekt.Streamer) (webserverv1.WebserverServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(256 * 1024)
	gs := grpc.NewServer()
	webserverv1.RegisterWebserverServiceServer(gs, NewServer(backend))
	go func() { _ = gs.Serve(lis) }()

	cc, err := grpc.NewClient("passthrough:///resume",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		_ = cc.Close()
		gs.Stop()
		_ = lis.Close()
	}
	return webserverv1.NewWebserverServiceClient(cc), cleanup
}

func streamReq(t *testing.T, q query.Q, token []byte, abandon bool) *webserverv1.StreamSearchRequest {
	t.Helper()
	req := &webserverv1.StreamSearchRequest{
		Request: &webserverv1.SearchRequest{
			Query: query.QToProto(q),
		},
		ResumeToken:     token,
		AbandonSnapshot: abandon,
	}
	return req
}

func drain(t *testing.T, cs webserverv1.WebserverService_StreamSearchClient) []*webserverv1.StreamSearchResponse {
	t.Helper()
	var out []*webserverv1.StreamSearchResponse
	for {
		m, err := cs.Recv()
		if err != nil {
			return out
		}
		out = append(out, m)
	}
}

func TestStreamSearchResumeErrors(t *testing.T) {
	cases := []struct {
		name string
		code zoekt.StreamResumeErrorCode
		want webserverv1.StreamResumeError
	}{
		{"invalid", zoekt.StreamResumeInvalidToken, webserverv1.StreamResumeError_STREAM_RESUME_ERROR_INVALID_TOKEN},
		{"mismatch", zoekt.StreamResumeMismatch, webserverv1.StreamResumeError_STREAM_RESUME_ERROR_MISMATCH},
		{"expired", zoekt.StreamResumeExpired, webserverv1.StreamResumeError_STREAM_RESUME_ERROR_EXPIRED},
		{"restarted", zoekt.StreamResumeRestarted, webserverv1.StreamResumeError_STREAM_RESUME_ERROR_RESTARTED},
		{"completed", zoekt.StreamResumeCompleted, webserverv1.StreamResumeError_STREAM_RESUME_ERROR_COMPLETED},
		{"abandoned", zoekt.StreamResumeAbandoned, webserverv1.StreamResumeError_STREAM_RESUME_ERROR_ABANDONED},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := &resumableBackend{terminal: zoekt.NewStreamResumeError(tc.code, "test")}
			client, cleanup := resumeTestServer(t, backend)
			defer cleanup()

			cs, err := client.StreamSearch(context.Background(), streamReq(t, mustQ(t, "foo"), []byte("tok"), false))
			if err != nil {
				t.Fatal(err)
			}
			msgs := drain(t, cs)
			if len(msgs) != 1 {
				t.Fatalf("got %d messages, want 1", len(msgs))
			}
			if msgs[0].GetResponseChunk() != nil {
				t.Fatal("terminal response must not carry a chunk")
			}
			if msgs[0].GetResumeError() != tc.want {
				t.Fatalf("got %v, want %v", msgs[0].GetResumeError(), tc.want)
			}
			if backend.streamCalls.Load() != 0 || backend.resumeCalls.Load() != 1 {
				t.Fatalf("non-resumable outcome must not run a search: streams=%d resumes=%d",
					backend.streamCalls.Load(), backend.resumeCalls.Load())
			}
		})
	}
}

func TestStreamSearchAbandonAck(t *testing.T) {
	backend := &resumableBackend{}
	client, cleanup := resumeTestServer(t, backend)
	defer cleanup()

	cs, err := client.StreamSearch(context.Background(), streamReq(t, mustQ(t, "foo"), []byte("tok"), true))
	if err != nil {
		t.Fatal(err)
	}
	msgs := drain(t, cs)
	if len(msgs) != 1 || !msgs[0].GetSnapshotAbandoned() {
		t.Fatalf("got %+v, want single snapshot_abandoned ack", msgs)
	}
	if got := backend.lastReq.Load(); got == nil || !got.Abandon {
		t.Fatal("abandon not forwarded to the backend")
	}
}

func TestStreamSearchResumeUnsupported(t *testing.T) {
	// The plain adapter does not implement ResumableStreamer.
	gs := grpc.NewServer()
	webserverv1.RegisterWebserverServiceServer(gs, NewServer(adapter{&mockSearcher.MockSearcher{}}))
	ts := httptest.NewServer(h2c.NewHandler(gs, &http2.Server{}))
	defer ts.Close()
	u, _ := url.Parse(ts.URL)
	cc, err := grpc.Dial(u.Host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	client := webserverv1.NewWebserverServiceClient(cc)

	cs, err := client.StreamSearch(context.Background(), streamReq(t, mustQ(t, "foo"), []byte("tok"), false))
	if err != nil {
		t.Fatal(err)
	}
	// A streaming handler error is delivered on the first Recv rather than
	// from the StreamSearch call.
	if _, err := cs.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("got %v, want FailedPrecondition", err)
	}
}

func TestResumeTokenOnlyOnFinalChunk(t *testing.T) {
	// One SearchResult whose wire size forces chunker to emit multiple
	// messages; the resume token may only appear on the last one.
	big := strings.Repeat("x", 800*1024)
	protoFiles := []*webserverv1.FileMatch{
		{
			Repository: "repo-a",
			LineMatches: []*webserverv1.LineMatch{
				{Line: []byte(big)},
			},
		},
		{
			Repository: "repo-b",
			LineMatches: []*webserverv1.LineMatch{
				{Line: []byte(big)},
			},
		},
	}
	files := make([]zoekt.FileMatch, len(protoFiles))
	for i, f := range protoFiles {
		files[i] = zoekt.FileMatchFromProto(f)
	}
	backend := &resumableBackend{events: []*zoekt.SearchResult{{
		ResumeToken: []byte("batch-token"),
		Files:       files,
	}}}
	client, cleanup := resumeTestServer(t, backend)
	defer cleanup()

	cs, err := client.StreamSearch(context.Background(), &webserverv1.StreamSearchRequest{
		Request: &webserverv1.SearchRequest{Query: query.QToProto(mustQ(t, "foo"))},
	})
	if err != nil {
		t.Fatal(err)
	}
	msgs := drain(t, cs)
	if len(msgs) < 2 {
		t.Fatalf("expected chunked messages, got %d", len(msgs))
	}
	for i, m := range msgs[:len(msgs)-1] {
		if len(m.GetResumeToken()) > 0 {
			t.Fatalf("token attached to non-final chunk %d", i)
		}
	}
	if string(msgs[len(msgs)-1].GetResumeToken()) != "batch-token" {
		t.Fatal("final chunk must carry the token")
	}
}

// delayedResumable wraps a ResumableStreamer. gate controls how many
// file-bearing batches are forwarded before the sender blocks on the caller
// context, which lets the test deterministically disconnect between batches.
type delayedResumable struct {
	inner zoekt.ResumableStreamer
	delay time.Duration
}

func (d *delayedResumable) Search(ctx context.Context, q query.Q, opts *zoekt.SearchOptions) (*zoekt.SearchResult, error) {
	return d.inner.Search(ctx, q, opts)
}
func (d *delayedResumable) List(ctx context.Context, q query.Q, opts *zoekt.ListOptions) (*zoekt.RepoList, error) {
	return d.inner.List(ctx, q, opts)
}
func (d *delayedResumable) StreamSearch(ctx context.Context, q query.Q, opts *zoekt.SearchOptions, sender zoekt.Sender) error {
	return d.inner.StreamSearch(ctx, q, opts, sender)
}
func (d *delayedResumable) StreamSearchResume(
	ctx context.Context,
	q query.Q,
	opts *zoekt.SearchOptions,
	req *zoekt.StreamResumeRequest,
	sender zoekt.Sender,
) (bool, error) {
	// The first run is gated (its second batch blocks until cancel); the
	// reference legacy run and the resume call forward everything.
	gate := 0
	if req != nil && len(req.Token) == 0 {
		gate = 1
	}
	return d.inner.StreamSearchResume(ctx, q, opts, req, d.paced(ctx, sender, gate))
}
func (d *delayedResumable) Close()         { d.inner.Close() }
func (d *delayedResumable) String() string { return "delayedResumable" }

func (d *delayedResumable) paced(ctx context.Context, sender zoekt.Sender, gate int) zoekt.Sender {
	fileBatches := 0
	return zoekt.SenderFunc(func(r *zoekt.SearchResult) {
		if gate > 0 && len(r.Files) > 0 {
			fileBatches++
			if fileBatches > gate {
				// Simulate the client going away before this batch is sent:
				// block until this stream's context is canceled.
				<-ctx.Done()
				return
			}
		}
		if d.delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(d.delay):
			}
		}
		sender.Send(r)
	})
}

// TestStreamSearchResumeEndToEnd drives a real DirectorySearcher (through a
// pacing wrapper) over gRPC, disconnects after one confirmed batch and resumes
// to completion.
func TestStreamSearchResumeEndToEnd(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	dir := t.TempDir()
	for i, name := range []string{"repo-a", "repo-b", "repo-c"} {
		b, err := index.NewShardBuilder(&zoekt.Repository{ID: uint32(i + 1), Name: name})
		if err != nil {
			t.Fatal(err)
		}
		for j := range 2 {
			if err := b.Add(index.Document{
				Name:    fmt.Sprintf("f%d.go", j),
				Content: []byte("needle needle needle"),
			}); err != nil {
				t.Fatal(err)
			}
		}
		var buf bytes.Buffer
		if err := b.Write(&buf); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name+".zoekt"), buf.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	searcher, err := searchpkg.NewDirectorySearcher(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(searcher.Close)

	backend := &delayedResumable{
		inner: searcher.(zoekt.ResumableStreamer),
	}
	// The reference run takes the legacy, snapshot-free path; the resumable
	// server drives start/resume and shares the same inner searcher so
	// tokens validate.
	refClient, refCleanup := resumeTestServer(t, legacyOnly{backend})
	defer refCleanup()
	client, cleanup := resumeTestServer(t, backend)
	defer cleanup()

	q := mustQ(t, "needle")
	recvFiles2 := func(cs webserverv1.WebserverService_StreamSearchClient) ([]string, []*webserverv1.StreamSearchResponse) {
		var out []string
		var msgs []*webserverv1.StreamSearchResponse
		for {
			m, err := cs.Recv()
			if err != nil {
				return out, msgs
			}
			msgs = append(msgs, m)
			for _, f := range m.GetResponseChunk().GetFiles() {
				out = append(out, f.GetRepository()+"/"+string(f.GetFileName()))
			}
		}
	}
	recvFiles := func(cs webserverv1.WebserverService_StreamSearchClient) []string {
		files, _ := recvFiles2(cs)
		return files
	}

	// Uninterrupted reference run (cancel after drain).
	refCtx, refCancel := context.WithCancel(context.Background())
	refStream, err := refClient.StreamSearch(refCtx, streamReq(t, q, nil, false))
	if err != nil {
		t.Fatal(err)
	}
	wantFiles := recvFiles(refStream)
	refCancel()
	if len(wantFiles) != 6 {
		t.Fatalf("reference files: got %d, want 6 (%v)", len(wantFiles), wantFiles)
	}

	// First run: stop after receiving the first batch token. A concurrent
	// drainer mimics the client continuously reading the stream, which is what
	// transports the cancellation to the server.
	ctx1, cancel1 := context.WithCancel(context.Background())
	var (
		token      []byte
		firstFiles []string
		tokenSeen  = make(chan struct{})
		firstDone  = make(chan struct{})
	)
	cs1, err := client.StreamSearch(ctx1, streamReq(t, q, nil, false))
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(firstDone)
		for {
			m, err := cs1.Recv()
			if err != nil {
				return
			}
			for _, f := range m.GetResponseChunk().GetFiles() {
				firstFiles = append(firstFiles, f.GetRepository()+"/"+string(f.GetFileName()))
			}
			if len(m.GetResumeToken()) > 0 {
				token = m.GetResumeToken()
				close(tokenSeen)
			}
		}
	}()
	select {
	case <-tokenSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("no token on first run")
	}
	cancel1()
	select {
	case <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not observe disconnect")
	}
	if len(token) == 0 {
		t.Fatal("no resume token observed on first run")
	}

	// Resume with the token and read to EOF.
	cs2, err := client.StreamSearch(context.Background(), streamReq(t, q, token, false))
	if err != nil {
		t.Fatal(err)
	}
	restFiles, restMsgs := recvFiles2(cs2)
	for _, m := range restMsgs {
		if e := m.GetResumeError(); e != 0 {
			t.Fatalf("resume terminal error: %v token=%q", e, string(token)[:8])
		}
	}

	got := append(firstFiles, restFiles...)
	if len(got) != len(wantFiles) {
		t.Fatalf("resumed files count: got %d (%v), want %d (%v)", len(got), got, len(wantFiles), wantFiles)
	}
	for i := range wantFiles {
		if got[i] != wantFiles[i] {
			t.Fatalf("resumed files differ at %d:\n got %v\nwant %v", i, got, wantFiles)
		}
	}

	// The confirmed shard must not be emitted again on resume.
	if len(firstFiles) > 0 {
		firstRepo := strings.SplitN(firstFiles[0], "/", 2)[0]
		for _, name := range restFiles {
			if strings.HasPrefix(name, firstRepo+"/") {
				t.Fatalf("resume re-emitted confirmed repo %s: %q", firstRepo, name)
			}
		}
	}
}

func mustQ(t *testing.T, s string) query.Q {
	t.Helper()
	q, err := query.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return q
}
