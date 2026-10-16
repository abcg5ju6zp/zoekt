package search

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"testing"
	"time"

	"github.com/sourcegraph/zoekt"
	"github.com/sourcegraph/zoekt/query"
)

func TestResumeTokenCodec(t *testing.T) {
	creds := newResumeCredentials()
	other := newResumeCredentials()
	id := make([]byte, snapshotIDSize)
	_, _ = rand.Read(id)
	expiry := time.Now().Add(time.Hour).Truncate(time.Second)

	tok := encodeResumeToken(creds, id, 3, expiry)
	if _, err := base64.RawURLEncoding.DecodeString(string(tok)); err != nil {
		t.Fatalf("token not raw-base64url: %v", err)
	}

	dec, rerr := decodeResumeToken(tok, creds, time.Now())
	if rerr != nil {
		t.Fatalf("valid token rejected: %v", rerr)
	}
	if dec.seq != 3 || dec.snapshotID != hex.EncodeToString(id) || !dec.expiresAt.Equal(expiry) {
		t.Fatalf("unexpected decoded token: %+v", dec)
	}

	cases := []struct {
		name string
		tok  []byte
		code zoekt.StreamResumeErrorCode
	}{
		{"corrupt base64", []byte("!!!!not base64!!!!"), zoekt.StreamResumeInvalidToken},
		{"truncated", tok[:len(tok)-8], zoekt.StreamResumeInvalidToken},
		{"wrong instance", encodeResumeToken(other, id, 3, expiry), zoekt.StreamResumeRestarted},
		{"expired", encodeResumeToken(creds, id, 3, time.Now().Add(-time.Second).Truncate(time.Second)), zoekt.StreamResumeExpired},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, rerr := decodeResumeToken(tc.tok, creds, time.Now())
			if rerr == nil || rerr.Code != tc.code {
				t.Fatalf("got %v, want code %v", rerr, tc.code)
			}
		})
	}

	// Tamper with one authenticated byte: HMAC must fail (not restart, not
	// expired).
	raw, _ := base64.RawURLEncoding.DecodeString(string(tok))
	raw[len(raw)-hmacSize-1] ^= 0xff
	tampered := make([]byte, base64.RawURLEncoding.EncodedLen(len(raw)))
	base64.RawURLEncoding.Encode(tampered, raw)
	if _, rerr := decodeResumeToken(tampered, creds, time.Now()); rerr == nil || rerr.Code != zoekt.StreamResumeInvalidToken {
		t.Fatalf("tampered token: got %v, want invalid_token", rerr)
	}
}

func newTestRegistry(now time.Time) (*snapshotRegistry, func()) {
	r := newSnapshotRegistryWithClock(func() time.Time { return now }, time.Minute)
	return r, r.stop
}

func newTestSnapshot(t *testing.T, r *snapshotRegistry, q query.Q, opts *zoekt.SearchOptions, tenantID int) *searchSnapshot {
	t.Helper()
	qfp, err := fingerprintQuery(q)
	if err != nil {
		t.Fatal(err)
	}
	ofp, err := fingerprintOptions(opts)
	if err != nil {
		t.Fatal(err)
	}
	idRaw := make([]byte, snapshotIDSize)
	_, _ = rand.Read(idRaw)
	now := r.now()
	s := &searchSnapshot{
		id:        hex.EncodeToString(idRaw),
		idRaw:     idRaw,
		createdAt: now,
		expiresAt: now.Add(r.ttl),
		queryFP:   qfp,
		optsFP:    ofp,
		tenantID:  tenantID,
		creds:     r.creds,
		registry:  r,
	}
	r.start(s)
	return s
}

func tokenFor(s *searchSnapshot) []byte {
	return encodeResumeToken(s.creds, s.idRaw, 0, s.expiresAt)
}

func TestSnapshotRegistryResumeValidation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	r, stop := newTestRegistry(now)
	defer stop()

	q1 := mustParseQuery(t, "foo")
	opts1 := &zoekt.SearchOptions{Whole: true}

	s := newTestSnapshot(t, r, q1, opts1, 7)
	valid := tokenFor(s)

	t.Run("unknown snapshot id", func(t *testing.T) {
		unknown := encodeResumeToken(r.creds, []byte("1234567890abcdef"), 0, now.Add(time.Minute))
		_, rerr := r.resume(unknown, q1, opts1, 7)
		if rerr == nil || rerr.Code != zoekt.StreamResumeRestarted {
			t.Fatalf("got %v, want restarted", rerr)
		}
	})

	t.Run("valid resume", func(t *testing.T) {
		view, rerr := r.resume(valid, q1, opts1, 7)
		if rerr != nil {
			t.Fatalf("valid resume rejected: %v", rerr)
		}
		if view.snapshot != s {
			t.Fatal("resume returned wrong snapshot")
		}
	})

	t.Run("query mismatch", func(t *testing.T) {
		_, rerr := r.resume(valid, mustParseQuery(t, "bar"), opts1, 7)
		if rerr == nil || rerr.Code != zoekt.StreamResumeMismatch {
			t.Fatalf("got %v, want mismatch", rerr)
		}
	})

	t.Run("options mismatch", func(t *testing.T) {
		_, rerr := r.resume(valid, q1, &zoekt.SearchOptions{ChunkMatches: true}, 7)
		if rerr == nil || rerr.Code != zoekt.StreamResumeMismatch {
			t.Fatalf("got %v, want mismatch", rerr)
		}
	})

	t.Run("tenant mismatch", func(t *testing.T) {
		_, rerr := r.resume(valid, q1, opts1, 8)
		if rerr == nil || rerr.Code != zoekt.StreamResumeMismatch {
			t.Fatalf("got %v, want mismatch", rerr)
		}
	})

	t.Run("no tenant vs tenant", func(t *testing.T) {
		_, rerr := r.resume(valid, q1, opts1, 0)
		if rerr == nil || rerr.Code != zoekt.StreamResumeMismatch {
			t.Fatalf("got %v, want mismatch", rerr)
		}
	})

	t.Run("seq ahead of snapshot", func(t *testing.T) {
		ahead := encodeResumeToken(r.creds, s.idRaw, 99, now.Add(time.Minute))
		_, rerr := r.resume(ahead, q1, opts1, 7)
		if rerr == nil || rerr.Code != zoekt.StreamResumeInvalidToken {
			t.Fatalf("got %v, want invalid_token", rerr)
		}
	})

	t.Run("completed", func(t *testing.T) {
		r.finish(s)
		_, rerr := r.resume(valid, q1, opts1, 7)
		if rerr == nil || rerr.Code != zoekt.StreamResumeCompleted {
			t.Fatalf("got %v, want completed", rerr)
		}
	})

	t.Run("abandoned", func(t *testing.T) {
		s2 := newTestSnapshot(t, r, q1, opts1, 7)
		tok2 := tokenFor(s2)
		if rerr := r.abandon(tok2, q1, opts1, 7); rerr != nil {
			t.Fatalf("abandon: %v", rerr)
		}
		_, rerr := r.resume(tok2, q1, opts1, 7)
		if rerr == nil || rerr.Code != zoekt.StreamResumeAbandoned {
			t.Fatalf("got %v, want abandoned", rerr)
		}
	})

	t.Run("expired status", func(t *testing.T) {
		s3 := newTestSnapshot(t, r, q1, opts1, 7)
		s3.mu.Lock()
		s3.status = snapshotExpired
		s3.mu.Unlock()
		_, rerr := r.resume(tokenFor(s3), q1, opts1, 7)
		if rerr == nil || rerr.Code != zoekt.StreamResumeExpired {
			t.Fatalf("got %v, want expired", rerr)
		}
	})

	t.Run("expired credential of removed snapshot", func(t *testing.T) {
		exp := encodeResumeToken(r.creds, []byte("fedcba0987654321"), 0, now.Add(-time.Second))
		_, rerr := r.resume(exp, q1, opts1, 7)
		if rerr == nil || rerr.Code != zoekt.StreamResumeExpired {
			t.Fatalf("got %v, want expired", rerr)
		}
	})
}

func TestFingerprints(t *testing.T) {
	fq1, _ := fingerprintQuery(mustParseQuery(t, "foo bar"))
	fq2, _ := fingerprintQuery(mustParseQuery(t, "(foo bar)"))
	if fq1 != fq2 {
		t.Fatal("syntactically different but equivalent queries must normalize equal")
	}
	fqOther, _ := fingerprintQuery(mustParseQuery(t, "foo baz"))
	if fq1 == fqOther {
		t.Fatal("different queries share a fingerprint")
	}

	base, _ := fingerprintOptions(&zoekt.SearchOptions{})
	whole, _ := fingerprintOptions(&zoekt.SearchOptions{Whole: true})
	docLimit, _ := fingerprintOptions(&zoekt.SearchOptions{MaxDocDisplayCount: 10})
	sched, _ := fingerprintOptions(&zoekt.SearchOptions{SchedulingClass: zoekt.SchedulingClassBatch})
	for name, got := range map[string][32]byte{"whole": whole, "docLimit": docLimit, "sched": sched} {
		if got == base {
			t.Fatalf("options fingerprint insensitive to %s", name)
		}
	}
}

func TestSnapshotBatchAndSeeding(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	r, stop := newTestRegistry(now)
	defer stop()

	s := newTestSnapshot(t, r, mustParseQuery(t, "foo"), &zoekt.SearchOptions{}, 0)
	t1 := s.recordBatch("k1", 5)
	t2 := s.recordBatch("k2", 7)

	d1, _ := decodeResumeToken(t1, r.creds, now)
	d2, _ := decodeResumeToken(t2, r.creds, now)
	if d1.seq != 1 || d2.seq != 2 {
		t.Fatalf("seq: got %d,%d", d1.seq, d2.seq)
	}

	view, rerr := r.resume(t2, mustParseQuery(t, "foo"), &zoekt.SearchOptions{}, 0)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if len(view.confirmed) != 2 || view.totalMatch != 12 {
		t.Fatalf("confirmed=%v total=%d", view.confirmed, view.totalMatch)
	}
	if view.confirmed[0].key != "k1" || view.confirmed[1].key != "k2" {
		t.Fatalf("confirmed keys: %+v", view.confirmed)
	}
}

func TestSnapshotPinsReleasedOnCompletion(t *testing.T) {
	ss := newShardedSearcher(1)
	defer ss.snapshots.stop()
	defer ss.Close()

	ss.replace(map[string]zoekt.Searcher{"k": newCountingSearcher(t, 1)})
	r1 := ss.getLoaded().shards[0]

	snap, rerr := ss.newSearchSnapshot(context.Background(), mustParseQuery(t, "needle"), &zoekt.SearchOptions{})
	if rerr != nil {
		t.Fatal(rerr)
	}

	// Hot-reload while the snapshot is alive: old generation stays open.
	ss.replace(map[string]zoekt.Searcher{"k": newCountingSearcher(t, 2)})
	if got := closerOf(r1).closes.Load(); got != 0 {
		t.Fatalf("old shard closed while snapshot active: %d", got)
	}

	ss.snapshots.finish(snap)
	if got := closerOf(r1).closes.Load(); got != 1 {
		t.Fatalf("old shard closes on completion: got %d want 1", got)
	}
}

func TestSnapshotExpiryReleasesPinsButSkipsActive(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	ss := newShardedSearcher(1)
	defer ss.Close()
	ss.snapshots.stop() // take manual control; no janitor
	ss.snapshots.now = func() time.Time { return now }
	ss.snapshots.ttl = time.Minute

	ss.replace(map[string]zoekt.Searcher{
		"a": newCountingSearcher(t, 1),
		"b": newCountingSearcher(t, 2),
	})
	old := ss.getLoaded().shards

	idle, rerr := ss.newSearchSnapshot(context.Background(), mustParseQuery(t, "needle"), &zoekt.SearchOptions{})
	if rerr != nil {
		t.Fatal(rerr)
	}
	busy, rerr := ss.newSearchSnapshot(context.Background(), mustParseQuery(t, "needle"), &zoekt.SearchOptions{})
	if rerr != nil {
		t.Fatal(rerr)
	}
	if !busy.beginCall(context.Background()) {
		t.Fatal("beginCall failed")
	}

	// Hot-reload retires the old generation; both snapshots pin it.
	ss.replace(map[string]zoekt.Searcher{
		"a": newCountingSearcher(t, 3),
		"b": newCountingSearcher(t, 4),
	})
	if got := closerOf(old[0]).closes.Load() + closerOf(old[1]).closes.Load(); got != 0 {
		t.Fatalf("old shards closed while snapshots active: %d", got)
	}

	// Advance past TTL: idle snapshot is reaped, the running one is kept.
	ss.snapshots.now = func() time.Time { return now.Add(2 * time.Minute) }
	ss.snapshots.releaseExpired()

	if got := closerOf(old[0]).closes.Load() + closerOf(old[1]).closes.Load(); got != 0 {
		t.Fatalf("old shards closed while one snapshot active: %d", got)
	}
	if _, ok := ss.snapshots.snapshots[busy.id]; !ok {
		t.Fatal("active snapshot expired")
	}
	if _, ok := ss.snapshots.snapshots[idle.id]; ok {
		t.Fatal("idle expired snapshot retained")
	}

	busy.endCall()
	ss.snapshots.releaseExpired()
	if got := closerOf(old[0]).closes.Load() + closerOf(old[1]).closes.Load(); got != 2 {
		t.Fatalf("both generations close after expiry: got %d want 2", got)
	}
}

func mustParseQuery(t *testing.T, s string) query.Q {
	t.Helper()
	q, err := query.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return q
}
