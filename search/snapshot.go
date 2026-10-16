package search

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/proto"

	"github.com/sourcegraph/zoekt"
	"github.com/sourcegraph/zoekt/index"
	"github.com/sourcegraph/zoekt/internal/tenant"
	"github.com/sourcegraph/zoekt/query"
)

// This file implements resumable query snapshots for StreamSearch. A snapshot
// fixes, at the time of the first (credentialless) request:
//
//   - the normalized query and the result-affecting SearchOptions,
//   - the tenant, and
//   - the exact set of loaded shards (pinned for the snapshot lifetime).
//
// The server mints an opaque credential after every safely confirmable batch
// (one fully-emitted shard result). Reconnecting with the credential continues
// the stream after the last confirmed batch: already-emitted batches are never
// sent twice, later batches are never skipped, and reads stay confined to the
// pinned shard generation even if the directory watcher replaces shards in the
// meantime.
//
// Credentials are only valid inside the process that minted them: a random
// per-process instance id (plaintext) distinguishes a restarted server while a
// random per-process HMAC key makes forged/corrupt credentials fail.

const (
	resumeTokenVersion byte = 1

	// instanceIDSize and snapshotIDSize are random identifiers.
	instanceIDSize = 8
	snapshotIDSize = 16
	hmacSize       = sha256.Size

	// defaultSnapshotTTL bounds the lifetime (and therefore the old-shard
	// retention) of a snapshot from its creation. It does not slide.
	defaultSnapshotTTL = 15 * time.Minute

	// snapshotRegistryCap bounds retained tombstones and snapshots; active
	// snapshots are never force-evicted, so a fresh stream always succeeds.
	snapshotRegistryCap = 10000
)

var (
	metricResumeSnapshotsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "zoekt_resume_snapshots_active",
		Help: "Number of resumable stream search snapshots currently retained.",
	})
	metricResumeSnapshotShardsPinned = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "zoekt_resume_snapshot_shards_pinned",
		Help: "Number of shards pinned by resumable stream search snapshots.",
	})
	metricResumeRejectedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "zoekt_resume_rejected_total",
		Help: "Total number of rejected stream search resume attempts by reason.",
	}, []string{"reason"})
)

func snapshotTTLFromEnv() time.Duration {
	v := os.Getenv("ZOEKT_RESUME_TTL")
	if v == "" {
		return defaultSnapshotTTL
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return defaultSnapshotTTL
	}
	return d
}

// resumeCredentials holds the per-process secret material of a snapshot
// registry.
type resumeCredentials struct {
	instanceID []byte
	hmacKey    []byte
}

func newResumeCredentials() resumeCredentials {
	c := resumeCredentials{
		instanceID: make([]byte, instanceIDSize),
		hmacKey:    make([]byte, 32),
	}
	if _, err := rand.Read(c.instanceID); err != nil {
		panic(err)
	}
	if _, err := rand.Read(c.hmacKey); err != nil {
		panic(err)
	}
	return c
}

// encodeResumeToken builds the opaque credential for snapshot id at the
// confirmed batch position seq.
//
// Layout (before base64url encoding):
//
//	version(1) | instanceID(8) | snapshotID(16) | seq(uvarint) |
//	expiresAtUnix(uvarint) | HMAC-SHA256(32)
func encodeResumeToken(c resumeCredentials, snapshotID []byte, seq uint64, expiresAt time.Time) []byte {
	payload := make([]byte, 0, 1+instanceIDSize+snapshotIDSize+binary.MaxVarintLen64*2)
	payload = append(payload, resumeTokenVersion)
	payload = append(payload, c.instanceID...)
	payload = append(payload, snapshotID...)
	payload = binary.AppendUvarint(payload, seq)
	payload = binary.AppendUvarint(payload, uint64(expiresAt.Unix()))

	mac := hmac.New(sha256.New, c.hmacKey)
	mac.Write(payload)
	token := append(payload, mac.Sum(nil)...)

	out := make([]byte, base64.RawURLEncoding.EncodedLen(len(token)))
	base64.RawURLEncoding.Encode(out, token)
	return out
}

type decodedResumeToken struct {
	snapshotID string
	seq        uint64
	expiresAt  time.Time
}

// decodeResumeToken validates shape, server instance, HMAC and expiry. The
// returned error classifies the failure. An expired token is returned together
// with an EXPIRED error so callers can identify its snapshot.
func decodeResumeToken(raw []byte, c resumeCredentials, now time.Time) (decodedResumeToken, *zoekt.StreamResumeError) {
	token := make([]byte, base64.RawURLEncoding.DecodedLen(len(raw)))
	n, err := base64.RawURLEncoding.Decode(token, raw)
	if err != nil {
		return decodedResumeToken{}, resumeError(zoekt.StreamResumeInvalidToken, "corrupt credential encoding")
	}
	token = token[:n]

	minLen := 1 + instanceIDSize + snapshotIDSize + 1 + 1 + hmacSize
	if len(token) < minLen || token[0] != resumeTokenVersion {
		return decodedResumeToken{}, resumeError(zoekt.StreamResumeInvalidToken, "unsupported credential version")
	}

	p := 1
	inst := token[p : p+instanceIDSize]
	p += instanceIDSize
	if !hmac.Equal(inst, c.instanceID) {
		// The credential was minted by another server process (e.g. after a
		// restart) or replayed against a different instance.
		return decodedResumeToken{}, resumeError(zoekt.StreamResumeRestarted, "credential from another server process")
	}

	id := token[p : p+snapshotIDSize]
	p += snapshotIDSize
	seq, m := binary.Uvarint(token[p:])
	if m <= 0 {
		return decodedResumeToken{}, resumeError(zoekt.StreamResumeInvalidToken, "corrupt batch position")
	}
	p += m
	expiryUnix, m := binary.Uvarint(token[p:])
	if m <= 0 {
		return decodedResumeToken{}, resumeError(zoekt.StreamResumeInvalidToken, "corrupt expiry")
	}
	p += m

	if len(token[p:]) != hmacSize {
		return decodedResumeToken{}, resumeError(zoekt.StreamResumeInvalidToken, "corrupt authentication tag")
	}
	mac := hmac.New(sha256.New, c.hmacKey)
	mac.Write(token[:p])
	if !hmac.Equal(mac.Sum(nil), token[p:]) {
		return decodedResumeToken{}, resumeError(zoekt.StreamResumeInvalidToken, "credential authentication failed")
	}

	dec := decodedResumeToken{
		snapshotID: hex.EncodeToString(id),
		seq:        seq,
		expiresAt:  time.Unix(int64(expiryUnix), 0),
	}
	if !now.Before(dec.expiresAt) {
		return dec, resumeError(zoekt.StreamResumeExpired, "snapshot credential expired")
	}
	return dec, nil
}

func resumeError(code zoekt.StreamResumeErrorCode, msg string) *zoekt.StreamResumeError {
	return zoekt.NewStreamResumeError(code, msg)
}

// fingerprintQuery deterministically identifies the normalized query:
// semantically identical queries that simplify to the same proto form share a
// snapshot.
func fingerprintQuery(q query.Q) ([32]byte, error) {
	b, err := proto.Marshal(query.QToProto(query.Simplify(q)))
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(b), nil
}

// fingerprintOptions deterministically identifies the SearchOptions.
func fingerprintOptions(opts *zoekt.SearchOptions) ([32]byte, error) {
	if opts == nil {
		opts = &zoekt.SearchOptions{}
	}
	b, err := proto.Marshal(opts.ToProto())
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(b), nil
}

// tenantIDFromContext returns the request tenant id; 0 means "no tenant".
func tenantIDFromContext(ctx context.Context) int {
	tnt, err := tenant.FromContext(ctx)
	if err != nil {
		return 0
	}
	return tnt.ID()
}

type snapshotStatus uint8

const (
	snapshotActive snapshotStatus = iota
	snapshotCompleted
	snapshotAbandoned
	snapshotExpired
)

// emittedBatch records one confirmed batch: the shard key and the match count
// its search produced (needed to seed TotalMaxMatchCount on resume).
type emittedBatch struct {
	key        string
	matchCount int
}

// searchSnapshot is the server-side state of a resumable StreamSearch.
type searchSnapshot struct {
	id        string
	idRaw     []byte
	expiresAt time.Time
	createdAt time.Time

	queryFP  [32]byte
	optsFP   [32]byte
	tenantID int

	// keys and shards are parallel slices in the canonical ranked order that
	// was loaded when the snapshot started. Every shard is pinned.
	keys   []string
	shards []*rankedShard
	ready  bool

	registry *snapshotRegistry
	creds    resumeCredentials

	mu          sync.Mutex
	cond        *sync.Cond
	status      snapshotStatus
	emitted     []emittedBatch
	activeCalls int

	// Shared stateful display truncator; created lazily for the first run and
	// reused on resume so MaxDocDisplayCount/MaxMatchDisplayCount behave as in
	// an uninterrupted stream.
	truncator        index.DisplayTruncator
	hasDisplayLimits bool

	// totalMatchLimitHit is recorded when TotalMaxMatchCount stopped new
	// shard dispatches; resume then skips all remaining shards.
	totalMatchLimitHit bool
}

// recordBatch appends a confirmed batch and returns the credential that
// confirms it (seq = number of confirmed batches).
func (s *searchSnapshot) recordBatch(key string, matchCount int) []byte {
	s.mu.Lock()
	s.emitted = append(s.emitted, emittedBatch{key: key, matchCount: matchCount})
	seq := uint64(len(s.emitted))
	s.mu.Unlock()
	return encodeResumeToken(s.creds, s.idRaw, seq, s.expiresAt)
}

// confirmedBatches returns the batches confirmed up to and including seq.
func (s *searchSnapshot) confirmedBatches(seq uint64) []emittedBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	if int(seq) > len(s.emitted) {
		seq = uint64(len(s.emitted))
	}
	out := make([]emittedBatch, seq)
	copy(out, s.emitted[:seq])
	return out
}

// beginCall reserves the snapshot for one run. If another run is finishing it
// waits (a reconnect can arrive while the previous handler is unwinding).
func (s *searchSnapshot) beginCall(ctx context.Context) bool {
	wait := time.NewTimer(30 * time.Second)
	defer wait.Stop()

	woken := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.condMu().Broadcast()
		case <-wait.C:
			s.condMu().Broadcast()
		case <-woken:
		}
	}()

	s.mu.Lock()
	if s.cond == nil {
		s.cond = sync.NewCond(&s.mu)
	}
	for s.status == snapshotActive && s.activeCalls > 0 {
		s.cond.Wait()
	}
	ok := s.status == snapshotActive
	if ok {
		s.activeCalls++
	}
	s.mu.Unlock()
	close(woken)
	return ok
}

// condMu lazily creates the broadcast condition variable.
func (s *searchSnapshot) condMu() *sync.Cond {
	if s.cond == nil {
		s.cond = sync.NewCond(&s.mu)
	}
	return s.cond
}

func (s *searchSnapshot) endCall() {
	s.mu.Lock()
	s.activeCalls--
	idle := s.activeCalls == 0
	s.mu.Unlock()
	if idle {
		s.condMu().Broadcast()
	}
}

func (s *searchSnapshot) setTotalMatchLimitHit() {
	s.mu.Lock()
	s.totalMatchLimitHit = true
	s.mu.Unlock()
}

func (s *searchSnapshot) snapshotState() (status snapshotStatus, limitHit bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status, s.totalMatchLimitHit
}

// displayTruncator returns the snapshot-shared display truncator, creating it
// on first use.
func (s *searchSnapshot) displayTruncator(opts *zoekt.SearchOptions) index.DisplayTruncator {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.truncator == nil {
		s.truncator, s.hasDisplayLimits = index.NewDisplayTruncator(opts)
	}
	return s.truncator
}

func (s *searchSnapshot) displayLimits() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasDisplayLimits
}

// releasePins unpins all snapshot shards exactly once.
func (s *searchSnapshot) releasePins() {
	s.mu.Lock()
	shards := s.shards
	s.shards = nil
	s.mu.Unlock()

	for _, sh := range shards {
		sh.unpin()
	}
}

// snapshotRegistry owns all snapshots of a shardedSearcher.
type snapshotRegistry struct {
	creds resumeCredentials
	ttl   time.Duration
	now   func() time.Time

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup

	mu        sync.Mutex
	snapshots map[string]*searchSnapshot
}

func newSnapshotRegistry() *snapshotRegistry {
	return newSnapshotRegistryWithClock(time.Now, snapshotTTLFromEnv())
}

func newSnapshotRegistryWithClock(now func() time.Time, ttl time.Duration) *snapshotRegistry {
	r := &snapshotRegistry{
		creds:     newResumeCredentials(),
		ttl:       ttl,
		now:       now,
		stopCh:    make(chan struct{}),
		snapshots: map[string]*searchSnapshot{},
	}
	r.wg.Add(1)
	go r.janitor()
	return r
}

func (r *snapshotRegistry) stop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
	r.wg.Wait()
}

// closeAll releases every snapshot's pinned shards. It is used when the
// sharded searcher closes: any retired shard generation that was kept alive by
// a snapshot can then be closed immediately.
func (r *snapshotRegistry) closeAll() {
	r.stop()

	r.mu.Lock()
	snaps := make([]*searchSnapshot, 0, len(r.snapshots))
	for _, s := range r.snapshots {
		snaps = append(snaps, s)
	}
	r.mu.Unlock()

	for _, s := range snaps {
		s.mu.Lock()
		s.status = snapshotAbandoned
		s.mu.Unlock()
		s.releasePins()
	}
}

func (r *snapshotRegistry) janitor() {
	defer r.wg.Done()
	interval := r.ttl / 10
	if interval > 30*time.Second || interval == 0 {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-t.C:
			r.releaseExpired()
		}
	}
}

func (r *snapshotRegistry) releaseExpired() {
	var toRelease []*searchSnapshot
	r.mu.Lock()
	now := r.now()
	for id, s := range r.snapshots {
		s.mu.Lock()
		expired := !now.Before(s.expiresAt) && s.activeCalls == 0
		if expired && s.status == snapshotActive {
			s.status = snapshotExpired
		}
		terminal := s.status != snapshotActive
		hasPins := s.shards != nil
		s.mu.Unlock()

		if expired && terminal && hasPins {
			toRelease = append(toRelease, s)
			delete(r.snapshots, id)
		}
	}
	r.setGaugesLocked()
	r.mu.Unlock()

	for _, s := range toRelease {
		s.releasePins()
	}
}

// start registers a new snapshot whose shards are already pinned.
func (r *snapshotRegistry) start(s *searchSnapshot) {
	r.mu.Lock()
	r.snapshots[s.id] = s
	r.evictTombstonesLocked()
	r.setGaugesLocked()
	r.mu.Unlock()
}

// evictTombstonesLocked keeps the registry bounded by dropping finished
// tombstones (which carry no pins). Active snapshots are never evicted.
func (r *snapshotRegistry) evictTombstonesLocked() {
	for len(r.snapshots) > snapshotRegistryCap {
		var oldestID string
		var oldestTime time.Time
		for id, s := range r.snapshots {
			s.mu.Lock()
			tombstone := s.status != snapshotActive && s.shards == nil
			created := s.createdAt
			s.mu.Unlock()
			if !tombstone {
				continue
			}
			if oldestID == "" || created.Before(oldestTime) {
				oldestID, oldestTime = id, created
			}
		}
		if oldestID == "" {
			// Only active snapshots remain; never force-evict them.
			return
		}
		delete(r.snapshots, oldestID)
	}
}

func (r *snapshotRegistry) setGaugesLocked() {
	active, pinned := 0, 0
	for _, s := range r.snapshots {
		s.mu.Lock()
		if s.status == snapshotActive {
			active++
		}
		pinned += len(s.shards)
		s.mu.Unlock()
	}
	metricResumeSnapshotsActive.Set(float64(active))
	metricResumeSnapshotShardsPinned.Set(float64(pinned))
}

// resumeView is what a validated resume request needs to run.
type resumeView struct {
	snapshot   *searchSnapshot
	seq        uint64
	confirmed  []emittedBatch
	totalMatch int
	limitHit   bool
}

// resume validates raw against the echoed query/options/tenant and returns the
// snapshot continuation state. It performs no search.
func (r *snapshotRegistry) resume(
	raw []byte,
	q query.Q,
	opts *zoekt.SearchOptions,
	tenantID int,
) (*resumeView, *zoekt.StreamResumeError) {
	dec, rerr := decodeResumeToken(raw, r.creds, r.now())

	r.mu.Lock()
	s, ok := r.snapshots[dec.snapshotID]
	r.mu.Unlock()
	if !ok {
		if rerr != nil && rerr.Code == zoekt.StreamResumeExpired {
			// The janitor already removed the expired snapshot but the
			// credential itself proves it was once valid and is expired.
			metricResumeRejectedTotal.WithLabelValues(rerr.Code.String()).Inc()
			return nil, rerr
		}
		// Unknown snapshot inside this process means it was tombstone-evicted;
		// a foreign instance is reported directly by decode. Both are
		// operationally "server restarted / state gone".
		metricResumeRejectedTotal.WithLabelValues(zoekt.StreamResumeRestarted.String()).Inc()
		return nil, resumeError(zoekt.StreamResumeRestarted, "unknown snapshot")
	}

	status, _ := s.snapshotState()
	if rerr != nil {
		metricResumeRejectedTotal.WithLabelValues(rerr.Code.String()).Inc()
		return nil, rerr
	}

	switch status {
	case snapshotCompleted:
		metricResumeRejectedTotal.WithLabelValues(zoekt.StreamResumeCompleted.String()).Inc()
		return nil, resumeError(zoekt.StreamResumeCompleted, "snapshot completed")
	case snapshotAbandoned:
		metricResumeRejectedTotal.WithLabelValues(zoekt.StreamResumeAbandoned.String()).Inc()
		return nil, resumeError(zoekt.StreamResumeAbandoned, "snapshot abandoned")
	case snapshotExpired:
		metricResumeRejectedTotal.WithLabelValues(zoekt.StreamResumeExpired.String()).Inc()
		return nil, resumeError(zoekt.StreamResumeExpired, "snapshot expired")
	}

	qfp, err := fingerprintQuery(q)
	if err != nil {
		metricResumeRejectedTotal.WithLabelValues(zoekt.StreamResumeMismatch.String()).Inc()
		return nil, resumeError(zoekt.StreamResumeMismatch, "cannot fingerprint query")
	}
	ofp, err := fingerprintOptions(opts)
	if err != nil {
		metricResumeRejectedTotal.WithLabelValues(zoekt.StreamResumeMismatch.String()).Inc()
		return nil, resumeError(zoekt.StreamResumeMismatch, "cannot fingerprint options")
	}
	if qfp != s.queryFP || ofp != s.optsFP || tenantID != s.tenantID {
		metricResumeRejectedTotal.WithLabelValues(zoekt.StreamResumeMismatch.String()).Inc()
		return nil, resumeError(zoekt.StreamResumeMismatch, "query, options or tenant do not match snapshot")
	}

	s.mu.Lock()
	maxSeq := uint64(len(s.emitted))
	s.mu.Unlock()
	if dec.seq > maxSeq {
		metricResumeRejectedTotal.WithLabelValues(zoekt.StreamResumeInvalidToken.String()).Inc()
		return nil, resumeError(zoekt.StreamResumeInvalidToken, "credential position is ahead of the snapshot")
	}

	confirmed := s.confirmedBatches(dec.seq)
	total := 0
	for _, b := range confirmed {
		total += b.matchCount
	}

	_, limitHit := s.snapshotState()

	return &resumeView{
		snapshot:   s,
		seq:        dec.seq,
		confirmed:  confirmed,
		totalMatch: total,
		limitHit:   limitHit,
	}, nil
}

// abandon validates raw like resume but releases the snapshot instead.
func (r *snapshotRegistry) abandon(raw []byte, q query.Q, opts *zoekt.SearchOptions, tenantID int) *zoekt.StreamResumeError {
	view, rerr := r.resume(raw, q, opts, tenantID)
	if rerr != nil {
		return rerr
	}
	s := view.snapshot

	s.mu.Lock()
	if s.status != snapshotActive || s.activeCalls > 0 {
		s.mu.Unlock()
		return resumeError(zoekt.StreamResumeInvalidToken, "snapshot cannot be abandoned in its current state")
	}
	s.status = snapshotAbandoned
	s.mu.Unlock()

	// Retain a zero-pin tombstone so the same credential later resolves to
	// ABANDONED rather than RESTARTED.
	s.releasePins()
	r.mu.Lock()
	r.setGaugesLocked()
	r.mu.Unlock()
	return nil
}

// finish marks a successfully completed run and releases the pins while
// retaining a zero-pin tombstone so later credentials get COMPLETED.
func (r *snapshotRegistry) finish(s *searchSnapshot) {
	s.mu.Lock()
	if s.status != snapshotActive {
		s.mu.Unlock()
		return
	}
	s.status = snapshotCompleted
	s.mu.Unlock()

	s.releasePins()
	r.mu.Lock()
	r.evictTombstonesLocked()
	r.setGaugesLocked()
	r.mu.Unlock()
}

// newSearchSnapshot builds and registers a snapshot of the current loaded set.
// The snapshot owns pins to all shards; they are released on completion,
// abandonment or expiry.
func (ss *shardedSearcher) newSearchSnapshot(ctx context.Context, q query.Q, opts *zoekt.SearchOptions) (*searchSnapshot, *zoekt.StreamResumeError) {
	qfp, err := fingerprintQuery(q)
	if err != nil {
		return nil, resumeError(zoekt.StreamResumeInvalidToken, "cannot fingerprint query: "+err.Error())
	}
	ofp, err := fingerprintOptions(opts)
	if err != nil {
		return nil, resumeError(zoekt.StreamResumeInvalidToken, "cannot fingerprint options: "+err.Error())
	}

	keys, shards, ok := ss.pinSnapshotSet()
	if !ok {
		return nil, resumeError(zoekt.StreamResumeRestarted, "shards changed while creating snapshot")
	}

	idRaw := make([]byte, snapshotIDSize)
	if _, err := rand.Read(idRaw); err != nil {
		for _, s := range shards {
			s.unpin()
		}
		return nil, resumeError(zoekt.StreamResumeInvalidToken, "cannot allocate snapshot id")
	}
	now := ss.snapshots.now

	s := &searchSnapshot{
		id:        hex.EncodeToString(idRaw),
		idRaw:     idRaw,
		createdAt: now(),
		expiresAt: now().Add(ss.snapshots.ttl),
		queryFP:   qfp,
		optsFP:    ofp,
		tenantID:  tenantIDFromContext(ctx),
		keys:      keys,
		shards:    shards,
		ready:     ss.getLoaded().ready,
		registry:  ss.snapshots,
		creds:     ss.snapshots.creds,
	}
	ss.snapshots.start(s)
	return s, nil
}

// pinSnapshotSet returns the ranked shards with their parallel keys and pins
// every shard. The pins are owned by the caller (the snapshot). A concurrent
// replace that retires a shard during pinning triggers one retry against the
// newest loaded set; ok is false only if that retry also fails.
func (ss *shardedSearcher) pinSnapshotSet() (keys []string, shards []*rankedShard, ok bool) {
	try := func() ([]string, []*rankedShard, bool) {
		ranked := ss.getLoaded().shards

		ss.mu.Lock()
		keyOf := make(map[*rankedShard]string, len(ss.shards))
		for k, v := range ss.shards {
			keyOf[v] = k
		}
		ss.mu.Unlock()

		ks := make([]string, 0, len(ranked))
		pinned := make([]*rankedShard, 0, len(ranked))
		for _, r := range ranked {
			if !r.pin() {
				for _, p := range pinned {
					p.unpin()
				}
				return nil, nil, false
			}
			pinned = append(pinned, r)
			ks = append(ks, keyOf[r])
		}
		return ks, pinned, true
	}

	keys, shards, ok = try()
	if !ok {
		keys, shards, ok = try()
	}
	return keys, shards, ok
}
