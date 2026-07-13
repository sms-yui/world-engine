package netcode

import (
	"bytes"
	"errors"
	"sort"
	"sync"
)

var (
	// ErrPeerNotRegistered is returned for an unknown replication peer.
	ErrPeerNotRegistered = errors.New("peer is not registered")
	// ErrUnknownFrame is returned when acknowledging a frame that is no longer pending.
	ErrUnknownFrame = errors.New("frame is not pending")
)

// EntityDelta contains only changed and removed components for an existing entity.
type EntityDelta struct {
	ID                EntityID
	Components        map[ComponentID][]byte
	RemovedComponents []ComponentID
}

// Frame is a reliable, transport-independent replication unit. Every frame is
// computed against BaseSequence, the most recently acknowledged baseline.
type Frame struct {
	Sequence     uint64
	BaseSequence uint64
	Tick         uint64
	Creates      []Entity
	Updates      []EntityDelta
	Deletes      []EntityID
}

type pendingFrame struct {
	state map[EntityID]Entity
}

type session struct {
	query        Query
	nextSequence uint64
	baseSequence uint64
	baseline     map[EntityID]Entity
	pending      map[uint64]pendingFrame
}

// Replicator maintains per-peer interest and acknowledged delta baselines.
// It intentionally does not prescribe a transport or component codec.
type Replicator struct {
	mu         sync.Mutex
	filter     InterestFilter
	projector  Projector
	maxPending int
	sessions   map[PeerID]*session
}

// Option configures a Replicator.
type Option func(*Replicator)

// WithInterestFilter replaces the default spherical interest filter.
func WithInterestFilter(filter InterestFilter) Option {
	return func(r *Replicator) {
		if filter != nil {
			r.filter = filter
		}
	}
}

// WithProjector configures game-specific network LOD projection.
func WithProjector(projector Projector) Option {
	return func(r *Replicator) {
		if projector != nil {
			r.projector = projector
		}
	}
}

// WithMaxPendingFrames bounds retained baselines per peer. Values below one
// retain the default of 64.
func WithMaxPendingFrames(maxPending int) Option {
	return func(r *Replicator) {
		if maxPending > 0 {
			r.maxPending = maxPending
		}
	}
}

// NewReplicator creates a transport-independent replication server.
func NewReplicator(options ...Option) *Replicator {
	replicator := &Replicator{
		filter:     SphereInterest{},
		projector:  identityProjector{},
		maxPending: 64,
		sessions:   make(map[PeerID]*session),
	}
	for _, option := range options {
		option(replicator)
	}
	return replicator
}

// AddPeer starts replication for a peer. Query updates are server-side so a
// client cannot expand visibility by forging its own area of interest.
func (r *Replicator) AddPeer(peer PeerID, query Query) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[peer] = &session{
		query:        query,
		nextSequence: 1,
		baseline:     make(map[EntityID]Entity),
		pending:      make(map[uint64]pendingFrame),
	}
}

// RemovePeer drops all replication state for a peer.
func (r *Replicator) RemovePeer(peer PeerID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, peer)
}

// SetQuery updates a peer's authoritative area of interest.
func (r *Replicator) SetQuery(peer PeerID, query Query) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[peer]
	if !ok {
		return ErrPeerNotRegistered
	}
	s.query = query
	return nil
}

// BuildFrame filters and projects a world snapshot, then creates a delta from
// the peer's last acknowledged frame. The caller may safely reuse its entities.
func (r *Replicator) BuildFrame(peer PeerID, tick uint64, entities []Entity) (Frame, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[peer]
	if !ok {
		return Frame{}, ErrPeerNotRegistered
	}

	visible := make(map[EntityID]Entity)
	for _, entity := range entities {
		if !r.filter.Interested(peer, s.query, entity) {
			continue
		}
		projected := r.projector.Project(peer, cloneEntity(entity), distance(s.query.Center, entity.Position))
		visible[entity.ID] = cloneEntity(projected)
	}

	frame := Frame{Sequence: s.nextSequence, BaseSequence: s.baseSequence, Tick: tick}
	s.nextSequence++
	ids := sortedEntityIDs(visible)
	for _, id := range ids {
		entity := visible[id]
		previous, exists := s.baseline[id]
		if !exists {
			frame.Creates = append(frame.Creates, cloneEntity(entity))
			continue
		}
		if delta, changed := diffEntity(previous, entity); changed {
			frame.Updates = append(frame.Updates, delta)
		}
	}
	for _, id := range sortedEntityIDs(s.baseline) {
		if _, exists := visible[id]; !exists {
			frame.Deletes = append(frame.Deletes, id)
		}
	}

	s.pending[frame.Sequence] = pendingFrame{state: cloneState(visible)}
	for len(s.pending) > r.maxPending {
		delete(s.pending, smallestSequence(s.pending))
	}
	return frame, nil
}

// Ack advances a peer's baseline to a delivered frame. Older or duplicate
// acknowledgements are harmless; unknown future/evicted frames are rejected.
func (r *Replicator) Ack(peer PeerID, sequence uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[peer]
	if !ok {
		return ErrPeerNotRegistered
	}
	if sequence <= s.baseSequence {
		return nil
	}
	pending, ok := s.pending[sequence]
	if !ok {
		return ErrUnknownFrame
	}
	s.baseline = cloneState(pending.state)
	s.baseSequence = sequence
	for pendingSequence := range s.pending {
		if pendingSequence <= sequence {
			delete(s.pending, pendingSequence)
		}
	}
	return nil
}

func diffEntity(previous, current Entity) (EntityDelta, bool) {
	delta := EntityDelta{ID: current.ID, Components: make(map[ComponentID][]byte)}
	for id, value := range current.Components {
		if old, exists := previous.Components[id]; !exists || !bytes.Equal(old, value) {
			delta.Components[id] = bytesClone(value)
		}
	}
	for id := range previous.Components {
		if _, exists := current.Components[id]; !exists {
			delta.RemovedComponents = append(delta.RemovedComponents, id)
		}
	}
	sort.Slice(delta.RemovedComponents, func(i, j int) bool {
		return delta.RemovedComponents[i] < delta.RemovedComponents[j]
	})
	return delta, len(delta.Components) > 0 || len(delta.RemovedComponents) > 0
}

func sortedEntityIDs[T any](entities map[EntityID]T) []EntityID {
	ids := make([]EntityID, 0, len(entities))
	for id := range entities {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func smallestSequence(pending map[uint64]pendingFrame) uint64 {
	var smallest uint64
	for sequence := range pending {
		if smallest == 0 || sequence < smallest {
			smallest = sequence
		}
	}
	return smallest
}
