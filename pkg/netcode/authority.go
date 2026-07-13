package netcode

import (
	"errors"
	"sync"
)

var (
	// ErrEntityNotRegistered is returned when an authority operation targets an unknown entity.
	ErrEntityNotRegistered = errors.New("entity is not registered")
	// ErrNotStateAuthority is returned when a peer tries to mutate authority it does not own.
	ErrNotStateAuthority = errors.New("peer does not have state authority")
	// ErrNotInputAuthority is returned when a peer submits input for an entity it does not own.
	ErrNotInputAuthority = errors.New("peer does not have input authority")
)

// Authority separates the peer that writes state from the peer that supplies
// input. This supports server simulation while a client retains input control.
type Authority struct {
	State PeerID
	Input PeerID
}

// AuthorityRegistry tracks entity ownership independently from game state.
type AuthorityRegistry struct {
	mu      sync.RWMutex
	entries map[EntityID]Authority
}

// NewAuthorityRegistry creates an empty authority registry.
func NewAuthorityRegistry() *AuthorityRegistry {
	return &AuthorityRegistry{entries: make(map[EntityID]Authority)}
}

// Register creates or replaces an entity's authority record.
func (r *AuthorityRegistry) Register(entity EntityID, authority Authority) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[entity] = authority
}

// Remove deletes an entity's authority record.
func (r *AuthorityRegistry) Remove(entity EntityID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, entity)
}

// Get returns the authority record for an entity.
func (r *AuthorityRegistry) Get(entity EntityID) (Authority, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	authority, ok := r.entries[entity]
	return authority, ok
}

// TransferState moves state authority when requested by its current owner.
func (r *AuthorityRegistry) TransferState(entity EntityID, from, to PeerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	authority, ok := r.entries[entity]
	if !ok {
		return ErrEntityNotRegistered
	}
	if authority.State != from {
		return ErrNotStateAuthority
	}
	authority.State = to
	r.entries[entity] = authority
	return nil
}

// TransferInput moves input authority when requested by the state authority.
func (r *AuthorityRegistry) TransferInput(entity EntityID, stateAuthority, to PeerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	authority, ok := r.entries[entity]
	if !ok {
		return ErrEntityNotRegistered
	}
	if authority.State != stateAuthority {
		return ErrNotStateAuthority
	}
	authority.Input = to
	r.entries[entity] = authority
	return nil
}
