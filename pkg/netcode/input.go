package netcode

import (
	"errors"
	"sort"
	"sync"
)

var (
	// ErrInputTooOld is returned for input targeting an already expired tick.
	ErrInputTooOld = errors.New("input tick is too old")
	// ErrInputTooFarAhead is returned when input exceeds the configured future window.
	ErrInputTooFarAhead = errors.New("input tick is too far ahead")
	// ErrInputOutOfOrder is returned for duplicate or decreasing input sequences.
	ErrInputOutOfOrder = errors.New("input sequence is out of order")
	// ErrInputQueueFull is returned when buffered input reaches its configured bound.
	ErrInputQueueFull = errors.New("input queue is full")
)

// Input is an opaque, game-defined command scheduled for a simulation tick.
type Input struct {
	Entity   EntityID
	Peer     PeerID
	Tick     uint64
	Sequence uint64
	Payload  []byte
}

type inputStream struct {
	entity EntityID
	peer   PeerID
}

// InputQueue validates input authority and buffers commands for deterministic
// tick consumption. It supports centralized simulation and client prediction.
type InputQueue struct {
	mu             sync.Mutex
	authority      *AuthorityRegistry
	maxFutureTicks uint64
	maxInputs      int
	count          int
	lastSequence   map[inputStream]uint64
	byTick         map[uint64][]Input
}

// NewInputQueue creates an authority-checked rolling input buffer.
func NewInputQueue(authority *AuthorityRegistry, maxFutureTicks uint64, maxInputs int) *InputQueue {
	if authority == nil {
		authority = NewAuthorityRegistry()
	}
	if maxInputs < 1 {
		maxInputs = 1024
	}
	return &InputQueue{
		authority:      authority,
		maxFutureTicks: maxFutureTicks,
		maxInputs:      maxInputs,
		lastSequence:   make(map[inputStream]uint64),
		byTick:         make(map[uint64][]Input),
	}
}

// Submit validates and queues an input relative to the current server tick.
func (q *InputQueue) Submit(currentTick uint64, input Input) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	authority, ok := q.authority.Get(input.Entity)
	if !ok {
		return ErrEntityNotRegistered
	}
	if authority.Input != input.Peer {
		return ErrNotInputAuthority
	}
	if input.Tick < currentTick {
		return ErrInputTooOld
	}
	if input.Tick-currentTick > q.maxFutureTicks {
		return ErrInputTooFarAhead
	}
	stream := inputStream{entity: input.Entity, peer: input.Peer}
	if input.Sequence <= q.lastSequence[stream] {
		return ErrInputOutOfOrder
	}
	if q.count >= q.maxInputs {
		return ErrInputQueueFull
	}
	input.Payload = bytesClone(input.Payload)
	q.byTick[input.Tick] = append(q.byTick[input.Tick], input)
	q.lastSequence[stream] = input.Sequence
	q.count++
	return nil
}

// Drain returns a stable entity/peer/sequence-ordered batch for one tick.
func (q *InputQueue) Drain(tick uint64) []Input {
	q.mu.Lock()
	defer q.mu.Unlock()
	inputs := q.byTick[tick]
	delete(q.byTick, tick)
	q.count -= len(inputs)
	sort.SliceStable(inputs, func(i, j int) bool {
		if inputs[i].Entity != inputs[j].Entity {
			return inputs[i].Entity < inputs[j].Entity
		}
		if inputs[i].Peer != inputs[j].Peer {
			return inputs[i].Peer < inputs[j].Peer
		}
		return inputs[i].Sequence < inputs[j].Sequence
	})
	return inputs
}

// DiscardBefore removes inputs that can no longer be simulated.
func (q *InputQueue) DiscardBefore(tick uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for inputTick, inputs := range q.byTick {
		if inputTick < tick {
			q.count -= len(inputs)
			delete(q.byTick, inputTick)
		}
	}
}
