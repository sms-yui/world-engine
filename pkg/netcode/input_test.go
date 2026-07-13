package netcode

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInputQueueValidatesAuthorityWindowAndSequence(t *testing.T) {
	t.Parallel()

	authority := NewAuthorityRegistry()
	authority.Register(1, Authority{State: "server", Input: "player"})
	queue := NewInputQueue(authority, 3, 4)

	assert.ErrorIs(t, queue.Submit(10, Input{Entity: 1, Peer: "other", Tick: 10, Sequence: 1}), ErrNotInputAuthority)
	assert.ErrorIs(t, queue.Submit(10, Input{Entity: 1, Peer: "player", Tick: 9, Sequence: 1}), ErrInputTooOld)
	assert.ErrorIs(t, queue.Submit(10, Input{Entity: 1, Peer: "player", Tick: 14, Sequence: 1}), ErrInputTooFarAhead)
	require.NoError(t, queue.Submit(10, Input{Entity: 1, Peer: "player", Tick: 11, Sequence: 1}))
	assert.ErrorIs(t, queue.Submit(10, Input{Entity: 1, Peer: "player", Tick: 11, Sequence: 1}), ErrInputOutOfOrder)
}

func TestInputQueueDrainsDeterministicallyAndCopiesPayload(t *testing.T) {
	t.Parallel()

	authority := NewAuthorityRegistry()
	authority.Register(1, Authority{Input: "player"})
	authority.Register(2, Authority{Input: "player"})
	queue := NewInputQueue(authority, 2, 10)
	payload := []byte("move")
	require.NoError(t, queue.Submit(10, Input{Entity: 2, Peer: "player", Tick: 11, Sequence: 1, Payload: payload}))
	require.NoError(t, queue.Submit(10, Input{Entity: 1, Peer: "player", Tick: 11, Sequence: 2}))
	require.NoError(t, queue.Submit(10, Input{Entity: 1, Peer: "player", Tick: 11, Sequence: 3}))
	payload[0] = 'x'

	inputs := queue.Drain(11)
	require.Len(t, inputs, 3)
	assert.Equal(t, []EntityID{1, 1, 2}, []EntityID{inputs[0].Entity, inputs[1].Entity, inputs[2].Entity})
	assert.Equal(t, []byte("move"), inputs[2].Payload)
	assert.Empty(t, queue.Drain(11))
}
