package netcode

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	transform ComponentID = 1
	health    ComponentID = 2
)

func TestReplicatorBuildsDeltasFromAcknowledgedBaseline(t *testing.T) {
	t.Parallel()

	replicator := NewReplicator()
	replicator.AddPeer("player", Query{Radius: 10})
	entities := []Entity{
		{ID: 2, Position: Vec3{X: 4}, Components: map[ComponentID][]byte{transform: []byte("x=4")}},
		{ID: 1, Position: Vec3{X: 2}, Components: map[ComponentID][]byte{
			transform: []byte("x=2"),
			health:    []byte("100"),
		}},
		{ID: 3, Position: Vec3{X: 20}, Components: map[ComponentID][]byte{transform: []byte("x=20")}},
	}

	initial, err := replicator.BuildFrame("player", 7, entities)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), initial.Sequence)
	assert.Zero(t, initial.BaseSequence)
	require.Len(t, initial.Creates, 2)
	assert.Equal(t, EntityID(1), initial.Creates[0].ID, "frames must be deterministic")
	assert.Equal(t, EntityID(2), initial.Creates[1].ID)
	require.NoError(t, replicator.Ack("player", initial.Sequence))

	entities[0].Position.X = 20
	entities[1].Components[health] = []byte("90")
	delete(entities[1].Components, transform)
	entities[2].Position.X = 5
	entities[2].Components[transform] = []byte("x=5")

	delta, err := replicator.BuildFrame("player", 8, entities)
	require.NoError(t, err)
	assert.Equal(t, initial.Sequence, delta.BaseSequence)
	require.Len(t, delta.Creates, 1)
	assert.Equal(t, EntityID(3), delta.Creates[0].ID)
	require.Len(t, delta.Updates, 1)
	assert.Equal(t, EntityID(1), delta.Updates[0].ID)
	assert.Equal(t, []byte("90"), delta.Updates[0].Components[health])
	assert.Equal(t, []ComponentID{transform}, delta.Updates[0].RemovedComponents)
	assert.Equal(t, []EntityID{2}, delta.Deletes)
}

func TestReplicatorResendsAgainstLastAckAfterLoss(t *testing.T) {
	t.Parallel()

	replicator := NewReplicator()
	replicator.AddPeer("player", Query{Radius: 10})
	entity := Entity{ID: 1, Components: map[ComponentID][]byte{health: []byte("100")}}

	first, err := replicator.BuildFrame("player", 1, []Entity{entity})
	require.NoError(t, err)
	second, err := replicator.BuildFrame("player", 2, []Entity{entity})
	require.NoError(t, err)

	assert.Zero(t, second.BaseSequence)
	assert.Len(t, second.Creates, 1, "unacknowledged creates must be self-healing")
	require.NoError(t, replicator.Ack("player", second.Sequence))
	require.NoError(t, replicator.Ack("player", first.Sequence), "late acknowledgements are harmless")

	third, err := replicator.BuildFrame("player", 3, []Entity{entity})
	require.NoError(t, err)
	assert.Equal(t, second.Sequence, third.BaseSequence)
	assert.Empty(t, third.Creates)
	assert.Empty(t, third.Updates)
}

type distanceProjector struct{}

func (distanceProjector) Project(_ PeerID, entity Entity, distance float64) Entity {
	if distance >= 5 {
		delete(entity.Components, health)
	}
	return entity
}

func TestReplicatorAppliesNetworkLODWithoutMutatingSource(t *testing.T) {
	t.Parallel()

	replicator := NewReplicator(WithProjector(distanceProjector{}))
	replicator.AddPeer("player", Query{Radius: 10})
	entity := Entity{ID: 1, Position: Vec3{X: 8}, Components: map[ComponentID][]byte{
		transform: []byte("x=8"),
		health:    []byte("100"),
	}}

	frame, err := replicator.BuildFrame("player", 1, []Entity{entity})
	require.NoError(t, err)
	require.Len(t, frame.Creates, 1)
	assert.NotContains(t, frame.Creates[0].Components, health)
	assert.Contains(t, entity.Components, health, "projection must not mutate authoritative state")
}

func TestReplicatorCopiesCallerBuffers(t *testing.T) {
	t.Parallel()

	replicator := NewReplicator()
	replicator.AddPeer("player", Query{Radius: 10})
	payload := []byte("100")
	frame, err := replicator.BuildFrame("player", 1, []Entity{{
		ID:         1,
		Components: map[ComponentID][]byte{health: payload},
	}})
	require.NoError(t, err)
	payload[0] = '0'
	assert.Equal(t, []byte("100"), frame.Creates[0].Components[health])
}
