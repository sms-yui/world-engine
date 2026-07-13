// Package netcode provides transport-independent building blocks for authoritative,
// tick-based game state replication.
package netcode

// PeerID identifies a client, simulator, or server participating in a session.
type PeerID string

// EntityID identifies a replicated game entity.
type EntityID uint64

// ComponentID identifies a game-defined replicated component schema.
type ComponentID uint32

// Vec3 is the spatial coordinate used for interest management. Games may map
// their native coordinate type to it at the replication boundary.
type Vec3 struct {
	X float64
	Y float64
	Z float64
}

// Entity is the transport-neutral state of one replicated entity. Components
// contain bytes encoded by the game-defined schema.
type Entity struct {
	ID         EntityID
	Position   Vec3
	Layers     uint64
	Components map[ComponentID][]byte
}

func cloneEntity(entity Entity) Entity {
	clone := entity
	clone.Components = make(map[ComponentID][]byte, len(entity.Components))
	for id, value := range entity.Components {
		clone.Components[id] = bytesClone(value)
	}
	return clone
}

func bytesClone(value []byte) []byte {
	return append([]byte(nil), value...)
}

func cloneState(state map[EntityID]Entity) map[EntityID]Entity {
	clone := make(map[EntityID]Entity, len(state))
	for id, entity := range state {
		clone[id] = cloneEntity(entity)
	}
	return clone
}
