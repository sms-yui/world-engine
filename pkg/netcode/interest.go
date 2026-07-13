package netcode

import "math"

// Query describes a peer's server-controlled area of interest. A zero layer
// mask matches every entity.
type Query struct {
	Center Vec3
	Radius float64
	Layers uint64
}

// InterestFilter decides whether an entity is visible to a peer.
type InterestFilter interface {
	Interested(peer PeerID, query Query, entity Entity) bool
}

// SphereInterest filters entities by radius and layer mask.
type SphereInterest struct{}

// Interested implements InterestFilter.
func (SphereInterest) Interested(_ PeerID, query Query, entity Entity) bool {
	if query.Radius < 0 {
		return false
	}
	if query.Layers != 0 && entity.Layers&query.Layers == 0 {
		return false
	}
	dx := query.Center.X - entity.Position.X
	dy := query.Center.Y - entity.Position.Y
	dz := query.Center.Z - entity.Position.Z
	return dx*dx+dy*dy+dz*dz <= query.Radius*query.Radius
}

// Projector applies game-specific network LOD. It may quantize or omit
// components without changing the authoritative entity.
type Projector interface {
	Project(peer PeerID, entity Entity, distance float64) Entity
}

type identityProjector struct{}

func (identityProjector) Project(_ PeerID, entity Entity, _ float64) Entity {
	return entity
}

func distance(left, right Vec3) float64 {
	dx := left.X - right.X
	dy := left.Y - right.Y
	dz := left.Z - right.Z
	return math.Sqrt(dx*dx + dy*dy + dz*dz)
}
