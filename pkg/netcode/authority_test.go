package netcode

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthoritySeparatesStateAndInputOwnership(t *testing.T) {
	t.Parallel()

	registry := NewAuthorityRegistry()
	registry.Register(1, Authority{State: "client", Input: "client"})

	require.NoError(t, registry.TransferState(1, "client", "server"))
	authority, ok := registry.Get(1)
	require.True(t, ok)
	assert.Equal(t, Authority{State: "server", Input: "client"}, authority)
	assert.ErrorIs(t, registry.TransferInput(1, "client", "other"), ErrNotStateAuthority)
	require.NoError(t, registry.TransferInput(1, "server", "other"))
}

func TestAuthorityRejectsUnknownEntities(t *testing.T) {
	t.Parallel()

	registry := NewAuthorityRegistry()
	err := registry.TransferState(99, "client", "server")
	assert.True(t, errors.Is(err, ErrEntityNotRegistered))
}
