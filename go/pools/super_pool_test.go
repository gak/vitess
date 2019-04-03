package pools

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSuperOpen(t *testing.T) {
	p := NewSuperPool(PoolFactory, 1, 2, 0, 0)
	defer p.Close()

	require.Equal(t, State{Capacity: 1}, p.State())
}

func TestSuperGetPut(t *testing.T) {
	lastID.Set(0)
	p := NewSuperPool(PoolFactory, 1, 1, 0, 0)
	defer p.Close()

	r := get(t, p).(*TestResource)
	require.Equal(t, r.num, int64(1))
	require.Equal(t, State{Capacity: 1, InUse: 1}, p.State())

	p.Put(r)
	require.Equal(t, State{Capacity: 1, InPool: 1}, p.State())
}

func TestSuperPutEmpty(t *testing.T) {
	p := NewSuperPool(PoolFactory, 1, 1, 0, 0)
	defer p.Close()

	get(t, p)
	p.Put(nil)

	require.Equal(t, len(p.pool), 0)
	require.Equal(t, State{Capacity: 1}, p.State())
}

func TestSuperPutWithoutGet(t *testing.T) {
	p := NewSuperPool(PoolFactory, 1, 1, 0, 0)
	defer p.Close()

	require.Panics(t, func() { p.Put(&TestResource{}) })
	require.Equal(t, State{Capacity: 1}, p.State())
}

func TestSuperPutTooFull(t *testing.T) {
	p := NewSuperPool(PoolFactory, 1, 1, 0, 1)
	defer p.Close()

	// Not sure how to cause the ErrFull panic naturally, so I'm hacking in a value.
	p.Lock()
	p.state.InUse = 1
	p.Unlock()

	require.Panics(t, func() { p.Put(&TestResource{}) })
	require.Equal(t, State{Capacity: 1, MinActive: 1, InPool: 1, InUse: 1}, p.State())

	// Allow p.Close() to not stall on a non-existent resource.
	p.Lock()
	p.state.InUse = 0
	p.Unlock()
}

