package pools

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
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
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 1, InPool: 1}, p.State())
}

func TestSuperPutEmpty(t *testing.T) {
	p := NewSuperPool(PoolFactory, 1, 1, 0, 0)
	defer p.Close()

	get(t, p)
	p.Put(nil)
	time.Sleep(time.Millisecond)

	require.Equal(t, len(p.pool), 0)
	require.Equal(t, State{Capacity: 1}, p.State())
}

func TestSuperPutWithoutGet(t *testing.T) {
	p := NewSuperPool(PoolFactory, 1, 1, 0, 0)
	defer p.Close()

	require.Panics(t, func() { p.Put(&TestResource{}) })
	require.Equal(t, State{Capacity: 1}, p.State())
}

