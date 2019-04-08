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

func TestSuperPutTooFull(t *testing.T) {
	p := NewSuperPool(PoolFactory, 1, 1, 0, 1)
	defer p.Close()

	// Not sure how to cause the ErrFull panic naturally, so I'm hacking in a value.
	state := p.State()
	state.InUse = 1
	p.state.Store(state)

	require.Panics(t, func() { p.Put(&TestResource{}) })
}

func TestSuperDrainBlock(t *testing.T) {
	p := NewSuperPool(SlowCloseFactory, 3, 3, 0, 0)
	defer p.Close()

	var resources []Resource
	for i := 0; i < 3; i++ {
		resources = append(resources, get(t, p))
	}
	p.Put(resources[0])
	require.Equal(t, State{Capacity: 3, InUse: 2, InPool: 1}, p.State())

	done := make(chan bool)
	go func() {
		require.NoError(t, p.SetCapacity(1, true))
		done <- true
	}()

	time.Sleep(time.Millisecond * 30)
	// The first resource should be closed by now, but waiting for the second to unblock.
	require.Equal(t, State{Capacity: 1, Draining: true, InUse: 2}, p.State())

	p.Put(resources[1])

	<-done
	require.Equal(t, State{Capacity: 1, InUse: 1}, p.State())

	// Clean up so p.Close() can run properly.
	p.Put(resources[2])
}

