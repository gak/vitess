package pools

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSuperOpen(t *testing.T) {
	p := NewSuperPool(PoolFactory, 1, 2, 0, 0)
	require.Equal(t, State{Capacity: 1}, p.State())
	p.Close()
}

func TestSuperGetPut(t *testing.T) {
	lastID.Set(0)
	p := NewSuperPool(PoolFactory, 1, 1, 0, 0)

	r := get(t, p).(*TestResource)
	require.Equal(t, r.num, int64(1))
	require.Equal(t, State{Capacity: 1, InUse: 1}, p.State())

	p.Put(r)
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 1, InPool: 1}, p.State())
	p.Close()
}

func TestSuperPutEmpty(t *testing.T) {
	p := NewSuperPool(PoolFactory, 1, 1, 0, 0)

	get(t, p)
	p.Put(nil)
	time.Sleep(time.Millisecond)

	require.Equal(t, len(p.pool), 0)
	require.Equal(t, State{Capacity: 1}, p.State())
	p.Close()
}

func TestSuperPutWithoutGet(t *testing.T) {
	p := NewSuperPool(PoolFactory, 1, 1, 0, 0)

	require.Panics(t, func() { p.Put(&TestResource{}) })
	require.Equal(t, State{Capacity: 1}, p.State())
	p.Close()
}

func TestSuperPutTooFull(t *testing.T) {
	p := NewSuperPool(PoolFactory, 1, 1, 0, 1)

	// Not sure how to cause the ErrFull panic naturally, so I'm hacking in a value.
	state := p.State()
	state.InUse = 1
	p.state.Store(state)

	require.Panics(t, func() { p.Put(&TestResource{}) })
	p.Close()
}

func TestSuperDrainBlock(t *testing.T) {
	p := NewSuperPool(SlowCloseFactory, 3, 3, 0, 0)

	var resources []Resource
	for i := 0; i < 3; i++ {
		resources = append(resources, get(t, p))
	}
	p.Put(resources[0])
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 3, InUse: 2, InPool: 1}, p.State())

	done := make(chan bool)
	go func() {
		require.NoError(t, p.SetCapacity(1, true))
		time.Sleep(time.Millisecond)
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
	p.Close()
}

func TestSuperDrainNoBlock(t *testing.T) {
	p := NewSuperPool(SlowCloseFactory, 2, 2, 0, 0)

	var resources []Resource
	for i := 0; i < 2; i++ {
		resources = append(resources, get(t, p))
	}
	p.Put(resources[0])
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 2, InUse: 1, InPool: 1}, p.State())

	require.NoError(t, p.SetCapacity(1, false))
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 1, Draining: true, InUse: 1, Closing: 1}, p.State())

	p.Put(resources[1])

	// Wait until the resources close, at least 20ms (2 * 10ms per SlowResource)
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, State{Capacity: 1, InPool: 1}, p.State())
	p.Close()
}

func TestSuperGrowBlock(t *testing.T) {
	p := NewSuperPool(PoolFactory, 1, 2, 0, 0)

	a := get(t, p)
	require.Equal(t, State{Capacity: 1, InUse: 1}, p.State())
	require.NoError(t, p.SetCapacity(2, true))
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 2, InUse: 1}, p.State())
	b := get(t, p)
	require.Equal(t, State{Capacity: 2, InUse: 2}, p.State())

	// Clean up
	p.Put(a)
	p.Put(b)
	p.Close()
}

func TestSuperGrowNoBlock(t *testing.T) {
	p := NewSuperPool(PoolFactory, 1, 2, 0, 0)

	a := get(t, p)
	require.Equal(t, State{Capacity: 1, InUse: 1}, p.State())
	require.NoError(t, p.SetCapacity(2, false))
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 2, InUse: 1}, p.State())
	b := get(t, p)
	require.Equal(t, State{Capacity: 2, InUse: 2}, p.State())

	// Clean up
	p.Put(a)
	p.Put(b)
	p.Close()
}

