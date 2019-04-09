package pools

import (
	"context"
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

func TestSuperFull1(t *testing.T) {
	s := func(state State) State {
		state.Capacity = 10
		state.MinActive = 5
		return state
	}

	p := NewSuperPool(PoolFactory, 10, 10, 0, 5)
	time.Sleep(time.Millisecond)

	require.Equal(t, s(State{InPool: 5}), p.State())

	a := get(t, p)
	require.Equal(t, s(State{InPool: 4, InUse: 1}), p.State())

	var resources []Resource
	for i := 0; i < 9; i++ {
		resources = append(resources, get(t, p))
	}
	require.Equal(t, s(State{InPool: 0, InUse: 10}), p.State())

	var b Resource
	done := make(chan bool)
	go func() {
		t.Helper()
		b = get(t, p)
		done <- true
	}()
	time.Sleep(time.Millisecond)
	require.Equal(t, s(State{InPool: 0, InUse: 10, Waiters: 1}), p.State())

	p.Put(a)
	<-done
	require.NotZero(t, p.State().WaitTime)
	wt := p.State().WaitTime
	require.Equal(t, s(State{InPool: 0, InUse: 10, Waiters: 0, WaitCount: 1, WaitTime: wt}), p.State())

	p.Put(b)
	require.Equal(t, s(State{InPool: 1, InUse: 9, Waiters: 0, WaitCount: 1, WaitTime: wt}), p.State())

	// Clean up
	for _, r := range resources {
		p.Put(r)
	}

	p.Close()
}

func TestSuperFull2(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewSuperPool(PoolFactory, 6, 6, time.Second, 0)

	err := p.SetCapacity(5, true)
	require.NoError(t, err)
	var resources [10]Resource

	for i := 0; i < 5; i++ {
		r, err := p.Get(ctx)
		resources[i] = r
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		if p.Available() != 5-i-1 {
			t.Errorf("expecting %d, received %d", 5-i-1, p.Available())
			return
		}
		if p.WaitCount() != 0 {
			t.Errorf("expecting 0, received %d", p.WaitCount())
			return
		}
		if p.WaitTime() != 0 {
			t.Errorf("expecting 0, received %d", p.WaitTime())
		}
		if lastID.Get() != int64(i+1) {
			t.Errorf("Expecting %d, received %d", i+1, lastID.Get())
		}
		if count.Get() != int64(i+1) {
			t.Errorf("Expecting %d, received %d", i+1, count.Get())
		}
	}

	ch := make(chan bool)
	go func() {
		for i := 0; i < 5; i++ {
			r, err := p.Get(ctx)
			if err != nil {
				t.Errorf("Get failed: %v", err)
				panic("exit")
			}
			resources[i] = r
		}
		for i := 0; i < 5; i++ {
			p.Put(resources[i])
		}
		ch <- true
	}()
	for i := 0; i < 5; i++ {
		// Sleep to ensure the goroutine waits
		time.Sleep(10 * time.Millisecond)
		p.Put(resources[i])
	}
	<-ch
	if p.WaitCount() != 5 {
		t.Errorf("Expecting 5, received %d", p.WaitCount())
	}
	if p.WaitTime() == 0 {
		t.Errorf("Expecting non-zero")
	}
	if lastID.Get() != 5 {
		t.Errorf("Expecting 5, received %d", lastID.Get())
	}

	// TestSuper Close resource
	r, err := p.Get(ctx)
	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	r.Close()

	p.Put(nil)
	if count.Get() != 4 {
		t.Errorf("Expecting 4, received %d", count.Get())
	}
	for i := 0; i < 5; i++ {
		r, err := p.Get(ctx)
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		resources[i] = r
	}
	for i := 0; i < 5; i++ {
		p.Put(resources[i])
	}
	if count.Get() != 5 {
		t.Errorf("Expecting 5, received %d", count.Get())
	}
	if lastID.Get() != 6 {
		t.Errorf("Expecting 6, received %d", lastID.Get())
	}
	// SetCapacity
	p.SetCapacity(3, true)
	if count.Get() != 3 {
		t.Errorf("Expecting 3, received %d", count.Get())
		return
	}
	if lastID.Get() != 6 {
		t.Errorf("Expecting 6, received %d", lastID.Get())
	}
	if p.Capacity() != 3 {
		t.Errorf("Expecting 3, received %d", p.Capacity())
	}
	if p.Available() != 3 {
		t.Errorf("Expecting 3, received %d", p.Available())
	}
	p.SetCapacity(6, true)
	if p.Capacity() != 6 {
		t.Errorf("Expecting 6, received %d", p.Capacity())
	}
	if p.Available() != 6 {
		t.Errorf("Expecting 6, received %d", p.Available())
	}
	for i := 0; i < 6; i++ {
		r, err := p.Get(ctx)
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		resources[i] = r
	}
	for i := 0; i < 6; i++ {
		p.Put(resources[i])
	}
	if count.Get() != 6 {
		t.Errorf("Expecting 5, received %d", count.Get())
	}
	if lastID.Get() != 9 {
		t.Errorf("Expecting 9, received %d", lastID.Get())
	}

	// Close
	p.Close()
	if p.Capacity() != 0 {
		t.Errorf("Expecting 0, received %d", p.Capacity())
	}
	if p.Available() != 0 {
		t.Errorf("Expecting 0, received %d", p.Available())
	}
	if count.Get() != 0 {
		t.Errorf("Expecting 0, received %d", count.Get())
	}

	p.Close()
}

