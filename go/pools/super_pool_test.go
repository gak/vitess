package pools

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func replaceState(p *SuperPool, f func(state *State)) {
	s := p.State()
	f(&s)
	p.cmd <- testingOnlyReplaceState{state: s}
	time.Sleep(time.Millisecond)
}

func TestSuperOpen(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 1, OpenWorkers: 1, CloseWorkers: 1})
	require.Equal(t, State{Capacity: 1}, p.State())
	p.Close()
}

func TestSuperGetPut(t *testing.T) {
	lastID.Set(0)
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 1, OpenWorkers: 1, CloseWorkers: 1})

	r := get(t, p).(*TestResource)
	require.Equal(t, r.num, int64(1))
	require.Equal(t, State{Capacity: 1, InUse: 1}, p.State())

	p.Put(r)
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 1, InPool: 1}, p.State())
	p.Close()
}

func TestSuperPutEmpty(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 1, OpenWorkers: 1, CloseWorkers: 1})

	get(t, p)
	p.Put(nil)
	time.Sleep(time.Millisecond)

	require.Equal(t, len(p.pool), 0)
	require.Equal(t, State{Capacity: 1}, p.State())
	p.Close()
}

func TestSuperPutWithoutGet(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 1, OpenWorkers: 1, CloseWorkers: 1})

	require.Panics(t, func() { p.Put(&TestResource{}) })
	require.Equal(t, State{Capacity: 1}, p.State())
	p.Close()
}

func TestSuperGetWait(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 1, OpenWorkers: 1, CloseWorkers: 1})

	a := get(t, p)
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 1, InUse: 1}, p.State())

	go func() {
		time.Sleep(time.Millisecond * 10)
		require.Equal(t, State{Capacity: 1, InUse: 1, Waiters: 1}, p.State())
		p.Put(a)
	}()

	b := get(t, p)
	time.Sleep(time.Millisecond)
	wt := p.State().WaitTime
	require.Equal(t, State{Capacity: 1, InUse: 1, WaitCount: 1, WaitTime: wt}, p.State())

	p.Put(b)
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 1, InPool: 1, WaitCount: 1, WaitTime: wt}, p.State())
	p.Close()
}

func TestSuperGetWaitNil(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 1, OpenWorkers: 1, CloseWorkers: 1})

	get(t, p)
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 1, InUse: 1}, p.State())

	go func() {
		time.Sleep(time.Millisecond * 10)
		require.Equal(t, State{Capacity: 1, InUse: 1, Waiters: 1}, p.State())
		p.Put(nil)
	}()

	b := get(t, p)
	time.Sleep(time.Millisecond)
	wt := p.State().WaitTime
	require.Equal(t, State{Capacity: 1, InUse: 1, WaitCount: 1, WaitTime: wt}, p.State())

	p.Put(b)
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 1, InPool: 1, WaitCount: 1, WaitTime: wt}, p.State())

	p.Close()
}

func TestSuperPutTooFull(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 1, OpenWorkers: 1, CloseWorkers: 1})

	// Not sure how to cause the ErrFull panic naturally, so I'm hacking in a value.
	replaceState(p, func(s *State) {
		s.InUse = 2
	})

	require.Panics(t, func() { p.Put(&TestResource{}) })

	replaceState(p, func(s *State) {
		s.InUse = 0
	})

	p.Close()
}

func TestSuperDrainBlock(t *testing.T) {
	p := NewSuperPool(Opts{Factory: SlowCloseFactory, Capacity: 3, OpenWorkers: 1, CloseWorkers: 1})

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
	p := NewSuperPool(Opts{Factory: SlowCloseFactory, Capacity: 2, OpenWorkers: 1, CloseWorkers: 1})

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
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 1, OpenWorkers: 1, CloseWorkers: 1})

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
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 1, OpenWorkers: 1, CloseWorkers: 1})

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

	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 10, MinActive: 5, OpenWorkers: 1, CloseWorkers: 1})
	time.Sleep(time.Millisecond * 10)

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
	time.Sleep(time.Millisecond)
	require.NotZero(t, p.State().WaitTime)
	wt := p.State().WaitTime
	require.Equal(t, s(State{InPool: 0, InUse: 10, Waiters: 0, WaitCount: 1, WaitTime: wt}), p.State())

	p.Put(b)
	time.Sleep(time.Millisecond)
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
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 6, IdleTimeout: time.Second, OpenWorkers: 1, CloseWorkers: 1})

	err := p.SetCapacity(5, true)
	require.NoError(t, err)
	var resources [10]Resource

	for i := 0; i < 5; i++ {
		r, err := p.Get(ctx)
		time.Sleep(time.Millisecond)
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
	require.NoError(t, p.SetCapacity(3, true))
	time.Sleep(time.Millisecond)
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
	require.NoError(t, p.SetCapacity(6, true))
	time.Sleep(time.Millisecond)
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
}

func TestSuperShrinking(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 5, IdleTimeout: time.Second, OpenWorkers: 1, CloseWorkers: 1})

	var resources [10]Resource
	// Leave one empty slot in the pool
	for i := 0; i < 4; i++ {
		resources[i] = get(t, p)
	}

	done := make(chan bool)
	go func() {
		t.Helper()
		require.NoError(t, p.SetCapacity(3, true))
		done <- true
	}()
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 3, InUse: 4, IdleTimeout: time.Second, Draining: true}, p.State())
	require.Equal(t, 0, p.Available())
	require.Equal(t, 4, p.Active())

	// There are already 2 resources available in the pool.
	// So, returning one should be enough for SetCapacity to complete.
	p.Put(resources[3])
	<-done
	time.Sleep(time.Millisecond * 10)

	// Return the rest of the resources
	for i := 0; i < 3; i++ {
		p.Put(resources[i])
	}
	time.Sleep(time.Millisecond * 10)

	require.Equal(t, State{Capacity: 3, InPool: 3, IdleTimeout: time.Second}, p.State())
	require.Equal(t, 3, p.Available())
	require.Equal(t, 3, p.Active())
	require.Equal(t, 3, int(count.Get()))

	// Ensure no deadlock if SetCapacity is called after we start
	// waiting for a resource
	var err error
	for i := 0; i < 3; i++ {
		resources[i] = get(t, p)
	}
	// This will wait because pool is empty
	go func() {
		r := get(t, p)
		p.Put(r)
		done <- true
	}()

	// This will also wait
	go func() {
		p.SetCapacity(2, true)
		done <- true
	}()
	time.Sleep(10 * time.Millisecond)

	// This should not hang
	for i := 0; i < 3; i++ {
		p.Put(resources[i])
	}
	<-done
	<-done
	time.Sleep(time.Millisecond)

	if p.Capacity() != 2 {
		t.Errorf("Expecting 2, received %d", p.Capacity())
	}
	if p.Available() != 2 {
		t.Errorf("Expecting 2, received %d", p.Available())
	}
	if p.WaitCount() != 1 {
		t.Errorf("Expecting 1, received %d", p.WaitCount())
		return
	}
	if count.Get() != 2 {
		t.Errorf("Expecting 2, received %d", count.Get())
	}

	// TestSuper race condition of SetCapacity with itself
	p.SetCapacity(3, true)
	for i := 0; i < 3; i++ {
		resources[i], err = p.Get(ctx)
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
	}
	// This will wait because pool is empty
	go func() {
		r, err := p.Get(ctx)
		if err != nil {
			t.Errorf("Unexpected error %v", err)
		}
		p.Put(r)
		done <- true
	}()
	time.Sleep(10 * time.Millisecond)

	// This will wait till we Put
	go p.SetCapacity(2, true)
	time.Sleep(10 * time.Millisecond)
	go p.SetCapacity(4, true)
	time.Sleep(10 * time.Millisecond)

	// This should not hang
	for i := 0; i < 3; i++ {
		p.Put(resources[i])
	}
	<-done

	require.Error(t, p.SetCapacity(-1, true))
	time.Sleep(time.Millisecond)
	require.Equal(t, p.Capacity(), 4)
	require.Equal(t, p.Available(), 4)

	p.Close()
}

func TestSuperClosing(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 5, IdleTimeout: time.Second, OpenWorkers: 1, CloseWorkers: 1})
	var resources [10]Resource
	for i := 0; i < 5; i++ {
		r, err := p.Get(ctx)
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		resources[i] = r
	}
	ch := make(chan bool)
	go func() {
		p.Close()
		ch <- true
	}()

	// Wait for goroutine to call Close
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, State{InUse: 5, Closed: true, Draining: true, IdleTimeout: time.Second}, p.State())

	// Put is allowed when closing
	for i := 0; i < 5; i++ {
		p.Put(resources[i])
	}

	// Wait for Close to return
	<-ch

	// SetCapacity must be ignored after Close
	err := p.SetCapacity(1, true)
	require.Error(t, err)
	require.Equal(t, State{Closed: true, IdleTimeout: time.Second}, p.State())
	require.Equal(t, 5, int(lastID.Get()))
	require.Equal(t, 0, int(count.Get()))
}

func TestSuperGetAfterClose(t *testing.T) {
	p := NewSuperPool(Opts{Factory: SlowCloseFactory, Capacity: 5, IdleTimeout: time.Second, OpenWorkers: 1, CloseWorkers: 1})
	a := get(t, p)

	done := make(chan bool)
	go func() {
		p.Close()
		done <- true
	}()

	time.Sleep(time.Millisecond)

	_, err := p.Get(context.Background())
	require.Error(t, err)

	p.Put(a)

	<-done
}

func TestSuperIdleTimeout(t *testing.T) {
	lastID.Set(0)
	count.Set(0)
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 1, IdleTimeout: 10 * time.Millisecond, OpenWorkers: 1, CloseWorkers: 1})

	r := get(t, p)
	require.Equal(t, 1, int(count.Get()))
	require.Equal(t, 0, int(p.IdleClosed()))

	p.Put(r)
	require.Equal(t, 1, int(count.Get()))
	require.Equal(t, 0, int(p.IdleClosed()))

	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 0, int(count.Get()))
	require.Equal(t, 1, int(p.IdleClosed()))
	require.Equal(t, 0, int(p.InUse()))
	require.Equal(t, 0, int(p.State().InPool))

	r = get(t, p)
	require.Equal(t, 2, int(lastID.Get()))
	require.Equal(t, 1, int(count.Get()))
	require.Equal(t, 1, int(p.IdleClosed()))

	// Sleep to let the idle closer run while all resources are in use
	// then make sure things are still as we expect.
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 2, int(lastID.Get()))
	require.Equal(t, 1, int(count.Get()))
	require.Equal(t, 1, int(p.IdleClosed()))
	require.Equal(t, 1, int(p.InUse()))

	p.Put(r)
	time.Sleep(time.Millisecond)
	r = get(t, p)
	require.Equal(t, 2, int(lastID.Get()))
	require.Equal(t, 1, int(count.Get()))
	require.Equal(t, 1, int(p.IdleClosed()))

	// The idle close thread wakes up every 1/10 of the idle time, so ensure
	// the timeout change applies to newly added resources.
	p.SetIdleTimeout(200 * time.Millisecond)
	p.Put(r)

	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 2, int(lastID.Get()))
	require.Equal(t, 1, int(count.Get()))
	require.Equal(t, 1, int(p.IdleClosed()))

	p.SetIdleTimeout(10 * time.Millisecond)
	// The previous 200ms timeout will wait 20ms per check, so wait for a bit longer.
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, 2, int(lastID.Get()))
	require.Equal(t, 0, int(count.Get()))
	require.Equal(t, 2, int(p.IdleClosed()))

	p.Close()
}

func TestSuperCreateFail(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewSuperPool(Opts{Factory: FailFactory, Capacity: 5, IdleTimeout: time.Second, OpenWorkers: 1, CloseWorkers: 1})

	_, err := p.Get(ctx)
	require.Error(t, err)

	require.EqualError(t, err, "Failed")
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 5, IdleTimeout: time.Second}, p.State())
	require.Equal(t, 5, p.Available())
	require.Equal(t, 0, p.Active())

	p.Close()
}

func TestSuperSlowCreateFail(t *testing.T) {
	lastID.Set(0)
	count.Set(0)
	p := NewSuperPool(Opts{Factory: SlowFailFactory, Capacity: 2, IdleTimeout: time.Second, OpenWorkers: 1, CloseWorkers: 1})

	ch := make(chan bool)
	// The third Get should not wait indefinitely
	for i := 0; i < 3; i++ {
		go func() {
			_, err := p.Get(context.Background())
			require.Error(t, err)
			ch <- true
		}()
	}
	for i := 0; i < 3; i++ {
		<-ch
	}
	time.Sleep(time.Millisecond)
	require.Equal(t, p.Available(), 2)

	p.Close()
}

func TestSuperTimeoutSlow(t *testing.T) {
	ctx := context.Background()
	p := NewSuperPool(Opts{Factory: SlowCreateFactory, Capacity: 4, IdleTimeout: time.Second, OpenWorkers: 1, CloseWorkers: 1})

	var resources []Resource
	for i := 0; i < 4; i++ {
		resources = append(resources, get(t, p))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Millisecond*5))
	r, err := p.Get(ctx)
	require.EqualError(t, err, "resource pool timed out")
	cancel()

	time.Sleep(time.Millisecond)

	expected := State{Capacity: 4, InUse: 4, IdleTimeout: time.Second}
	require.Equal(t, expected, p.State())

	for _, r = range resources {
		p.Put(r)
	}

	p.Close()
}

func TestSuperTimeoutPoolFull(t *testing.T) {
	ctx := context.Background()
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 1, IdleTimeout: time.Second, OpenWorkers: 1, CloseWorkers: 1})

	b := get(t, p)
	expected := State{Capacity: 1, InUse: 1, IdleTimeout: time.Second}
	require.Equal(t, expected, p.State())

	shortCtx, _ := context.WithTimeout(ctx, time.Millisecond*50)
	_, err := p.Get(shortCtx)
	require.EqualError(t, err, "resource pool timed out")

	time.Sleep(time.Millisecond * 10)
	require.Equal(t, expected, p.State())

	p.Put(b)
	time.Sleep(time.Millisecond)
	expected.InUse = 0
	expected.InPool = 1
	require.Equal(t, expected, p.State())

	p.Close()
}

func TestSuperExpired(t *testing.T) {
	lastID.Set(0)
	count.Set(0)
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 1, IdleTimeout: time.Second, OpenWorkers: 1, CloseWorkers: 1})
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Second))
	r, err := p.Get(ctx)
	if err == nil {
		p.Put(r)
	}
	cancel()
	want := "resource pool timed out"
	if err == nil || err.Error() != want {
		t.Errorf("got %v, want %s", err, want)
	}

	p.Close()
}

func TestSuperMinActive(t *testing.T) {
	lastID.Set(0)
	count.Set(0)
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 5, IdleTimeout: time.Second, MinActive: 3, OpenWorkers: 1, CloseWorkers: 1})
	time.Sleep(time.Millisecond)

	if p.Available() != 5 {
		t.Errorf("Expecting 5, received %d", p.Available())
	}
	if p.Active() != 3 {
		t.Errorf("Expecting 3, received %d", p.Active())
	}
	if lastID.Get() != 3 {
		t.Errorf("Expecting 3, received %d", lastID.Get())
	}
	if count.Get() != 3 {
		t.Errorf("Expecting 3, received %d", count.Get())
	}

	p.Close()
}

func TestSuperMinActiveWithExpiry(t *testing.T) {
	timeout := time.Millisecond * 20
	lastID.Set(0)
	count.Set(0)
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 5, IdleTimeout: timeout, MinActive: 3, OpenWorkers: 1, CloseWorkers: 1})
	time.Sleep(time.Millisecond)

	require.Equal(t, 3, p.Active())

	expected := State{Capacity: 5, MinActive: 3, IdleTimeout: timeout}

	if count.Get() != 3 {
		t.Errorf("Expecting 3, received %d", count.Get())
		return
	}

	// Get one more than the minActive.
	var resources []Resource
	for i := 0; i < 4; i++ {
		r, err := p.Get(context.Background())
		if err != nil {
			t.Errorf("Got an unexpected error: %v", err)
		}
		resources = append(resources, r)
	}
	time.Sleep(time.Millisecond)

	// Should have only ever allocated 4 (3 min, 1 extra).
	expected.InUse = 4
	require.Equal(t, expected, p.State())
	require.Equal(t, 4, p.Active())
	require.Equal(t, 4, int(count.Get()))

	// Put one resource back and let it expire.
	p.Put(resources[0])
	time.Sleep(timeout * 2)

	expected.InPool = 0
	expected.InUse = 3
	expected.IdleClosed = 1
	require.Equal(t, expected, p.State())
	require.Equal(t, 3, p.Active())

	// Clean up
	p.Put(resources[1])
	p.Put(resources[2])
	p.Put(resources[3])
	p.Close()
}

func TestSuperMinActiveSelfRefreshing(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 5, IdleTimeout: time.Second, MinActive: 3, OpenWorkers: 1, CloseWorkers: 1})
	time.Sleep(time.Millisecond)

	// Get 5
	var resources []Resource
	for i := 0; i < 5; i++ {
		resources = append(resources, get(t, p))
	}

	// Put back all nils except one
	for i := 0; i < 4; i++ {
		p.Put(nil)
	}
	p.Put(resources[4])

	time.Sleep(200 * time.Millisecond)

	require.Equal(t, 3, p.Active())
	p.Close()
}

func TestSuperMinActiveTooHigh(t *testing.T) {
	defer func() {
		if r := recover(); r.(error).Error() != "minActive 2 higher than capacity 1" {
			t.Errorf("Did not panic correctly: %v", r)
		}
	}()

	NewSuperPool(Opts{Factory: FailFactory, Capacity: 1, IdleTimeout: time.Second, MinActive: 2, OpenWorkers: 1, CloseWorkers: 1})
}

func TestSuperCloseIdleResourcesInPoolChangingRaceAttempt(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 100, IdleTimeout: time.Millisecond, OpenWorkers: 100, CloseWorkers: 100})

	done := make(chan bool)
	go func() {
		var rs []Resource
		for total := 0; total < 1000; total += 20 {
			rs = nil
			for i := 0; i < 100; i++ {
				time.Sleep(time.Duration(total))
				r, err := p.Get(context.Background())
				if err != nil {
					fmt.Println(err)
					continue
				}
				rs = append(rs, r)
			}
			time.Sleep(time.Millisecond)

			for _, r := range rs {
				time.Sleep(time.Duration(total))
				p.Put(r)
			}
		}
		done <- true
	}()
	<-done

	require.NotZero(t, p.State().IdleClosed)

	p.Close()
}

func TestSuperMinActiveTooHighAfterSetCapacity(t *testing.T) {
	p := NewSuperPool(Opts{Factory: FailFactory, Capacity: 3, IdleTimeout: time.Second, MinActive: 2, OpenWorkers: 1, CloseWorkers: 1})

	require.NoError(t, p.SetCapacity(2, true))

	err := p.SetCapacity(1, true)
	expecting := "minActive 2 would now be higher than capacity 1"
	if err == nil || err.Error() != expecting {
		t.Errorf("Expecting: %v, instead got: %v", expecting, err)
	}

	p.Close()
}

func TestSuperGetPutRace(t *testing.T) {
	p := NewSuperPool(Opts{Factory: SlowCreateFactory, Capacity: 1, IdleTimeout: 10 * time.Nanosecond, OpenWorkers: 20, CloseWorkers: 20})

	for j := 0; j < 200; j++ {
		done := make(chan bool)
		for i := 0; i < 2; i++ {
			go func() {
				time.Sleep(time.Duration(rand.Int() % 10))
				r, err := p.Get(context.Background())
				if err != nil {
					panic(err)
				}

				time.Sleep(time.Duration(rand.Int() % 10))

				p.Put(r)
				done <- true
			}()
			runtime.Gosched()
		}
		<-done
		<-done
	}

	p.Close()
}

func TestSuperCloseTwicePanic(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 1, OpenWorkers: 1, CloseWorkers: 1})
	p.Close()
	require.Panics(t, func() {
		p.Close()
	})
}

func TestSuperSetCapacityWhileClosed(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 1, OpenWorkers: 1, CloseWorkers: 1})
	p.Close()
	require.Error(t, p.SetCapacity(5, true))
	require.Error(t, p.SetCapacity(5, false))
}

func TestSuperSetCapacityTwice(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: 2, OpenWorkers: 1, CloseWorkers: 1})
	a := get(t, p)
	b := get(t, p)
	var err error
	done := make(chan bool)
	go func() {
		err = p.SetCapacity(1, true)
		done <- true
	}()

	time.Sleep(time.Millisecond)
	require.NoError(t, p.SetCapacity(10, true))

	<-done
	require.Error(t, err)

	p.Put(a)
	p.Put(b)

	p.Close()
}

func TestSuperPanicStringCreateFactory(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PanicStringCreateFactory, Capacity: 10, OpenWorkers: 1, CloseWorkers: 1})
	_, err := p.Get(context.Background())
	require.EqualError(t, err, "resource creation panicked: resource")
	p.Close()
}

func TestSuperPanicErrorCreateFactory(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PanicErrorCreateFactory, Capacity: 10, OpenWorkers: 1, CloseWorkers: 1})
	_, err := p.Get(context.Background())
	require.Error(t, err)
	p.Close()
}

func TestSuperPanicStringCloseFactory(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PanicStringCloseFactory, Capacity: 10, OpenWorkers: 1, CloseWorkers: 1})
	r := get(t, p)
	p.Put(r)
	require.Panics(t, func() {
		p.Close()
	})
}

func TestSuperPanicErrorCloseFactory(t *testing.T) {
	p := NewSuperPool(Opts{Factory: PanicErrorCloseFactory, Capacity: 10, OpenWorkers: 1, CloseWorkers: 1})
	r := get(t, p)
	p.Put(r)
	require.Panics(t, func() {
		p.Close()
	})
}

func TestSuperFullOpenQueue(t *testing.T) {
	capacity := 1000
	p := NewSuperPool(Opts{Factory: PoolFactory, Capacity: capacity, OpenWorkers: 10, CloseWorkers: 1, MaxOpenQueue: 1})

	jobs := make(chan bool, capacity)
	for i := 0; i < capacity; i++ {
		jobs <- true
	}

	type ret struct {
		r   Resource
		err error
	}

	done := make(chan ret)
	for i := 0; i < 100; i++ {
		go func() {
			for range jobs {
				r, err := p.Get(context.Background())
				if err != nil {
					done <- ret{err: err}
				} else {
					done <- ret{r: r}
				}
			}
		}()
	}

	var rs []Resource
	didErr := false
	for i := 0; i < capacity; i++ {
		result := <-done
		if result.err != nil {
			didErr = true
			require.EqualError(t, result.err, ErrOpenBufferFull.Error())
		} else {
			rs = append(rs, result.r)
		}
	}
	require.True(t, didErr)
	close(jobs)

	for _, r := range rs {
		p.Put(r)
	}
	time.Sleep(time.Millisecond * 100)
	require.Equal(t, State{Capacity: capacity, InPool: len(rs)}, p.State())
	p.Close()
}
