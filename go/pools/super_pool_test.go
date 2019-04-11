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
	p := NewSuperPool(PoolFactory, 1, 1, 0, 0)

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

//func TestSuperFull1(t *testing.T) {
//	s := func(state State) State {
//		state.Capacity = 10
//		state.MinActive = 5
//		return state
//	}
//
//	p := NewSuperPool(PoolFactory, 10, 10, 0, 5)
//	time.Sleep(time.Millisecond)
//
//	require.Equal(t, s(State{InPool: 5}), p.State())
//
//	a := get(t, p)
//	require.Equal(t, s(State{InPool: 4, InUse: 1}), p.State())
//
//	var resources []Resource
//	for i := 0; i < 9; i++ {
//		resources = append(resources, get(t, p))
//	}
//	require.Equal(t, s(State{InPool: 0, InUse: 10}), p.State())
//
//	var b Resource
//	done := make(chan bool)
//	go func() {
//		t.Helper()
//		b = get(t, p)
//		done <- true
//	}()
//	time.Sleep(time.Millisecond)
//	require.Equal(t, s(State{InPool: 0, InUse: 10, Waiters: 1}), p.State())
//
//	p.Put(a)
//	<-done
//	require.NotZero(t, p.State().WaitTime)
//	wt := p.State().WaitTime
//	require.Equal(t, s(State{InPool: 0, InUse: 10, Waiters: 0, WaitCount: 1, WaitTime: wt}), p.State())
//
//	p.Put(b)
//	require.Equal(t, s(State{InPool: 1, InUse: 9, Waiters: 0, WaitCount: 1, WaitTime: wt}), p.State())
//
//	// Clean up
//	for _, r := range resources {
//		p.Put(r)
//	}
//
//	p.Close()
//}
//
//func TestSuperFull2(t *testing.T) {
//	ctx := context.Background()
//	lastID.Set(0)
//	count.Set(0)
//	p := NewSuperPool(PoolFactory, 6, 6, time.Second, 0)
//
//	err := p.SetCapacity(5, true)
//	require.NoError(t, err)
//	var resources [10]Resource
//
//	for i := 0; i < 5; i++ {
//		r, err := p.Get(ctx)
//		resources[i] = r
//		if err != nil {
//			t.Errorf("Unexpected error %v", err)
//		}
//		if p.Available() != 5-i-1 {
//			t.Errorf("expecting %d, received %d", 5-i-1, p.Available())
//			return
//		}
//		if p.WaitCount() != 0 {
//			t.Errorf("expecting 0, received %d", p.WaitCount())
//			return
//		}
//		if p.WaitTime() != 0 {
//			t.Errorf("expecting 0, received %d", p.WaitTime())
//		}
//		if lastID.Get() != int64(i+1) {
//			t.Errorf("Expecting %d, received %d", i+1, lastID.Get())
//		}
//		if count.Get() != int64(i+1) {
//			t.Errorf("Expecting %d, received %d", i+1, count.Get())
//		}
//	}
//
//	ch := make(chan bool)
//	go func() {
//		for i := 0; i < 5; i++ {
//			r, err := p.Get(ctx)
//			if err != nil {
//				t.Errorf("Get failed: %v", err)
//				panic("exit")
//			}
//			resources[i] = r
//		}
//		for i := 0; i < 5; i++ {
//			p.Put(resources[i])
//		}
//		ch <- true
//	}()
//	for i := 0; i < 5; i++ {
//		// Sleep to ensure the goroutine waits
//		time.Sleep(10 * time.Millisecond)
//		p.Put(resources[i])
//	}
//	<-ch
//	if p.WaitCount() != 5 {
//		t.Errorf("Expecting 5, received %d", p.WaitCount())
//	}
//	if p.WaitTime() == 0 {
//		t.Errorf("Expecting non-zero")
//	}
//	if lastID.Get() != 5 {
//		t.Errorf("Expecting 5, received %d", lastID.Get())
//	}
//
//	// TestSuper Close resource
//	r, err := p.Get(ctx)
//	if err != nil {
//		t.Errorf("Unexpected error %v", err)
//	}
//	r.Close()
//
//	p.Put(nil)
//	if count.Get() != 4 {
//		t.Errorf("Expecting 4, received %d", count.Get())
//	}
//	for i := 0; i < 5; i++ {
//		r, err := p.Get(ctx)
//		if err != nil {
//			t.Errorf("Get failed: %v", err)
//		}
//		resources[i] = r
//	}
//	for i := 0; i < 5; i++ {
//		p.Put(resources[i])
//	}
//	if count.Get() != 5 {
//		t.Errorf("Expecting 5, received %d", count.Get())
//	}
//	if lastID.Get() != 6 {
//		t.Errorf("Expecting 6, received %d", lastID.Get())
//	}
//	// SetCapacity
//	p.SetCapacity(3, true)
//	if count.Get() != 3 {
//		t.Errorf("Expecting 3, received %d", count.Get())
//		return
//	}
//	if lastID.Get() != 6 {
//		t.Errorf("Expecting 6, received %d", lastID.Get())
//	}
//	if p.Capacity() != 3 {
//		t.Errorf("Expecting 3, received %d", p.Capacity())
//	}
//	if p.Available() != 3 {
//		t.Errorf("Expecting 3, received %d", p.Available())
//	}
//	p.SetCapacity(6, true)
//	if p.Capacity() != 6 {
//		t.Errorf("Expecting 6, received %d", p.Capacity())
//	}
//	if p.Available() != 6 {
//		t.Errorf("Expecting 6, received %d", p.Available())
//	}
//	for i := 0; i < 6; i++ {
//		r, err := p.Get(ctx)
//		if err != nil {
//			t.Errorf("Get failed: %v", err)
//		}
//		resources[i] = r
//	}
//	for i := 0; i < 6; i++ {
//		p.Put(resources[i])
//	}
//	if count.Get() != 6 {
//		t.Errorf("Expecting 5, received %d", count.Get())
//	}
//	if lastID.Get() != 9 {
//		t.Errorf("Expecting 9, received %d", lastID.Get())
//	}
//
//	// Close
//	p.Close()
//	if p.Capacity() != 0 {
//		t.Errorf("Expecting 0, received %d", p.Capacity())
//	}
//	if p.Available() != 0 {
//		t.Errorf("Expecting 0, received %d", p.Available())
//	}
//	if count.Get() != 0 {
//		t.Errorf("Expecting 0, received %d", count.Get())
//	}
//
//	p.Close()
//}
//
//func TestSuperShrinking(t *testing.T) {
//	ctx := context.Background()
//	lastID.Set(0)
//	count.Set(0)
//	p := NewSuperPool(PoolFactory, 5, 5, time.Second, 0)
//	defer p.Close()
//
//	var resources [10]Resource
//	// Leave one empty slot in the pool
//	for i := 0; i < 4; i++ {
//		resources[i] = get(t, p)
//	}
//
//	done := make(chan bool)
//	go func() {
//		t.Helper()
//		require.NoError(t, p.SetCapacity(3, true))
//		done <- true
//	}()
//	time.Sleep(time.Millisecond)
//	require.Equal(t, State{Capacity: 3, InUse: 4, IdleTimeout: time.Second, Draining: true}, p.State())
//	require.Equal(t, 0, p.Available())
//	require.Equal(t, 4, p.Active())
//
//	// There are already 2 resources available in the pool.
//	// So, returning one should be enough for SetCapacity to complete.
//	p.Put(resources[3])
//	<-done
//	time.Sleep(time.Millisecond * 10)
//
//	// Return the rest of the resources
//	for i := 0; i < 3; i++ {
//		p.Put(resources[i])
//	}
//	time.Sleep(time.Millisecond * 10)
//
//	require.Equal(t, State{Capacity: 3, InPool: 3, IdleTimeout: time.Second}, p.State())
//	require.Equal(t, 3, p.Available())
//	require.Equal(t, 3, p.Active())
//	require.Equal(t, 3, int(count.Get()))
//
//	// Ensure no deadlock if SetCapacity is called after we start
//	// waiting for a resource
//	var err error
//	for i := 0; i < 3; i++ {
//		resources[i] = get(t, p)
//	}
//	// This will wait because pool is empty
//	go func() {
//		r := get(t, p)
//		p.Put(r)
//		done <- true
//	}()
//
//	// This will also wait
//	go func() {
//		p.SetCapacity(2, true)
//		done <- true
//	}()
//	time.Sleep(10 * time.Millisecond)
//
//	// This should not hang
//	for i := 0; i < 3; i++ {
//		p.Put(resources[i])
//	}
//	<-done
//	<-done
//	time.Sleep(time.Millisecond)
//
//	if p.Capacity() != 2 {
//		t.Errorf("Expecting 2, received %d", p.Capacity())
//	}
//	if p.Available() != 2 {
//		t.Errorf("Expecting 2, received %d", p.Available())
//	}
//	if p.WaitCount() != 1 {
//		t.Errorf("Expecting 1, received %d", p.WaitCount())
//		return
//	}
//	if count.Get() != 2 {
//		t.Errorf("Expecting 2, received %d", count.Get())
//	}
//
//	// TestSuper race condition of SetCapacity with itself
//	p.SetCapacity(3, true)
//	for i := 0; i < 3; i++ {
//		resources[i], err = p.Get(ctx)
//		if err != nil {
//			t.Errorf("Unexpected error %v", err)
//		}
//	}
//	// This will wait because pool is empty
//	go func() {
//		r, err := p.Get(ctx)
//		if err != nil {
//			t.Errorf("Unexpected error %v", err)
//		}
//		p.Put(r)
//		done <- true
//	}()
//	time.Sleep(10 * time.Millisecond)
//
//	// This will wait till we Put
//	go p.SetCapacity(2, true)
//	time.Sleep(10 * time.Millisecond)
//	go p.SetCapacity(4, true)
//	time.Sleep(10 * time.Millisecond)
//
//	// This should not hang
//	for i := 0; i < 3; i++ {
//		p.Put(resources[i])
//	}
//	<-done
//
//	err = p.SetCapacity(-1, true)
//	if err == nil {
//		t.Errorf("Expecting error")
//	}
//	err = p.SetCapacity(255555, true)
//	time.Sleep(time.Millisecond)
//	if err == nil {
//		t.Errorf("Expecting error")
//	}
//
//	if p.Capacity() != 4 {
//		t.Errorf("Expecting 4, received %d", p.Capacity())
//	}
//	if p.Available() != 4 {
//		t.Errorf("Expecting 4, received %d", p.Available())
//	}
//}

func TestSuperClosing(t *testing.T) {
	ctx := context.Background()
	lastID.Set(0)
	count.Set(0)
	p := NewSuperPool(PoolFactory, 5, 5, time.Second, 0)
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
	p := NewSuperPool(SlowCloseFactory, 5, 5, time.Second, 0)
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
	p := NewSuperPool(PoolFactory, 1, 1, 10*time.Millisecond, 0)

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

	fmt.Println("OK????")

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
	p := NewSuperPool(FailFactory, 5, 5, time.Second, 0)

	_, err := p.Get(ctx)
	require.Error(t, err)
	if err.Error() != "Failed" {
		t.Errorf("Expecting Failed, received %v", err)
	}
	time.Sleep(time.Millisecond)
	require.Equal(t, State{Capacity: 5, IdleTimeout: time.Second}, p.State())
	require.Equal(t, 5, p.Available())
	require.Equal(t, 0, p.Active())

	p.Close()
}

func TestSuperSlowCreateFail(t *testing.T) {
	lastID.Set(0)
	count.Set(0)
	p := NewSuperPool(SlowFailFactory, 2, 2, time.Second, 0)

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
	p := NewSuperPool(SlowCreateFactory, 4, 4, time.Second, 0)

	var resources []Resource
	for i := 0; i < 4; i++ {
		resources = append(resources, get(t, p))
	}

	fmt.Println("------------")
	fmt.Println("------------")
	fmt.Println("------------")
	fmt.Println("------------")
	fmt.Println("------------")

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
	p := NewSuperPool(PoolFactory, 1, 1, time.Second, 0)

	b := get(t, p)
	expected := State{Capacity: 1, InUse: 1, IdleTimeout: time.Second}
	require.Equal(t, expected, p.State())

	newctx, cancel := context.WithTimeout(ctx, time.Millisecond*50)
	a, err := p.Get(newctx)
	cancel()
	require.EqualError(t, err, "resource pool timed out")

	time.Sleep(time.Millisecond * 10)
	require.Equal(t, expected, p.State())

	p.Put(b)
	expected.InUse = 0
	expected.InPool = 1
	require.Equal(t, expected, p.State())

	p.Put(a)

	p.Close()
}

func TestSuperExpired(t *testing.T) {
	lastID.Set(0)
	count.Set(0)
	p := NewSuperPool(PoolFactory, 1, 1, time.Second, 0)
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
	p := NewSuperPool(PoolFactory, 5, 5, time.Second, 3)
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
	p := NewSuperPool(PoolFactory, 5, 5, timeout, 3)
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
	p := NewSuperPool(PoolFactory, 5, 5, time.Second, 3)

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

	NewSuperPool(FailFactory, 1, 1, time.Second, 2)
}

func TestSuperTooManyGetsAtOnce(t *testing.T) {
	panic("TODO")
}

func TestSuperCloseIdleResourcesInPoolChangingRaceAttempt(t *testing.T) {
	p := NewSuperPool(PoolFactory, 100, 100, time.Millisecond, 0)

	done := make(chan bool)
	go func() {
		var rs []Resource
		for total := 0; total < 5000; total += 10 {
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

	t.Log(p.StatsJSON())
	require.NotZero(t, p.State().IdleClosed)

	p.Close()
}

func TestSuperMinActiveTooHighAfterSetCapacity(t *testing.T) {
	p := NewSuperPool(FailFactory, 3, 3, time.Second, 2)

	if err := p.SetCapacity(2, true); err != nil {
		t.Errorf("Expecting no error, instead got: %v", err)
	}

	err := p.SetCapacity(1, true)
	expecting := "minActive 2 would now be higher than capacity 1"
	if err == nil || err.Error() != expecting {
		t.Errorf("Expecting: %v, instead got: %v", expecting, err)
	}

	p.Close()
}

func TestSuperGetPutRace(t *testing.T) {
	p := NewSuperPool(SlowCreateFactory, 1, 1, 10*time.Nanosecond, 0)

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

func TestSuperPanicStringCreateFactory(t *testing.T) {
	p := NewSuperPool(PanicStringCreateFactory, 10, 10, 0, 0)
	require.Panics(t, func() {
		_, _ = p.Get(context.Background())
	})
	p.Close()
}

func TestSuperPanicErrorCreateFactory(t *testing.T) {
	p := NewSuperPool(PanicErrorCreateFactory, 10, 10, 0, 0)
	require.Panics(t, func() {
		_, _ = p.Get(context.Background())
	})
	p.Close()
}

func TestSuperPanicStringCloseFactory(t *testing.T) {
	p := NewSuperPool(PanicStringCloseFactory, 10, 10, 0, 0)
	r := get(t, p)
	p.Put(r)
	require.Panics(t, func() {
		p.Close()
	})
}

func TestSuperPanicErrorCloseFactory(t *testing.T) {
	p := NewSuperPool(PanicErrorCloseFactory, 10, 10, 0, 0)
	r := get(t, p)
	p.Put(r)
	require.Panics(t, func() {
		p.Close()
	})
}
