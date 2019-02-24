/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package pools provides functionality to manage and reuse resources
// like connections.
package pools

import (
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"sync/atomic"
	"time"
)

var (
	// ErrClosed is returned if ResourcePool is used when it's closed.
	ErrClosed = errors.New("resource pool is closed")

	// ErrTimeout is returned if a resource get times out.
	ErrTimeout = errors.New("resource pool timed out")
)

// Factory is a function that can be used to create a resource.
type Factory func() (Resource, error)

// Resource defines the interface that every resource must provide.
// Thread synchronization between Close() and IsClosed()
// is the responsibility of the caller.
type Resource interface {
	Close()
}

// ResourcePool allows you to use a pool of resources.
type ResourcePool struct {
	factory Factory

	// state contains settings, inventory counts, and statistics of the pool.
	state atomic.Value

	// pool contains active resources.
	pool chan resourceWrapper

	// get is a channel to request the availability of a resource.
	get chan bool

	// put is a channel to put back a resource into the pool.
	put chan *resourceWrapper

	// setCapacity is a channel to change the capacity.
	setCapacity chan setCapacityRequest

	// getCapacity is used only to block on a changed capacity.
	getCapacity chan bool

	// setIdleTimeout is a channel to request the timeout to be changed.
	setIdleTimeout chan time.Duration

	// waitTimers tracks when each waiter started to wait.
	waitTimers []time.Time
}

type State struct {
	Waiters     int
	InPool      int
	InUse       int
	Capacity    int
	MinActive   int
	Closed      bool
	IdleTimeout time.Duration

	// IdleClosed tracks the number of resources closed due to being idle.
	IdleClosed int64

	// WaitCount contains the number of times Get() had to block and wait
	// for a resource.
	WaitCount int64

	// WaitCount tracks the total time waiting for a resource.
	WaitTime time.Duration
}

type resourceWrapper struct {
	resource Resource
	timeUsed time.Time
}

// setCapacityRequest is used to send over the setCapacity channel.
type setCapacityRequest struct {
	capacity int
	block    bool
}

// NewResourcePool creates a new ResourcePool pool.
// capacity is the number of possible resources in the pool:
// there can be up to 'capacity' of these at a given time.
// maxCap specifies the extent to which the pool can be resized
// in the future through the SetCapacity function.
// You cannot resize the pool beyond maxCap.
// If a resource is unused beyond idleTimeout, it's discarded.
// An idleTimeout of 0 means that there is no timeout.
//
// minActive is used to prepare and maintain a minimum amount
// of active resources. Any errors when instantiating the factory
// will cause the active resource count to be lower than requested.
func NewResourcePool(factory Factory, capacity, maxCap int, idleTimeout time.Duration, minActive int) *ResourcePool {
	if capacity <= 0 || maxCap <= 0 || capacity > maxCap {
		panic(errors.New("invalid/out of range capacity"))
	}
	if minActive > capacity {
		panic(fmt.Errorf("minActive %v higher than capacity %v", minActive, capacity))
	}

	rp := &ResourcePool{
		factory:        factory,
		pool:           make(chan resourceWrapper, maxCap),
		get:            make(chan bool),
		put:            make(chan *resourceWrapper),
		setCapacity:    make(chan setCapacityRequest),
		getCapacity:    make(chan bool),
		setIdleTimeout: make(chan time.Duration),
	}

	state := State{
		Capacity:    capacity,
		MinActive:   minActive,
		IdleTimeout: idleTimeout,
	}

	rp.storeState(state)
	rp.ensureMinimumActive(&state)
	go rp.run(state)

	return rp
}

func (rp *ResourcePool) run(state State) {
	var idleTick <-chan time.Time
	if state.IdleTimeout != 0 {
		idleTick = time.Tick(state.IdleTimeout / 10)
	}

	for {
		rp.storeState(state)
		rp.ensureMinimumActive(&state)

		select {
		case <-rp.get:
			rp.handleGet(&state)

		case wrapper := <-rp.put:
			rp.handlePut(&state, wrapper)

		case capReq := <-rp.setCapacity:
			rp.handleSetCapacity(&state, capReq)

		case <-idleTick:
			rp.closeIdleResources(&state)

		case idleTimeout := <-rp.setIdleTimeout:
			idleTick = rp.handleSetIdleTimeout(&state, idleTimeout)
		}
	}
}

func (rp *ResourcePool) handleSetIdleTimeout(state *State, idleTimeout time.Duration) <-chan time.Time {
	state.IdleTimeout = idleTimeout
	if state.IdleTimeout == 0 {
		return nil
	} else {
		return time.Tick(state.IdleTimeout / 10)
	}
}

func (rp *ResourcePool) handleSetCapacity(state *State, capReq setCapacityRequest) {
	newCap := capReq.capacity
	oldCap := state.Capacity
	delta := newCap - oldCap
	state.Capacity = newCap
	// Closing a resource is slow, so let's update the state.
	rp.storeState(state)
	fmt.Println("got a setcap command", oldCap, newCap, delta)
	// Shrinking capacity
	if delta < 0 {
		delta = -delta
		fmt.Println("shrinking!", delta, state.InPool)

		// We can't remove InUse resources, so only target the pool.
		// Collect the other resources lazily when they're returned.
		for i := 0; i < delta && state.InPool > 0; i++ {
			r := <-rp.pool
			// TODO: Delegate this for non-blocking access to the pool?
			r.resource.Close()
			state.InPool--
			// Closing a resource is slow, so let's update the state.
			rp.storeState(state)
		}
	}
	if newCap == 0 {
		close(rp.pool)
		state.Closed = true
	}
	if capReq.block {
		rp.getCapacity <- true
	}
}

func (rp *ResourcePool) handleGet(state *State) {
	if state.InPool == 0 {
		if state.InUse < state.Capacity {
			// The pool is empty and we still have pool capacity.
			// Hand over an empty wrapper for the caller to instantiate.
			state.InUse++
			rp.pool <- rp.createEmpty()
		} else {
			// The pool is empty but we don't have enough capacity.
			// The user will be blocked for a resource to be returned to the pool.
			//
			// InUse and InPool do not change here, because there has not
			// been any new resources given, returned, or created.
			rp.waitTimers = append(rp.waitTimers, time.Now())
			state.Waiters++
		}
	} else if state.InPool != 0 {
		state.InPool--
		state.InUse++
	}
}

func (rp *ResourcePool) handlePut(state *State, wrapper *resourceWrapper) {
	// Allow resources to be closed while pool is declared closed.
	// OR
	// Don't place resources back into the pull when we've reached
	// capacity. This can happen after SetCapacity call.
	if state.Closed || state.InPool == state.Capacity {
		if wrapper != nil {
			wrapper.resource.Close()
		}
		state.InUse--
		return
	}

	if state.Waiters == 0 {
		if wrapper != nil {
			state.InPool++
		}
		state.InUse--
		if wrapper != nil {
			rp.pool <- *wrapper
		}
		return
	}

	// There is a queue of waiters.
	//
	// When a resource is returned while waiting, we don't need to
	// modify InPool or InUse because 1) we are not returning it to the
	// pool, and 2) there is no change in the number of uses of resources.
	// Basically we are just handing over the resource to another owner.
	state.Waiters--

	// Track how many times and long each wait took.
	var start time.Time
	start, rp.waitTimers = rp.waitTimers[0], rp.waitTimers[1:]
	state.WaitCount++
	state.WaitTime += time.Now().Sub(start)

	if wrapper != nil {
		rp.pool <- *wrapper
	} else {
		// We have a waiter, but the returned resource was nil. We need to create one.
		rp.pool <- rp.createEmpty()
	}
}

func (rp *ResourcePool) storeState(state State) {
	rp.state.Store(state)
}

func (rp *ResourcePool) create() (resourceWrapper, error) {
	r, err := rp.factory()
	if err != nil {
		return resourceWrapper{}, err
	}

	return resourceWrapper{
		resource: r,
		timeUsed: time.Now(),
	}, nil
}

func (rp *ResourcePool) createEmpty() resourceWrapper {
	return resourceWrapper{}
}

func (rp *ResourcePool) ensureMinimumActive(state *State) {
	if state.MinActive == 0 || state.Closed {
		return
	}

	required := state.MinActive - rp.Active()
	fmt.Println("required", required)
	for i := 0; i < required; i++ {
		// TODO(gak): This might be better as an empty, so it doesn't
		// block the main goroutine.
		r, err := rp.create()
		if err != nil {
			fmt.Println("error creating factory", err)
			// TODO(gak): How to handle factory error?
			break
		}
		rp.pool <- r
		state.InPool++
		fmt.Println("added minactive to pool")
	}
	rp.storeState(*state)
}

// closeIdleResources scans the pool for idle resources
func (rp *ResourcePool) closeIdleResources(state *State) {
	// Shouldn't be zero, but checking in case.
	if state.IdleTimeout == 0 {
		return
	}

	for i := 0; i < state.InPool; i++ {
		wrapper := <-rp.pool

		if wrapper.timeUsed.Add(state.IdleTimeout).Sub(time.Now()) < 0 {
			fmt.Println("closing resource due to timeout")
			wrapper.resource.Close()
			wrapper.resource = nil
			state.IdleClosed++
			state.InPool--
			continue
		}

		// Not expired--back into the pool we go.
		rp.pool <- wrapper
	}
}

// Close empties the pool calling Close on all its resources.
// You can call Close while there are outstanding resources.
// It waits for all resources to be returned (Put).
// After a Close, Get is not allowed.
func (rp *ResourcePool) Close() {
	_ = rp.SetCapacity(0, true)
}

// IsClosed returns true if the resource pool is closed.
func (rp *ResourcePool) IsClosed() bool {
	return rp.State().Closed
}

// Get will return the next available resource. If capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will wait till the next resource becomes available or a timeout.
// A timeout of 0 is an indefinite wait.
func (rp *ResourcePool) Get(ctx context.Context) (resource Resource, err error) {
	// Check for context timeout
	select {
	case <-ctx.Done():
		return nil, ErrTimeout
	default:
	}

	// Inform the pool that we want a resource.
	rp.get <- true

	select {
	case wrapper, ok := <-rp.pool:
		if !ok {
			return nil, ErrClosed
		}

		if wrapper.resource == nil {
			wrapper, err = rp.create()
			if err != nil {
				// Put back an available empty resource.
				rp.Put(nil)
				return nil, err
			}
		}
		return wrapper.resource, nil
	case <-ctx.Done():
		return nil, ErrTimeout
	}
}

// Put will return a resource to the pool. For every successful Get,
// a corresponding Put is required. If you no longer need a resource,
// you will need to call Put(nil) instead of returning the closed resource.
// The will eventually cause a new resource to be created in its place.
func (rp *ResourcePool) Put(resource Resource) {
	fmt.Println("\n\nPUT", resource, rp.StatsJSON())

	if resource == nil {
		fmt.Println("putting nil")
		rp.put <- nil
		fmt.Println("putting nil done")
	} else {
		fmt.Println("putting resource back")
		rp.put <- &resourceWrapper{
			resource: resource,
			timeUsed: time.Now(),
		}
		fmt.Println("resource returned")

		//select {
		//case rp.resources <- wrapper:
		//default:
		//	panic(errors.New("attempt to Put into a full ResourcePool (2)"))
		//}

	}
}

// SetCapacity changes the capacity of the pool.
// You can use it to shrink or expand, but not beyond
// the max capacity. If the change requires the pool
// to be shrunk, SetCapacity waits till the necessary
// number of resources are returned to the pool.
// A SetCapacity of 0 is equivalent to closing the ResourcePool.
func (rp *ResourcePool) SetCapacity(capacity int, block bool) error {
	fmt.Println("\n\nsetCapacity", capacity, rp.StatsJSON())
	if rp.State().Closed {
		return fmt.Errorf("pool is closed")
	}

	if capacity < 0 || capacity > cap(rp.pool) {
		return fmt.Errorf("capacity %d is out of range", capacity)
	}

	if capacity != 0 {
		minActive := rp.State().MinActive
		if capacity < minActive {
			return fmt.Errorf("minActive %v would now be higher than capacity %v", minActive, capacity)
		}
	}

	rp.setCapacity <- setCapacityRequest{
		capacity: capacity,
		block:    block,
	}

	if block {
		<-rp.getCapacity
	}

	return nil
}

// SetIdleTimeout sets the idle timeout for resources. The timeout is
// checked at the 10th of the period of the timeout.
func (rp *ResourcePool) SetIdleTimeout(idleTimeout time.Duration) {
	rp.setIdleTimeout <- idleTimeout
}

// StatsJSON returns the stats in JSON format.
func (rp *ResourcePool) StatsJSON() string {
	return fmt.Sprintf(`{"Capacity": %v, "Available": %v, "Active": %v, "InUse": %v, "MaxCapacity": %v, "WaitCount": %v, "WaitTime": %v, "IdleTimeout": %v, "IdleClosed": %v}`,
		rp.Capacity(),
		rp.Available(),
		rp.Active(),
		rp.InUse(),
		rp.MaxCap(),
		rp.WaitCount(),
		rp.WaitTime().Nanoseconds(),
		rp.IdleTimeout().Nanoseconds(),
		rp.IdleClosed(),
	)
}

func (rp *ResourcePool) State() State {
	return rp.state.Load().(State)
}

// Capacity returns the capacity.
func (rp *ResourcePool) Capacity() int {
	return rp.State().Capacity
}

// Available returns the number of currently unused and available resources.
func (rp *ResourcePool) Available() int {
	s := rp.State()
	available := s.Capacity - s.InUse
	// Sometimes we can be over capacity temporarily when the capacity shrinks.
	if available < 0 {
		return 0
	}
	return available
}

// Active returns the number of active (i.e. non-nil) resources either in the
// pool or claimed for use
func (rp *ResourcePool) Active() int {
	s := rp.State()
	return s.InUse + s.InPool
}

// MinActive returns the minimum amount of resources keep active.
func (rp *ResourcePool) MinActive() int {
	return rp.State().MinActive
}

// InUse returns the number of claimed resources from the pool
func (rp *ResourcePool) InUse() int {
	return rp.State().InUse
}

// MaxCap returns the max capacity.
func (rp *ResourcePool) MaxCap() int {
	return cap(rp.pool)
}

// WaitCount returns the total number of waits.
func (rp *ResourcePool) WaitCount() int64 {
	return rp.State().WaitCount
}

// WaitTime returns the total wait time.
func (rp *ResourcePool) WaitTime() time.Duration {
	return rp.State().WaitTime
}

// IdleTimeout returns the idle timeout.
func (rp *ResourcePool) IdleTimeout() time.Duration {
	return rp.State().IdleTimeout
}

// IdleClosed returns the count of resources closed due to idle timeout.
func (rp *ResourcePool) IdleClosed() int64 {
	return rp.State().IdleClosed
}
