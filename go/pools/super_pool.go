package pools

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Pool = &SuperPool{}

type SuperPool struct {
	factory CreateFactory
	pool    []resourceWrapper
	cmd     chan command

	// worker goroutines to close
	finished chan bool

	// final finisher for the main goroutine
	final chan bool

	// Opener
	open chan openRequest

	// Closer
	close chan closeRequest

	// The number of workers that need to exit when closing.
	workers int

	newCapWait chan error

	idleTimer *time.Ticker

	state atomic.Value
}

// Commands

type command interface {
	execute(p *SuperPool, state *State)
}

// NewSuperPool
func NewSuperPool(factory CreateFactory, capacity, maxCap int, idleTimeout time.Duration, minActive int) *SuperPool {
	if capacity <= 0 || maxCap <= 0 || capacity > maxCap {
		panic("invalid/out of range capacity")
	}
	if minActive > capacity {
		panic(fmt.Errorf("minActive %v higher than capacity %v", minActive, capacity))
	}

	numOpeners := 1
	numClosers := 1

	p := &SuperPool{
		factory:  factory,
		cmd:      make(chan command),
		open:     make(chan openRequest, numOpeners),
		close:    make(chan closeRequest, capacity),
		finished: make(chan bool),
		final:    make(chan bool),
	}

	state := State{
		Capacity:    capacity,
		MinActive:   minActive,
		IdleTimeout: idleTimeout,
	}
	p.state.Store(state)

	p.workers = numOpeners + numClosers

	go p.main(state)

	for i := 0; i < numOpeners; i++ {
		go p.opener()
	}

	for i := 0; i < numClosers; i++ {
		go p.closer()
	}

	return p
}

func (p *SuperPool) updateIdleTimer(state *State) {
	if state.IdleTimeout > 0 {
		p.idleTimer = time.NewTicker(state.IdleTimeout / 10)
	} else {
		p.idleTimer = time.NewTicker(100 * time.Millisecond)
	}
}

func (p *SuperPool) main(state State) {
	p.updateIdleTimer(&state)

	for {
		fmt.Println("------------------------------------")
		fmt.Printf("%+v\n", state)
		fmt.Println("------------------------------------")
		p.state.Store(state)

		if len(p.pool) != state.InPool {
			fmt.Println("something is not right", len(p.pool), state.InPool)
			panic("something not right")
		}

		if !state.Draining {
			for i := 0; i < p.MinActive()-state.InPool-state.InUse-state.Spawning; i++ {
				state.Spawning++
				fmt.Println("min active create")
				p.open <- openRequest{
					reason: forPool,
				}
			}
		}

		select {
		case <-p.final:
			return

		case cmd, ok := <-p.cmd:
			if !ok {
				return
			}

			fmt.Println("CMD:", reflect.TypeOf(cmd))
			cmd.execute(p, &state)

		case <-p.idleTimer.C:
			idx := 0
			for _, w := range p.pool {
				deadline := w.timeUsed.Add(state.IdleTimeout)
				if time.Now().Before(deadline) {
					p.pool[idx] = w
					idx++
					continue
				}

				state.InPool--
				state.Closing++
				state.IdleClosed++
				p.close <- closeRequest{
					wrapper: w,
				}
			}

			p.pool = p.pool[:idx]
		}
	}
}

type getCommand struct {
	callback chan resourceWrapper
	ctx      context.Context
}

func (cmd getCommand) execute(p *SuperPool, state *State) {
	if len(p.pool) > 0 {
		var r resourceWrapper
		r, p.pool = p.pool[0], p.pool[1:]
		state.InPool--
		state.InUse++
		fmt.Println("Already in pool, serving...")
		cmd.callback <- r
	} else if state.InPool >= state.Capacity {
		cmd.callback <- resourceWrapper{
			err: vterrors.Wrap(ErrFull, ""),
		}
	} else {
		fmt.Println("Spawning...")
		state.Spawning++
		state.Waiters++
		select {
		case p.open <- openRequest{
			reason:   forUse,
			callback: cmd.callback,
			ctx:      cmd.ctx,
		}:
			fmt.Println("Spawning... sent!")
		default:
			state.Spawning--
			state.Waiters--
			fmt.Println("Spawning... default!")
			cmd.callback <- resourceWrapper{
				err: vterrors.Wrap(ErrOpenBufferFull, ""),
			}
		}
	}
}

type putCommand struct {
	callback chan error
	resource Resource
}

func (cmd putCommand) execute(p *SuperPool, state *State) {
	wrapper := resourceWrapper{
		resource: cmd.resource,
		timeUsed: time.Now(),
	}

	if state.InUse <= 0 {
		cmd.callback <- vterrors.Wrap(ErrPutBeforeGet, "")
		return
	}

	if state.InPool+state.InUse > state.Capacity {
		if !state.Draining {
			cmd.callback <- vterrors.Wrap(ErrFull, "")
			return
		}

		state.InUse--
		if cmd.resource != nil {
			state.Closing++
			p.close <- closeRequest{
				wrapper: wrapper,
			}
		}
		cmd.callback <- nil
		return
	}

	if cmd.resource != nil {
		p.pool = append(p.pool, wrapper)
		state.InPool++
	}
	state.InUse--
	cmd.callback <- nil
}

type setCapacityCommand struct {
	size int
	wait chan error
}

func (cmd setCapacityCommand) execute(p *SuperPool, state *State) {
	// TODO(gak): If anyone has been waiting for an existing setCapacity, we tell them it's done
	//  immediately because it'll get hairy to track multiple setCapacity's for now.
	if p.newCapWait != nil {
		p.newCapWait <- nil
		p.newCapWait = nil
	}

	if state.Closed {
		if cmd.wait != nil {
			cmd.wait <- vterrors.Wrap(ErrClosed, "")
		}
		return
	}

	state.Capacity = cmd.size
	toDrain := state.InPool + state.InUse - state.Capacity

	if toDrain > 0 {
		var drain []resourceWrapper
		p.newCapWait = cmd.wait
		state.Draining = true
		if state.InPool < toDrain {
			drain, p.pool = p.pool, []resourceWrapper{}
			toDrain -= state.InPool
			if state.Capacity == 0 {
				state.Closed = true
			}
		} else {
			drain, p.pool = p.pool[:toDrain], p.pool[toDrain:]
		}
		state.InPool -= len(drain)
		state.Closing += len(drain)
		for _, w := range drain {
			p.close <- closeRequest{
				wrapper: w,
			}
		}
	} else {
		if cmd.wait != nil {
			cmd.wait <- nil
		}
	}
}

type setIdleTimeoutCommand struct {
	timeout time.Duration
}

func (cmd setIdleTimeoutCommand) execute(p *SuperPool, state *State) {
	state.IdleTimeout = cmd.timeout
	p.updateIdleTimer(state)
}

// Internal channel messages

type openRequest struct {
	callback chan resourceWrapper
	reason   createType
	ctx      context.Context
}

type didOpenCommand struct {
	request  openRequest
	resource resourceWrapper
}

func (cmd didOpenCommand) execute(p *SuperPool, state *State) {
	state.Spawning--
	if cmd.resource.err != nil {
		if cmd.request.reason == forUse {
			state.Waiters--
		}
		if cmd.request.callback != nil {
			cmd.request.callback <- cmd.resource
		}
		return
	}

	switch cmd.request.reason {
	case forPool:
		state.InPool++
		p.pool = append(p.pool, cmd.resource)
	case forUse:
		state.InUse++
		state.Waiters--
	}
	if cmd.request.callback != nil {
		cmd.request.callback <- cmd.resource
	}
}

type closeRequest struct {
	wrapper resourceWrapper
}

type didCloseCommand struct{}

func (cmd didCloseCommand) execute(p *SuperPool, state *State) {
	state.Closing--

	if state.Draining && state.Capacity >= state.InPool+state.InUse+state.Closing {
		fmt.Println("Draining finished!")
		state.Draining = false
		if state.Capacity == 0 {
			state.Closed = true
		}
		if p.newCapWait != nil {
			p.newCapWait <- nil
			p.newCapWait = nil
		}
	}
}

// Used for testing

type testingOnlyReplaceState struct {
	state State
}

func (cmd testingOnlyReplaceState) execute(p *SuperPool, state *State) {
	*state = cmd.state
}

func (p *SuperPool) opener() {
	for {
		select {
		case <-p.finished:
			return

		case requester, ok := <-p.open:
			fmt.Println("opener called")
			if !ok {
				return
			}

			var r Resource
			var err error
			aborted := false;
			done := make(chan bool)
			doc := didOpenCommand{
				request: requester,
			}
			go func() {
				r, err = p.factory()
				done <- true
			}()

			maybeCtx := requester.ctx
			var cancelChan <-chan struct{}
			if maybeCtx != nil {
				cancelChan = maybeCtx.Done()
			}

		WaitForResource:
			select {
			case <-done:
				if aborted {
					break
				}

				if err != nil {
					doc.resource = resourceWrapper{
						err: err,
					}
				} else {
					doc.resource = resourceWrapper{
						resource: r,
						timeUsed: time.Now(),
					}
				}
				p.cmd <- doc

			case <-cancelChan:
				if aborted {
					// Caller has cancelled again...
					goto WaitForResource
				}
				aborted = true
				doc.resource = resourceWrapper{
					err: ErrTimeout,
				}
				p.cmd <- doc

				goto WaitForResource
			}
		}
	}
}

func (p *SuperPool) closer() {
	for {
		select {
		case <-p.finished:
			return

		case req, ok := <-p.close:
			if !ok {
				return
			}
			req.wrapper.resource.Close()
			req.wrapper.resource = nil
			p.cmd <- didCloseCommand{}
		}
	}
}

func (p *SuperPool) Close() {
	fmt.Println("Closing!")

	err := p.SetCapacity(0, true)
	if err != nil {
		panic(err)
	}

	for i := 0; i < p.workers; i++ {
		p.finished <- true
	}
	p.final <- true

	close(p.open)
	close(p.close)
	close(p.cmd)

	fmt.Println("Fully Closed!")
}

func (p *SuperPool) Get(ctx context.Context) (Resource, error) {
	callback := make(chan resourceWrapper, 1)
	p.cmd <- getCommand{
		callback: callback,
		ctx:      ctx,
	}
	select {
	case w := <-callback:
		if w.err != nil {
			return nil, w.err
		}
		return w.resource, nil
		//
		//case <-ctx.Done():
		//	return nil, ErrTimeout
	}
}

func (p *SuperPool) Put(r Resource) {
	ret := make(chan error, 1)
	p.cmd <- putCommand{
		callback: ret,
		resource: r,
	}
	err := <-ret
	if err != nil {
		fmt.Printf("error: %+v\n", err)
		root := vterrors.RootCause(err)
		if root == ErrFull || root == ErrPutBeforeGet {
			panic(err)
		}
	}
}

func (p *SuperPool) SetCapacity(size int, block bool) error {
	if p.State().Closed {
		return vterrors.Wrap(ErrClosed, "")
	}

	var wait chan error
	if block {
		wait = make(chan error, 1)
	}
	p.cmd <- setCapacityCommand{
		size: size,
		wait: wait,
	}
	if block {
		return <-wait
	}
	return nil
}

func (p *SuperPool) SetIdleTimeout(duration time.Duration) {
	p.cmd <- setIdleTimeoutCommand{timeout: duration}
}

func (p *SuperPool) Capacity() int {
	return p.State().Capacity
}

func (p *SuperPool) IdleTimeout() time.Duration {
	panic("implement me")
}

func (p *SuperPool) MaxCap() int {
	panic("implement me")
}

func (p *SuperPool) MinActive() int {
	return p.State().MinActive
}

func (p *SuperPool) Active() int {
	return p.State().InUse + p.State().InPool
}

func (p *SuperPool) Available() int {
	s := p.State()
	available := s.Capacity - s.InUse
	// Sometimes we can be over capacity temporarily while the capacity shrinks.
	if available < 0 {
		return 0
	}
	return available
}

func (p *SuperPool) InUse() int {
	return p.State().InUse
}

func (p *SuperPool) WaitTime() time.Duration {
	return p.State().WaitTime
}

func (p *SuperPool) WaitCount() int64 {
	return p.State().WaitCount
}

func (p *SuperPool) IdleClosed() int64 {
	return p.State().IdleClosed
}

func (p *SuperPool) State() State {
	return p.state.Load().(State)
}

func (p *SuperPool) StatsJSON() string {
	state := p.State()
	d, err := json.Marshal(&state)
	if err != nil {
		return ""
	}
	return string(d)
}
