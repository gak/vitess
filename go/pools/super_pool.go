package pools

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Pool = &SuperPool{}

type SuperPool struct {
	factory  CreateFactory
	pool     []resourceWrapper
	cmd      chan command
	finished chan bool

	// Opener
	open chan openRequest

	// Closer
	close chan closeRequest

	// The number of routines that need to exit when closing.
	routines int

	state atomic.Value
}

// Commands

type command interface {
	command()
}

type getCommand struct {
	callback chan resourceWrapper
}

func (c getCommand) command() {}

type putCommand struct {
	callback chan error
	resource Resource
}

func (c putCommand) command() {}

type didOpenCommand struct {
	callback  chan resourceWrapper
	resource  resourceWrapper
	createFor createType
}

func (c didOpenCommand) command() {}

type setCapacityCommand struct {
	size int
	wait chan bool
}

func (c setCapacityCommand) command() {}

type didCloseCommand struct{}

func (c didCloseCommand) command() {}

type finishCommand struct{}

func (c finishCommand) command() {}

// Internal channel messages

type openRequest struct {
	callback chan resourceWrapper
	reason   createType
}

type closeRequest struct {
	wrapper resourceWrapper
}

// NewSuperPool
func NewSuperPool(factory CreateFactory, capacity, maxCap int, idleTimeout time.Duration, minActive int) *SuperPool {
	p := &SuperPool{
		factory:  factory,
		cmd:      make(chan command),
		open:     make(chan openRequest, 10),
		close:    make(chan closeRequest, 10),
		finished: make(chan bool),
	}

	state := State{
		Capacity:  capacity,
		MinActive: minActive,
	}
	p.state.Store(state)

	p.routines = 3

	go p.main(state)
	go p.opener()
	go p.closer()

	return p
}

func (p *SuperPool) main(state State) {
	idle := time.NewTimer(time.Second)

	flush := func() {
		p.state.Store(state)
	}
	active := func() int {
		return state.InPool + state.InUse
	}

	var newCapWait chan bool
	for {
		fmt.Println("------------------------------------")
		fmt.Printf("%+v\n", state)
		fmt.Println("------------------------------------")
		flush()

		if len(p.pool) != state.InPool {
			fmt.Println("something is not right", len(p.pool), state.InPool)
			panic("something not right")
		}

		if !state.Draining {
			for i := 0; i < p.MinActive()-active()-state.Spawning; i++ {
				state.Spawning++
				p.open <- openRequest{
					reason: forPool,
				}
			}
		}

		select {
		case <-p.finished:
			return

		case cmd, ok := <-p.cmd:
			if !ok {
				return
			}
			fmt.Println("CMD:", reflect.TypeOf(cmd))
			switch cmd := cmd.(type) {
			case getCommand:
				if 1+active()+state.Spawning > state.Capacity {
					cmd.callback <- resourceWrapper{
						err: ErrFull,
					}
				} else if len(p.pool) > 0 {
					var r resourceWrapper
					r, p.pool = p.pool[0], p.pool[1:]
					state.InPool--
					state.InUse++
					cmd.callback <- r
				} else {
					state.Spawning++
					state.Waiters++
					p.open <- openRequest{
						callback: cmd.callback,
						reason:   forUse,
					}
				}

			case didOpenCommand:
				state.Spawning--
				switch cmd.createFor {
				case forPool:
					state.InPool++
					p.pool = append(p.pool, cmd.resource)
				case forUse:
					state.InUse++
					state.Waiters--
				}
				if cmd.callback != nil {
					cmd.callback <- cmd.resource
				}

			case putCommand:
				wrapper := resourceWrapper{resource: cmd.resource}

				if state.InUse <= 0 {
					cmd.callback <- vterrors.Wrap(ErrPutBeforeGet, "")
					break
				}

				if active() > state.Capacity {
					if !state.Draining {
						cmd.callback <- vterrors.Wrap(ErrFull, "")
						break
					}

					state.InUse--
					if cmd.resource != nil {
						state.Closing++
						p.close <- closeRequest{
							wrapper: wrapper,
						}
					}
					cmd.callback <- nil
					break
				}

				if cmd.resource != nil {
					p.pool = append(p.pool, wrapper)
					state.InPool++
				}
				state.InUse--
				cmd.callback <- nil

			case setCapacityCommand:
				fmt.Println("newSize", cmd.size)

				// TODO(gak): If anyone has been waiting for an existing setCapacity, we tell them it's done
				//  immediately because it'll get hairy to track multiple setCapacity's for now.
				if newCapWait != nil {
					newCapWait <- true
					newCapWait = nil
				}

				state.Capacity = cmd.size
				toDrain := active() - state.Capacity

				if toDrain > 0 {
					var drain []resourceWrapper
					newCapWait = cmd.wait
					state.Draining = true
					if state.InPool < toDrain {
						drain, p.pool = p.pool, []resourceWrapper{}
						toDrain -= state.InPool
					} else {
						drain, p.pool = p.pool[:toDrain], p.pool[toDrain:]
					}
					state.InPool -= len(drain)
					state.Closing += len(drain)
					for _, r := range drain {
						p.close <- closeRequest{
							wrapper: r,
						}
					}
				} else {
					if cmd.wait != nil {
						cmd.wait <- true
					}
				}

			case finishCommand:
				fmt.Println("got close command TODO")

			case didCloseCommand:
				state.Closing--

				if state.Draining && state.Capacity >= active()+state.Closing {
					fmt.Println("Draining finished!")
					state.Draining = false
					if newCapWait != nil {
						newCapWait <- true
						newCapWait = nil
					}
				}

			default:
				panic("unknown command")
			}

		case <-idle.C:
			fmt.Println("timer...")

		}
	}
}

func (p *SuperPool) opener() {
	for {
		select {
		case <-p.finished:
			return

		case requester, ok := <-p.open:
			if !ok {
				return
			}
			r, _ := p.factory()
			p.cmd <- didOpenCommand{
				callback:  requester.callback,
				resource:  resourceWrapper{resource: r},
				createFor: requester.reason,
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

	wait := make(chan bool)
	p.cmd <- setCapacityCommand{size: 0, wait: wait}
	<-wait

	for i := 0; i < p.routines; i++ {
		p.finished <- true
	}

	close(p.cmd)
	close(p.open)
	close(p.close)

	fmt.Println("implement me close")
}

func (p *SuperPool) Get(context.Context) (Resource, error) {
	callback := make(chan resourceWrapper)
	p.cmd <- getCommand{callback: callback}
	w := <-callback
	if w.err != nil {
		return nil, w.err
	}
	return w.resource, nil
}

func (p *SuperPool) Put(r Resource) {
	ret := make(chan error)
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
	var wait chan bool
	if block {
		wait = make(chan bool)
	}
	p.cmd <- setCapacityCommand{
		size: size,
		wait: wait,
	}
	if block {
		<-wait
	}
	return nil
}

func (p *SuperPool) SetIdleTimeout(duration time.Duration) {
	panic("implement me")
}

func (p *SuperPool) Capacity() int {
	panic("implement me")
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
	panic("implement me")
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

func (SuperPool) InUse() int {
	panic("implement me")
}

func (SuperPool) WaitTime() time.Duration {
	panic("implement me")
}

func (SuperPool) WaitCount() int64 {
	panic("implement me")
}

func (SuperPool) IdleClosed() int64 {
	panic("implement me")
}

func (p *SuperPool) State() State {
	return p.state.Load().(State)
}

func (SuperPool) StatsJSON() string {
	panic("implement me")
}
