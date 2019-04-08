package pools

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

var _ Pool = &SuperPool{}

type SuperPool struct {
	factory CreateFactory
	pool    chan resourceWrapper
	cmd     chan command

	// Opener
	open chan chan resourceWrapper

	// Closer
	close chan bool

	state atomic.Value
}

// Commands

type command interface{}

type getCommand *chan resourceWrapper

type putCommand struct {
	callback chan error
	resource Resource
}

type newResourceCommand struct {
	requester chan resourceWrapper
	resource  resourceWrapper
}

type setCapacityCommand struct {
	size  int
	block bool
}

type didCloseCommand bool

type closeCommand bool

// Internal channel messages

// NewSuperPool
func NewSuperPool(factory CreateFactory, capacity, maxCap int, idleTimeout time.Duration, minActive int) *SuperPool {
	p := &SuperPool{
		factory: factory,
		pool:    make(chan resourceWrapper, maxCap),
		cmd:     make(chan command),
		open:    make(chan chan resourceWrapper),
	}

	state := State{
		Capacity:  capacity,
		MinActive: minActive,
	}
	p.state.Store(state)

	go p.main(state)
	go p.opener()
	go p.closer()

	return p
}

func (p *SuperPool) main(state State) {
	idle := time.NewTimer(time.Second)
	for {
		fmt.Println("main: store")
		p.state.Store(state)

		fmt.Println("main: select")
		select {
		case cmd := <-p.cmd:
			fmt.Println("got a command!", cmd)
			switch cmd := cmd.(type) {
			case getCommand:
				select {
				case r, _ := <-p.pool:
					fmt.Println("Got an existing resource")
					*cmd <- r
				default:
					fmt.Println("No free resources. Asking to open.")
					p.open <- *cmd
				}

			case newResourceCommand:
				fmt.Println("a new resource was created for a caller")
				state.InUse++
				cmd.requester <- cmd.resource

			case putCommand:
				fmt.Println("putCommand")
				if state.InUse <= 0 {
					fmt.Println("err1")
					cmd.callback <- ErrPutBeforeGet
					break
				}

				if state.InUse+state.InPool > state.Capacity {
					fmt.Println("err2")
					cmd.callback <- ErrFull
					break
				}

				fmt.Println("got put command inuse--")
				if cmd.resource != nil {
					fmt.Println("inpool increased")
					select {
					case p.pool <- resourceWrapper{resource: cmd.resource}:
						state.InPool++
						state.InUse--
						cmd.callback <- nil
					default:
						fmt.Println("pool full???")
						cmd.callback <- ErrFull
					}
				} else {
					state.InUse--
					cmd.callback <- nil
				}

			case setCapacityCommand:
				state.Capacity = cmd.size
				if cmd.size < state.Capacity {
					state.Draining = true
					for i := 0; i < state.Capacity-cmd.size; i++ {
						p.close <- true
					}
				}

			case closeCommand:
				fmt.Println("got close command TODO")

			case didCloseCommand:
				state.InPool--
				if state.Draining && state.Capacity >= state.InPool+state.InUse {
					state.Draining = false
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
		case requester := <-p.open:
			r, _ := p.factory()
			fmt.Println("Creating resource")
			p.cmd <- newResourceCommand{
				requester: requester,
				resource:  resourceWrapper{resource: r},
			}
			fmt.Println("Creating resource returned")
		}
	}
}

func (p *SuperPool) closer() {
	for {
		select {
		case <-p.close:
			select {
			case wrapper := <-p.pool:
				fmt.Println("closing...")
				wrapper.resource.Close()
				wrapper.resource = nil
				p.cmd <- didCloseCommand(true)
			default:
				fmt.Println("pool is empty already!")
			}
		}
	}
}

func (p *SuperPool) Close() {
	fmt.Println("implement me close")
}

func (p *SuperPool) Get(context.Context) (Resource, error) {
	get := make(chan resourceWrapper)
	fmt.Println("asking to get!")
	p.cmd <- getCommand(&get)
	fmt.Println("asking to get -- waiting")
	w := <-get
	fmt.Println("asking to get -- got")
	return w.resource, nil
}

func (p *SuperPool) Put(r Resource) {
	fmt.Println("sending putcommand")
	ret := make(chan error)
	p.cmd <- putCommand{
		callback: ret,
		resource: r,
	}
	err := <-ret
	if err == ErrFull || err == ErrPutBeforeGet {
		panic(err)
	}
}

func (p *SuperPool) SetCapacity(size int, block bool) error {
	p.cmd <- setCapacityCommand{
		size:  size,
		block: block,
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
	panic("implement me")
}

func (p *SuperPool) Active() int {
	panic("implement me")
}

func (p *SuperPool) Available() int {
	panic("implement me")
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
