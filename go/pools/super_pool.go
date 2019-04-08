package pools

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

var _ Pool = &SuperPool{}

type SuperPool struct {
	factory      CreateFactory
	pool         chan resourceWrapper
	cmd          chan command
	open         chan chan resourceWrapper
	newResources chan newResource

	state atomic.Value
}

type newResource struct {
	requester chan resourceWrapper
	resource  resourceWrapper
}

type returnedResource struct {
	callback chan error
	resource Resource
}

type command interface{}
type getCommand *chan resourceWrapper
type putCommand returnedResource
type closeCommand bool

func NewSuperPool(factory CreateFactory, capacity, maxCap int, idleTimeout time.Duration, minActive int) *SuperPool {
	p := &SuperPool{
		factory:      factory,
		pool:         make(chan resourceWrapper, maxCap),
		cmd:          make(chan command),
		open:         make(chan chan resourceWrapper),
		newResources: make(chan newResource),
	}

	state := State{
		Capacity:  capacity,
		MinActive: minActive,
	}
	p.state.Store(state)

	go p.main(state)
	go p.opener()
	//go p.closer()

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
			case putCommand:
				if state.InUse <= 0 {

					cmd.callback <- ErrPutBeforeGet
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
			case closeCommand:
				fmt.Println("got close command TODO")
			default:
				panic("unknown command")
			}

		case info := <-p.newResources:
			fmt.Println("a new resource was created for a caller")
			state.InUse++
			info.requester <- info.resource

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
			p.newResources <- newResource{
				requester: requester,
				resource:  resourceWrapper{resource: r},
			}
			fmt.Println("Creating resource returned")
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
	p.cmd <- putCommand(returnedResource{
		callback: ret,
		resource: r,
	})
	err := <-ret
	if err == ErrFull || err == ErrPutBeforeGet {
		panic(err)
	}
}

func (p *SuperPool) SetCapacity(int, bool) error {
	panic("implement me")
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
