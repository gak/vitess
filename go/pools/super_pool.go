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
	ret      chan resourceWrapper
	resource resourceWrapper
}

//
//type command interface {
//	command()
//}
//
//type getCommand chan resourceWrapper
//func (g getCommand) command()  {}
//
//type putCommand Resource
//func (p putCommand) command() {}
//
//func foo() {
//	var cmd command = make(getCommand)
//	switch cmd := cmd.(type) {
//	case getCommand:
//	case putCommand:
//	}
//}

type command struct {
	get   *chan resourceWrapper
	put   Resource
	close bool
}

func NewSuperPool(factory CreateFactory, capacity, maxCap int, idleTimeout time.Duration, minActive int) *SuperPool {
	p := &SuperPool{
		factory: factory,
		pool:    make(chan resourceWrapper, maxCap),
	}

	state := State{
		Capacity:  capacity,
		MinActive: minActive,
	}
	p.state.Store(state)

	go p.main()
	go p.opener()
	//go p.closer()

	return p
}

func (p *SuperPool) main() {
	idle := time.NewTimer(time.Second)
	for {
		select {
		case info := <-p.newResources:
			info.ret <- info.resource

		case cmd := <-p.cmd:
			if cmd.get != nil {
				ret := *cmd.get
				select {
				case r, _ := <-p.pool:
					ret <- r
				default:
					p.open <- ret
				}
			} else if cmd.put != nil {
				p.pool <- resourceWrapper{resource: cmd.put}
			} else if cmd.close {

			} else {
				panic(fmt.Sprintf("unknown command: %+v", cmd))
			}
		case <-idle.C:
			fmt.Println("timer...")

		}
	}
}

func (p *SuperPool) opener() {
	for {
		select {
		case ret := <-p.open:
			r, _ := p.factory()
			ret <- resourceWrapper{resource: r}
		}
	}
}

func (SuperPool) Close() {
	panic("implement me")
}

func (p *SuperPool) Get(context.Context) (Resource, error) {
	get := make(chan resourceWrapper)
	p.cmd <- command{get: &get}
	w := <-get
	return w.resource, nil
}

func (p *SuperPool) Put(r Resource) {
	p.cmd <- command{put: r}
}

func (SuperPool) SetCapacity(int, bool) error {
	panic("implement me")
}

func (SuperPool) SetIdleTimeout(duration time.Duration) {
	panic("implement me")
}

func (SuperPool) Capacity() int {
	panic("implement me")
}

func (SuperPool) IdleTimeout() time.Duration {
	panic("implement me")
}

func (SuperPool) MaxCap() int {
	panic("implement me")
}

func (SuperPool) MinActive() int {
	panic("implement me")
}

func (SuperPool) Active() int {
	panic("implement me")
}

func (SuperPool) Available() int {
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

func (SuperPool) StatsJSON() string {
	panic("implement me")
}
