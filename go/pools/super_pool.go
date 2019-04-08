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
	open chan chan resourceWrapper

	// Closer
	close chan closeRequest

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
	size int
	wait chan bool
}

type didCloseCommand struct {
	createType createType
}

type finishCommand bool

// Internal channel messages

type closeRequest struct {
	wrapper    resourceWrapper
	createType createType
}

// NewSuperPool
func NewSuperPool(factory CreateFactory, capacity, maxCap int, idleTimeout time.Duration, minActive int) *SuperPool {
	p := &SuperPool{
		factory:  factory,
		cmd:      make(chan command),
		open:     make(chan chan resourceWrapper),
		close:    make(chan closeRequest),
		finished: make(chan bool),
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

	flush := func() {
		p.state.Store(state)
	}

	var newCapCallback chan bool
	var toDrain = 0
	for {
		fmt.Println("------------------------------------")
		fmt.Printf("%+v\n", state)
		fmt.Println("------------------------------------")
		flush()

		select {
		case cmd, ok := <-p.cmd:
			if !ok {
				return
			}
			fmt.Println("CMD:", reflect.TypeOf(cmd))
			switch cmd := cmd.(type) {
			case getCommand:
				if len(p.pool) > 0 {
					var r resourceWrapper
					r, p.pool = p.pool[0], p.pool[1:]
					*cmd <- r
				} else {
					p.open <- *cmd
				}

			case newResourceCommand:
				state.InUse++
				cmd.requester <- cmd.resource

			case putCommand:
				wrapper := resourceWrapper{resource: cmd.resource}

				if state.InUse <= 0 {
					cmd.callback <- vterrors.Wrap(ErrPutBeforeGet, "")
					break
				}

				if state.Draining {
					if toDrain > 0 {
						toDrain--
						if cmd.resource != nil {
							p.close <- closeRequest{
								createType: forUse,
								wrapper: wrapper,
							}
						}
						cmd.callback <- nil
						break
					}
				}

				if state.InUse+state.InPool > state.Capacity {
					cmd.callback <- vterrors.Wrap(ErrFull, "")
					break
				}

				if cmd.resource != nil {
					p.pool = append(p.pool, wrapper)
					state.InPool++
					state.InUse--
				} else {
					state.InUse--
				}
				cmd.callback <- nil

			case setCapacityCommand:
				fmt.Println("wtf!")

				// TODO(gak): If anyone has been waiting for an existing setCapacity, we tell them it's done
				//  immediately because it'll get hairy to track multiple setCapacity's for now.
				if newCapCallback != nil {
					newCapCallback <- true
					newCapCallback = nil
				}

				oldCap := state.Capacity
				newCap := cmd.size
				fmt.Println("setcap!")

				if newCap < oldCap {
					fmt.Println("draining")
					newCapCallback = cmd.wait
					var drain []resourceWrapper
					state.Draining = true
					toDrain = oldCap - newCap
					fmt.Println("draining toDrain", toDrain)
					if state.InPool < toDrain {
						fmt.Println("not enough in pool")
						drain, p.pool = p.pool, []resourceWrapper{}
						toDrain -= state.InPool
					} else {
						fmt.Println("enough in pool")
						drain, p.pool = p.pool[:toDrain], p.pool[toDrain:]
					}
					for _, r := range drain {
						p.close <- closeRequest{
							createType: forPool,
							wrapper: r,
						}
					}
				} else {
					cmd.wait <- true
				}
				state.Capacity = cmd.size

			case finishCommand:
				fmt.Println("got close command TODO")

			case didCloseCommand:
				switch cmd.createType {
				case forPool:
					state.InPool--
				case forUse:
					state.InUse--
				}

				fmt.Println("edrain", state.Draining)
				fmt.Println("cap", state.Capacity)
				fmt.Println("active", state.InPool + state.InUse)
				if state.Draining && state.Capacity >= state.InPool+state.InUse {
					fmt.Println("wut")
					state.Draining = false
					if newCapCallback != nil {
						newCapCallback <- true
						//newCapCallback = nil
					}
					//newCapCallback = nil
					fmt.Println("wut......")
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
		case requester, ok := <-p.open:
			if !ok {
				return
			}
			r, _ := p.factory()
			p.cmd <- newResourceCommand{
				requester: requester,
				resource:  resourceWrapper{resource: r},
			}
		}
	}
}

func (p *SuperPool) closer() {
	for {
		select {
		case req, ok := <-p.close:
			if !ok {
				return
			}
			req.wrapper.resource.Close()
			req.wrapper.resource = nil
			p.cmd <- didCloseCommand{createType: req.createType}
		}
	}
}

func (p *SuperPool) Close() {
	//wait := make(chan bool)
	//p.cmd <- setCapacityCommand{size: 0, wait: wait}
	//<-wait
	close(p.open)
	close(p.close)
	close(p.cmd)
	fmt.Println("implement me close")
}

func (p *SuperPool) Get(context.Context) (Resource, error) {
	get := make(chan resourceWrapper)
	p.cmd <- getCommand(&get)
	w := <-get
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
	<-wait
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
