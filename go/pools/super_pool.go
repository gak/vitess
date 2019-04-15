package pools

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Pool = &SuperPool{}

type Opts struct {
	Factory      CreateFactory
	Capacity     int
	MinActive    int
	OpenWorkers  int
	CloseWorkers int
	MaxOpenQueue int
	IdleTimeout  time.Duration
}

type SuperPool struct {
	factory CreateFactory
	started bool
	pool    []resourceWrapper
	queue   []getCmd
	cmd     chan command

	// finishMain used to close the main goroutine.
	finishMain chan bool
	finishOpenWorkers  chan bool
	finishCloseWorkers chan bool

	// Opener
	open chan openReq

	// Closer
	close chan closeReq

	newCapWait chan error

	idleChan <-chan time.Time

	state        atomic.Value
	openWorkers  int
	closeWorkers int
}

// NewSuperPool
func NewSuperPool(opts Opts) *SuperPool {
	if opts.Factory == nil {
		panic("specify a factory")
	}
	if opts.Capacity <= 0 {
		panic("invalid/out of range capacity")
	}
	if opts.MinActive > opts.Capacity {
		panic(fmt.Errorf("minActive %v higher than capacity %v", opts.MinActive, opts.Capacity))
	}
	if opts.OpenWorkers == 0 {
		panic("open workers required to be set")
	}
	if opts.CloseWorkers == 0 {
		panic("close workers required to be set")
	}
	if opts.MaxOpenQueue == 0 {
		opts.MaxOpenQueue = opts.Capacity * 2
	}

	p := &SuperPool{
		factory:            opts.Factory,
		cmd:                make(chan command),
		open:               make(chan openReq, opts.MaxOpenQueue),
		close:              make(chan closeReq, opts.Capacity),
		finishOpenWorkers:  make(chan bool),
		finishCloseWorkers: make(chan bool),
		finishMain:         make(chan bool),
		openWorkers:        opts.OpenWorkers,
		closeWorkers:       opts.CloseWorkers,
	}

	state := State{
		Capacity:    opts.Capacity,
		MinActive:   opts.MinActive,
		IdleTimeout: opts.IdleTimeout,
	}
	p.state.Store(state)

	go p.main(state)

	for i := 0; i < p.openWorkers; i++ {
		go p.opener()
	}
	for i := 0; i < p.closeWorkers; i++ {
		go p.closer()
	}

	return p
}

func (p *SuperPool) updateIdleTimer(state *State) {
	if state.IdleTimeout > 0 {
		p.idleChan = time.NewTicker(state.IdleTimeout / 10).C
	} else {
		p.idleChan = nil
	}
}

func (state *State) activeTransient() int {
	return state.InPool + state.InUse + state.Spawning + state.Closing
}

func (p *SuperPool) main(state State) {
	p.updateIdleTimer(&state)

	for {
		//fmt.Printf("\n--- %+v\n\n", state)
		p.checkForWaiters(&state)
		p.handleCancelled(&state)
		p.createMinActive(&state)
		p.state.Store(state)

		select {
		case <-p.finishMain:
			return

		case cmd := <-p.cmd:
			//fmt.Printf("CMD %s\n", reflect.TypeOf(cmd))
			cmd.execute(p, &state)

		case <-p.idleChan:
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
				p.close <- closeReq{
					wrapper: w,
				}
			}

			p.pool = p.pool[:idx]
		}
	}
}

func (p *SuperPool) checkForWaiters(state *State) {
	for len(p.queue) > 0 && (state.activeTransient() < state.Capacity || state.InPool > 0) {
		var get getCmd
		get, p.queue = p.queue[0], p.queue[1:]
		state.Waiters--
		get.execute(p, state)
	}
}

func (p *SuperPool) createMinActive(state *State) {
	if state.Draining {
		return
	}

	toSpawn := p.MinActive() - state.InPool - state.InUse - state.Spawning
	for i := 0; i < toSpawn; i++ {
		select {
		case p.open <- openReq{reason: forPool}:
			state.Spawning++
		default:
		}
	}
}

func (p *SuperPool) handleCancelled(state *State) {
	if len(p.queue) == 0 {
		return
	}

	idx := 0
	for _, get := range p.queue {
		select {
		case <-get.ctx.Done():
			state.Waiters--
			get.callback <- errResource(ErrTimeout)
		default:
			p.queue[idx] = get
			idx++
		}
	}
	p.queue = p.queue[:idx]
}

type command interface {
	execute(p *SuperPool, state *State)
}

type getCmd struct {
	callback chan resourceWrapper
	ctx      context.Context
	when     time.Time
}

func (cmd getCmd) execute(p *SuperPool, state *State) {
	if len(p.pool) > 0 {
		var r resourceWrapper
		r, p.pool = p.pool[0], p.pool[1:]
		state.InPool--
		state.InUse++
		if !cmd.when.IsZero() {
			state.WaitCount++
			state.WaitTime += time.Now().Sub(cmd.when)
		}
		cmd.callback <- r
		return
	}

	if state.activeTransient() >= state.Capacity {
		state.Waiters++
		cmd.when = time.Now()
		p.queue = append(p.queue, cmd)
		//p.cmd <- newWaiter{}
		return
	}

	state.Spawning++
	state.Waiters++
	select {
	case p.open <- openReq{
		reason:   forUse,
		callback: cmd.callback,
		ctx:      cmd.ctx,
		when:     cmd.when,
	}:
	default:
		state.Spawning--
		state.Waiters--
		cmd.callback <- errResource(ErrOpenBufferFull)
	}
}

type putCmd struct {
	callback chan error
	resource Resource
}

func (cmd putCmd) execute(p *SuperPool, state *State) {
	wrapper := resourceWrapper{
		resource: cmd.resource,
		timeUsed: time.Now(),
	}

	if state.InUse <= 0 {
		cmd.callback <- errWrap(ErrPutBeforeGet)
		return
	}

	if state.InPool+state.InUse > state.Capacity {
		if !state.Draining {
			cmd.callback <- errWrap(ErrFull)
			return
		}

		state.InUse--
		if cmd.resource != nil {
			state.Closing++
			p.close <- closeReq{
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

type setCapCmd struct {
	size int
	wait chan error
}

func (cmd setCapCmd) execute(p *SuperPool, state *State) {
	// Only allow one running SetCapacity at a time.
	if p.newCapWait != nil {
		p.newCapWait <- nil
		p.newCapWait = nil
	}

	if state.Closed {
		if cmd.wait != nil {
			cmd.wait <- errWrap(ErrClosed)
		}
		return
	}

	if cmd.size < 0 {
		if cmd.wait != nil {
			cmd.wait <- errStr("capacity %d is out of range", cmd.size)
		}
		return
	}

	if cmd.size > 0 && state.MinActive > cmd.size {
		if cmd.wait != nil {
			cmd.wait <- errStr("minActive %v would now be higher than capacity %v", state.MinActive, cmd.size)
		}
		return
	}

	state.Capacity = cmd.size
	toDrain := state.InPool + state.InUse - state.Capacity

	if state.Capacity == 0 {
		state.Closed = true
	}

	if toDrain <= 0 {
		if cmd.wait != nil {
			cmd.wait <- nil
		}
		return
	}

	var drain []resourceWrapper
	p.newCapWait = cmd.wait
	state.Draining = true
	if state.InPool < toDrain {
		drain, p.pool = p.pool, []resourceWrapper{}
		toDrain -= state.InPool
	} else {
		drain, p.pool = p.pool[:toDrain], p.pool[toDrain:]
	}
	state.InPool -= len(drain)
	state.Closing += len(drain)
	for _, w := range drain {
		p.close <- closeReq{wrapper: w}
	}
}

type setTimeoutCmd struct {
	timeout time.Duration
}

func (cmd setTimeoutCmd) execute(p *SuperPool, state *State) {
	state.IdleTimeout = cmd.timeout
	p.updateIdleTimer(state)
}

type openReq struct {
	callback chan resourceWrapper
	reason   createType
	ctx      context.Context
	when     time.Time
}

type didOpenCmd struct {
	request  openReq
	resource resourceWrapper
}

func (cmd didOpenCmd) execute(p *SuperPool, state *State) {
	state.Spawning--
	if cmd.request.reason == forUse {
		state.Waiters--
	}

	if cmd.resource.err != nil {
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

		if !cmd.request.when.IsZero() {
			state.WaitCount++
			state.WaitTime += time.Now().Sub(cmd.request.when)
		}
	}
	if cmd.request.callback != nil {
		cmd.request.callback <- cmd.resource
	}
}

type closeReq struct {
	wrapper resourceWrapper
}

type didClose struct {
	err error
}

func (cmd didClose) execute(p *SuperPool, state *State) {
	state.Closing--

	if state.Draining && state.Capacity >= state.InPool+state.InUse+state.Closing {
		state.Draining = false
		if state.Capacity == 0 {
			state.Closed = true
		}
		if p.newCapWait != nil {
			p.newCapWait <- cmd.err
			p.newCapWait = nil
		}
		return
	}
}

type testingOnlyReplaceState struct {
	state State
}

func (cmd testingOnlyReplaceState) execute(p *SuperPool, state *State) {
	*state = cmd.state
}

func (p *SuperPool) opener() {
	for {
		select {
		case <-p.finishOpenWorkers:
			return

		case requester := <-p.open:
			aborted := false;
			done := make(chan bool)
			doc := didOpenCmd{
				request: requester,
			}

			maybeCtx := requester.ctx
			var cancelChan <-chan struct{}
			if maybeCtx != nil {
				cancelChan = maybeCtx.Done()
			}

			var r Resource
			var err error
			go func() {
				defer func() {
					if r := recover(); r != nil {
						err = vterrors.NewWithoutCode(fmt.Sprintf("resource creation panicked: %v", r))
					}
					done <- true
				}()
				r, err = p.factory()
			}()

		WaitForResource:
			select {
			case <-done:
				if aborted {
					break
				}

				if err != nil {
					doc.resource = errResource(err)
				} else {
					doc.resource = resourceWrapper{
						resource: r,
						timeUsed: time.Now(),
					}
				}
				p.cmd <- doc

			case <-cancelChan:
				if aborted {
					// Caller has cancelled again. We ignore these.
					goto WaitForResource
				}
				aborted = true
				doc.resource = errResource(ErrTimeout)
				p.cmd <- doc

				goto WaitForResource
			}
		}
	}
}

func (p *SuperPool) closer() {
	for {
		select {
		case <-p.finishCloseWorkers:
			return

		case req := <-p.close:
			err := p.closeResource(req)
			p.cmd <- didClose{err: err}
		}
	}
}

func (p *SuperPool) closeResource(req closeReq) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = vterrors.NewWithoutCode(fmt.Sprintf("resource closing panicked: %v", r))
		}
	}()

	req.wrapper.resource.Close()
	return
}

func (p *SuperPool) Close() {
	err := p.SetCapacity(0, true)
	if err != nil {
		fmt.Printf("%+v\n", err)
		panic(err)
	}

	for i := 0; i < p.openWorkers; i++ {
		p.finishOpenWorkers <- true
	}
	for i := 0; i < p.closeWorkers; i++ {
		p.finishCloseWorkers <- true
	}
	p.finishMain <- true

	close(p.open)
	close(p.close)
	close(p.cmd)
}

func (p *SuperPool) Get(ctx context.Context) (Resource, error) {
	if p.State().Closed {
		return nil, errWrap(ErrClosed)
	}

	callback := make(chan resourceWrapper, 1)
	p.cmd <- getCmd{
		callback: callback,
		ctx:      ctx,
	}
	w := <-callback
	if w.err != nil {
		return nil, w.err
	}
	return w.resource, nil
}

func (p *SuperPool) Put(r Resource) {
	ret := make(chan error, 1)
	p.cmd <- putCmd{
		callback: ret,
		resource: r,
	}
	err := <-ret
	if err != nil {
		fmt.Printf("error: %+v\n", err)
		if vterrors.Equals(err, ErrFull) || vterrors.Equals(err, ErrPutBeforeGet) {
			panic(err)
		}
	}
}

func (p *SuperPool) SetCapacity(size int, block bool) error {
	if p.State().Closed {
		return errWrap(ErrClosed)
	}

	var wait chan error
	if block {
		wait = make(chan error, 1)
	}
	p.cmd <- setCapCmd{
		size: size,
		wait: wait,
	}
	if block {
		return <-wait
	}
	return nil
}

func (p *SuperPool) SetIdleTimeout(duration time.Duration) {
	p.cmd <- setTimeoutCmd{timeout: duration}
}

func (p *SuperPool) Capacity() int {
	return p.State().Capacity
}

func (p *SuperPool) IdleTimeout() time.Duration {
	return p.State().IdleTimeout
}

// MaxCap is not used in this pool implementation, so just give the current capacity.
func (p *SuperPool) MaxCap() int {
	return p.State().Capacity
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

func errWrap(err error) error {
	return vterrors.NewWithoutCode(err.Error())
}

func errStr(f string, s ...interface{}) error {
	return vterrors.NewWithoutCode(fmt.Sprintf(f, s...))
}

func errResource(err error) resourceWrapper {
	return resourceWrapper{err: errWrap(err)}
}
