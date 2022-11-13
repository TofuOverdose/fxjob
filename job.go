package fxjob

import (
	"context"
	"fmt"
)

// Job represents a long-running process that runs in the background and activates on certain events.
// Job is configured via its Emitter and Handler and acts as a runtime for them:
// it waits for data from Emitter and passes it to Handler.
type Job interface {
	// Start initiates the job processing.
	// If job's Handler also implements Starter, its OnStart method will be called during start (with passing an error).
	// It can be called multiple times, but only after calling Stop() first, otherwise it will panic.
	Start(startCtx context.Context) error
	// Stop finishes the job processing.
	// If job's Handler also implements Stopper, its OnStop method wil be called during stop (with passing an error).
	// It can be called only after calling Start(), otherwise it will panic.
	Stop(stopCtx context.Context) error
	// Done provides a channel that will return value once the job processing is completely finished,
	// either due to unrecoverable error or if either Emitter or Handler returned TerminatingError.
	// The returned value from channel is nil if the job was stopped by calling Stop().
	Done() <-chan error
	// Errors provides a channel that returns all non-terminal errors produced by either Emitter or Handler.
	Errors() <-chan error
}

// Handler encapsulates a handling logic that Job executes after receiving data of type PT from its Emitter.
type Handler[PT any] interface {
	Handle(context.Context, PT) error
}

type handlerAdapter[PT any] struct {
	handleFunc func(context.Context, PT) error
}

func (h handlerAdapter[PT]) Handle(ctx context.Context, input PT) error {
	return h.handleFunc(ctx, input)
}

// HandlerFromFunc creates new Handler from the provided function.
// Use it if the handling logic is better expressed as a function and does not require initialization or destruction.
func HandlerFromFunc[PT any](handleFunc func(ctx context.Context, input PT) error) Handler[PT] {
	h := handlerAdapter[PT]{
		handleFunc: handleFunc,
	}
	return h
}

// Starter should be implemented alongside Handler when initialization is required.
type Starter interface {
	OnStart(context.Context) error
}

// Stopper should be implemented alongside Handler when destruction is required.
type Stopper interface {
	OnStop(context.Context) error
}

// Emitter encapsulates logic that triggers Job's processing with its Handler.
type Emitter[PT any] interface {
	Emit() (PT, error)
}

type emitterAdapter[PT any] struct {
	emitFunc func() (PT, error)
}

func (h emitterAdapter[PT]) Emit() (PT, error) {
	return h.emitFunc()
}

func EmitterFromFunc[PT any](emitFunc func() (PT, error)) Emitter[PT] {
	em := emitterAdapter[PT]{
		emitFunc: emitFunc,
	}
	return em
}

type progressor struct {
	// public chanel exposed in Errors()
	errorsChan chan error
	// public channel exposed in Done()
	doneChan chan error
	// for internal signalling
	stopChan chan struct{}
}

func newProgressor() progressor {
	return progressor{
		errorsChan: make(chan error, 1),
		doneChan:   make(chan error, 1),
		stopChan:   make(chan struct{}),
	}
}

func (p *progressor) Done() <-chan error {
	return p.doneChan
}

func (p *progressor) terminate(err error) {
	close(p.stopChan)
	p.doneChan <- err
	close(p.doneChan)
	close(p.errorsChan)
}

func (p *progressor) Errors() <-chan error {
	return p.errorsChan
}

func (p *progressor) sendError(err error) {
	if p.errorsChan != nil {
		p.errorsChan <- err
	}
}

type job[PT any] struct {
	progressor
	em Emitter[PT]
	h  Handler[PT]
}

func NewJob[EventType any](em Emitter[EventType], h Handler[EventType]) Job {
	return &job[EventType]{
		progressor: newProgressor(),
		em:         em,
		h:          h,
	}
}

func (j *job[PT]) Start(startCtx context.Context) error {
	if h, ok := j.h.(Starter); ok {
		if err := h.OnStart(startCtx); err != nil {
			return fmt.Errorf("handler returned error on start: %w", err)
		}
	}
	j.run()
	return nil
}

func (j *job[PT]) Stop(stopCtx context.Context) error {
	j.terminate(nil)
	if h, ok := j.h.(Stopper); ok {
		if err := h.OnStop(stopCtx); err != nil {
			return fmt.Errorf("handler returned error on stop: %w", err)
		}
	}
	return nil
}

func (j *job[PT]) run() {
	go j.runHandle(j.runEmit())
}

func (j *job[PT]) runEmit() (<-chan PT, <-chan error) {
	emChan, emErrChan := make(chan PT, 1), make(chan error, 1)

	go func() {
		defer j.recoverFromPanic()

		for {
			select {
			case <-j.stopChan:
				return
			default:
				e, err := j.em.Emit()
				if err != nil {
					emErrChan <- err
				}
				emChan <- e
			}
		}
	}()

	return emChan, emErrChan
}

func (j *job[PT]) runHandle(emChan <-chan PT, emErrChan <-chan error) {
	defer j.recoverFromPanic()

	hctx, hcancel := context.WithCancel(context.Background())
	defer hcancel()

loop:
	for {
		select {
		case <-j.stopChan:
			return
		case err := <-emErrChan:
			if _, ok := err.(TerminatingError); ok {
				j.terminate(err)
				return
			} else {
				j.sendError(err)
			}
		case e := <-emChan:
			err := j.h.Handle(hctx, e)
			if err == nil {
				continue loop
			}
			if _, ok := err.(TerminatingError); ok {
				j.terminate(err)
				return
			} else {
				j.sendError(err)
			}
		}
	}
}

func (j *job[PT]) recoverFromPanic() {
	if perr := recover(); perr != nil {
		j.terminate(fmt.Errorf("panic recovered: %v", perr))
	}
}
