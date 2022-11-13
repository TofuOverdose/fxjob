package fxjob_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"fxjob"
)

func TestJob_StartStop(t *testing.T) {
	em := fxjob.EmitterFromFunc(func() (mockPayload, error) {
		return mockPayload{}, nil
	})

	h := fxjob.HandlerFromFunc(func(ctx context.Context, input mockPayload) error {
		return nil
	})

	assert.NotPanics(t, func() {
		j := fxjob.NewJob(em, h)

		err := j.Start(context.Background())
		assert.NoError(t, err)

		err = j.Stop(context.Background())
		assert.NoError(t, err)
	})
}

func TestJob_Start_OnStartError(t *testing.T) {
	em := fxjob.EmitterFromFunc(func() (mockPayload, error) {
		return mockPayload{}, nil
	})

	h := mockHandler{
		onStart: func(ctx context.Context) error {
			return errors.New("error")
		},
	}

	assert.NotPanics(t, func() {
		j := fxjob.NewJob[mockPayload](em, h)

		err := j.Start(context.Background())
		assert.Error(t, err)
	})
}

func TestJob_Stop_OnStopError(t *testing.T) {
	em := fxjob.EmitterFromFunc(func() (mockPayload, error) {
		return mockPayload{}, nil
	})

	h := mockHandler{
		onStart: func(ctx context.Context) error {
			return nil
		},
		handle: func(ctx context.Context, input mockPayload) error {
			return nil
		},
		onStop: func(ctx context.Context) error {
			return errors.New("error")
		},
	}

	assert.NotPanics(t, func() {
		j := fxjob.NewJob[mockPayload](em, h)

		err := j.Start(context.Background())
		assert.NoError(t, err)

		err = j.Stop(context.Background())
		assert.Error(t, err)
	})
}

func TestJob_Handle_ErrorsFromEmitter(t *testing.T) {
	expectedError := errors.New("error")
	em := fxjob.EmitterFromFunc(func() (mockPayload, error) {
		return mockPayload{}, expectedError
	})

	h := fxjob.HandlerFromFunc(func(ctx context.Context, input mockPayload) error {
		return nil
	})

	j := fxjob.NewJob[mockPayload](em, h)
	jobErrors := j.Errors()

	err := j.Start(context.Background())
	assert.NoError(t, err)

	assert.NotNil(t, jobErrors)
	err = <-jobErrors
	assert.ErrorIs(t, err, expectedError)

	select {
	case <-j.Done():
		assert.Fail(t, "job should not be done")
	default:
		break
	}
}

func TestJob_Handle_TerminatedFromEmitter(t *testing.T) {
	expectedError := errors.New("error")
	em := fxjob.EmitterFromFunc(func() (mockPayload, error) {
		return mockPayload{}, fxjob.TerminatingError{Cause: expectedError}
	})

	h := fxjob.HandlerFromFunc(func(ctx context.Context, input mockPayload) error {
		t.Fatal("handle should not be called")
		return nil
	})

	j := fxjob.NewJob[mockPayload](em, h)
	jobErrors := j.Errors()
	jobDone := j.Done()

	err := j.Start(context.Background())
	assert.NoError(t, err)

	assert.NotNil(t, jobErrors)
	err, open := <-jobErrors
	assert.NoError(t, err)
	assert.False(t, open, "Error() should be closed")

	err = <-jobDone
	terr := fxjob.TerminatingError{}
	assert.ErrorAs(t, err, &terr)
	assert.ErrorIs(t, terr.Unwrap(), expectedError)
}

func TestJob_Handle_PanicFromEmitter(t *testing.T) {
	em := fxjob.EmitterFromFunc(func() (mockPayload, error) {
		panic("pls help me")
	})

	h := fxjob.HandlerFromFunc(func(ctx context.Context, input mockPayload) error {
		t.Fatal("handle should not be called")
		return nil
	})

	assert.NotPanics(t, func() {
		j := fxjob.NewJob[mockPayload](em, h)
		jobErrors := j.Errors()
		jobDone := j.Done()

		err := j.Start(context.Background())
		assert.NoError(t, err)

		assert.NotNil(t, jobErrors)
		err, open := <-jobErrors
		assert.NoError(t, err)
		assert.False(t, open, "Error() should be closed")

		err = <-jobDone
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "panic recovered")
	})
}

func TestJob_Handle_ErrorsFromHandler(t *testing.T) {
	em := fxjob.EmitterFromFunc(func() (mockPayload, error) {
		return mockPayload{}, nil
	})

	expectedError := errors.New("error")
	h := fxjob.HandlerFromFunc(func(ctx context.Context, input mockPayload) error {
		return expectedError
	})

	j := fxjob.NewJob[mockPayload](em, h)
	jobErrors := j.Errors()

	err := j.Start(context.Background())
	assert.NoError(t, err)

	assert.NotNil(t, jobErrors)
	err = <-jobErrors
	assert.ErrorIs(t, err, expectedError)

	select {
	case <-j.Done():
		assert.Fail(t, "job should not be done")
	default:
		break
	}
}

func TestJob_Handle_TerminatedFromHandler(t *testing.T) {
	expectedError := errors.New("error")
	em := fxjob.EmitterFromFunc(func() (mockPayload, error) {
		return mockPayload{}, nil
	})

	h := fxjob.HandlerFromFunc(func(ctx context.Context, input mockPayload) error {
		return fxjob.TerminatingError{Cause: expectedError}
	})

	j := fxjob.NewJob[mockPayload](em, h)
	jobErrors := j.Errors()
	jobDone := j.Done()

	err := j.Start(context.Background())
	assert.NoError(t, err)

	assert.NotNil(t, jobErrors)
	err, open := <-jobErrors
	assert.NoError(t, err)
	assert.False(t, open, "Error() should be closed")

	err = <-jobDone
	terr := fxjob.TerminatingError{}
	assert.ErrorAs(t, err, &terr)
	assert.ErrorIs(t, terr.Unwrap(), expectedError)
}

func TestJob_Handle_PanicFromHandler(t *testing.T) {
	em := fxjob.EmitterFromFunc(func() (mockPayload, error) {
		return mockPayload{}, nil
	})

	h := fxjob.HandlerFromFunc(func(ctx context.Context, input mockPayload) error {
		panic("pls help me")
	})

	assert.NotPanics(t, func() {
		j := fxjob.NewJob[mockPayload](em, h)
		jobErrors := j.Errors()
		jobDone := j.Done()

		err := j.Start(context.Background())
		assert.NoError(t, err)

		assert.NotNil(t, jobErrors)
		err, open := <-jobErrors
		assert.NoError(t, err)
		assert.False(t, open, "Error() should be closed")

		err = <-jobDone
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "panic recovered")
	})
}

func TestJob_Handle_EndToEnd(t *testing.T) {
	payload := []mockPayload{{"uno"}, {"dos"}, {"tres"}}

	emIter := -1
	em := fxjob.EmitterFromFunc(func() (mockPayload, error) {
		emIter += 1
		if emIter >= len(payload) {
			// just hangs
			c := make(chan struct{})
			c <- struct{}{}
		}
		return payload[emIter], nil
	})

	hDone := make(chan struct{})
	hTimesCalled := 0
	h := fxjob.HandlerFromFunc(func(ctx context.Context, input mockPayload) error {
		if hTimesCalled+1 == len(payload) {
			close(hDone)
			return fxjob.TerminatingError{Cause: errors.New("im done")}
		}
		if hTimesCalled > len(payload) {
			assert.Fail(t, "handle called more times than expected")
		}
		assert.Equal(t, payload[hTimesCalled].Data, input.Data)
		hTimesCalled++
		return nil
	})

	j := fxjob.NewJob[mockPayload](em, h)
	jobErrors := j.Errors()
	jobDone := j.Done()

	err := j.Start(context.Background())
	assert.NoError(t, err)

	<-hDone

	terr := fxjob.TerminatingError{}
	err = <-jobDone
	assert.ErrorAs(t, err, &terr)

	_, open := <-jobDone
	assert.False(t, open, "Done() should be closed")

	_, open = <-jobErrors
	assert.False(t, open, "Errors() should be closed")
}

type mockPayload struct {
	Data string
}

type mockHandler struct {
	onStart func(ctx context.Context) error
	onStop  func(ctx context.Context) error
	handle  func(ctx context.Context, input mockPayload) error
}

func (h mockHandler) OnStart(ctx context.Context) error {
	return h.onStart(ctx)
}

func (h mockHandler) OnStop(ctx context.Context) error {
	return h.onStop(ctx)
}

func (h mockHandler) Handle(ctx context.Context, in mockPayload) error {
	return h.handle(ctx, in)
}
