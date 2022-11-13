package fxjob

import "fmt"

// TerminatingError can be returned by Handler in case the job needs to be stopped permanently.
// It should be used in rare cases where it's not possible to continue executing the job.
// Note that this error won't be propagated outside of Job it was returned to, only its Cause error will be.
type TerminatingError struct {
	Cause error
}

// Error is an implementation of standard error interface.
func (e TerminatingError) Error() string {
	return fmt.Sprintf("job was terminated by handler: %s", e.Cause.Error())
}

// Unwrap returns its Cause to support unwrapping with errors.Unwrap.
func (e TerminatingError) Unwrap() error {
	return e.Cause
}
