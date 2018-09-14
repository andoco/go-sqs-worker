package sqslib

import "fmt"

type DBFailureError struct {
}

func (err DBFailureError) Error() string {
	return "access to the DB has failed"
}

func CheckPostgresDBFailure(err error) error {
	return err
}

type QueueFailureError struct {
	Err error
}

func (e QueueFailureError) Error() string {
	return fmt.Sprintf("access to the queue has failed: %v", e.Err)
}
