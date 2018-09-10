package sqslib

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Scheduler is an interface for managing a set of Worker instances
type Scheduler interface {
	Run(ctx context.Context, workerFunc NewWorkerFunc) error
}

func NewDefaultScheduler(logger *zap.SugaredLogger) *DefaultScheduler {
	return &DefaultScheduler{logger: logger}
}

type DefaultScheduler struct {
	logger *zap.SugaredLogger
}

type NewWorkerFunc func() Worker

func (s DefaultScheduler) Run(ctx context.Context, workerFunc NewWorkerFunc) error {
	s.logger.Debug("SCHEDULING")
	worker := workerFunc()
	s.logger.Debug("Created worker")

	for {
		select {
		case <-ctx.Done():
			s.logger.Debugw("Context was cancelled. Will exit scheduler.")
			return nil
		default:
			if err := worker.ReceiveAndDispatch(ctx); err != nil {
				return errors.Wrap(err, "receiving and dispatching")
			}
		}
	}
}
