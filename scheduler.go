package sqslib

import (
	"context"
	"time"

	"github.com/paulbellamy/ratecounter"
	"go.uber.org/zap"
)

// Scheduler is an interface for managing a set of Worker instances
type Scheduler interface {
	Run(ctx context.Context, workerFunc NewWorkerFunc) error
}

func NewDefaultScheduler(logger *zap.SugaredLogger, exiter Exiter) *DefaultScheduler {
	return &DefaultScheduler{logger: logger, exiter: exiter}
}

type DefaultScheduler struct {
	logger *zap.SugaredLogger
	exiter Exiter
}

type NewWorkerFunc func() Worker

const ErrorRateThreshold = 2

func (s DefaultScheduler) Run(ctx context.Context, workerFunc NewWorkerFunc) error {
	s.logger.Debug("SCHEDULING")
	worker := workerFunc()
	s.logger.Debug("Created worker")

	counter := ratecounter.NewRateCounter(1 * time.Second)

	for {
		select {
		case <-ctx.Done():
			s.logger.Debugw("Context was cancelled. Will exit scheduler.")
			return nil
		default:
			if err := worker.ReceiveAndDispatch(ctx); err != nil {
				s.logger.Errorw("Error returned from worker", "error", err)
				counter.Incr(1)
				if counter.Rate() >= ErrorRateThreshold {
					s.logger.Errorw("Triggering shutdown due to exceeded error rate", "errorRate", counter.Rate(), "errorRateThreshold", ErrorRateThreshold)
					s.exiter.Trigger()
				}
			}
		}
	}
}
