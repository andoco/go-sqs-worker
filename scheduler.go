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

const ThrottleThreshold = 2
const ShutdownThreshold = 3

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
				switch {
				case counter.Rate() >= ShutdownThreshold:
					s.logger.Errorw("Triggering shutdown due to exceeded error rate", "errorRate", counter.Rate(), "errorRateThreshold", ShutdownThreshold)
					s.exiter.Trigger()
				case counter.Rate() >= ThrottleThreshold:
					s.logger.Errorw("Throttling due to exceeded error rate", "errorRate", counter.Rate(), "errorRateThreshold", ThrottleThreshold)
					time.Sleep(10 * time.Second)
				}
			}
		}
	}
}
