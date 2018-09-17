package sqslib

import (
	"context"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Scheduler is an interface for managing a set of Worker instances
type Scheduler interface {
	Run(ctx context.Context, workerFunc NewWorkerFunc) error
}

func NewDefaultScheduler(logger *zap.SugaredLogger, exiter Exiter, errMonitor ErrorMonitor) *DefaultScheduler {
	return &DefaultScheduler{logger: logger, exiter: exiter, errMonitor: errMonitor}
}

type DefaultScheduler struct {
	logger     *zap.SugaredLogger
	exiter     Exiter
	errMonitor ErrorMonitor
}

type NewWorkerFunc func() Worker

func (s DefaultScheduler) Run(ctx context.Context, workerFunc NewWorkerFunc) error {
	s.logger.Debug("SCHEDULING")
	worker := workerFunc()
	s.logger.Debug("Created worker")

	monitorChan := s.errMonitor.Monitor(ctx)

	for {
		select {
		case <-ctx.Done():
			s.logger.Debugw("Context was cancelled. Will exit scheduler.")
			return nil

		case monitorResult := <-monitorChan:
			switch monitorResult {
			case MonitorResultTypeThrottle:
				s.logger.Errorw("Throttling due to exceeded error rate", "errorRateThreshold", ErrorThrottleThreshold)
				time.Sleep(10 * time.Second)
			case MonitorResultTypeShutdown:
				s.logger.Errorw("Triggering shutdown due to exceeded error rate", "errorRateThreshold", ErrorShutdownThreshold)
				s.exiter.Trigger()
			}

		default:
			if err := worker.ReceiveAndDispatch(ctx); err != nil {
				s.logger.Errorw("Error returned from worker", "error", err)
				s.errMonitor.Inc()
			}
		}
	}
}

type MonitorResultType int

const (
	MonitorResultTypeNone MonitorResultType = iota
	MonitorResultTypeThrottle
	MonitorResultTypeShutdown
)

const ErrorThrottleThreshold = 2
const ErrorShutdownThreshold = 3

type ErrorMonitor interface {
	Monitor(ctx context.Context) chan MonitorResultType
	Inc()
}

func NewDefaultErrorMonitor() *DefaultErrorMonitor {
	return &DefaultErrorMonitor{}
}

type DefaultErrorMonitor struct {
	count uint64
}

func (d *DefaultErrorMonitor) Monitor(ctx context.Context) chan MonitorResultType {
	ch := make(chan MonitorResultType)
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
			errCount := atomic.LoadUint64(&d.count)
			if errCount >= ErrorShutdownThreshold {
				ch <- MonitorResultTypeShutdown
			} else if errCount >= ErrorThrottleThreshold {
				ch <- MonitorResultTypeThrottle
			}
		}
	}()
	return ch
}

func (d *DefaultErrorMonitor) Inc() {
	atomic.AddUint64(&d.count, 1)
}
