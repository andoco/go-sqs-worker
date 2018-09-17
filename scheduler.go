package sqslib

import (
	"context"
	"sync"
	"time"

	"github.com/paulbellamy/ratecounter"
	"go.uber.org/zap"
)

// Scheduler is an interface for managing a set of Worker instances
type Scheduler interface {
	Run(ctx context.Context, workerFunc NewWorkerFunc) error
}

func NewDefaultScheduler(config *DefaultSchedulerConfig, logger *zap.SugaredLogger, exiter Exiter, errMonitor ErrorMonitor) *DefaultScheduler {
	return &DefaultScheduler{config: config, logger: logger, exiter: exiter, errMonitor: errMonitor}
}

type DefaultScheduler struct {
	config     *DefaultSchedulerConfig
	logger     *zap.SugaredLogger
	exiter     Exiter
	errMonitor ErrorMonitor
}

func NewDefaultSchedulerConfig() *DefaultSchedulerConfig {
	return &DefaultSchedulerConfig{
		NumWorkers: 1,
	}
}

type DefaultSchedulerConfig struct {
	NumWorkers int
}

type NewWorkerFunc func(logger *zap.SugaredLogger) Worker

func (s DefaultScheduler) Run(ctx context.Context, workerFunc NewWorkerFunc) error {
	s.logger.Debugw("SCHEDULING", "numWorkers", s.config.NumWorkers)

	monitorChan := s.errMonitor.Monitor(ctx)

	wg := sync.WaitGroup{}

	for i := 0; i < s.config.NumWorkers; i++ {
		workerLogger := s.logger.With("workerId", i)
		wg.Add(1)

		go func(workerId int, logger *zap.SugaredLogger) {
			worker := workerFunc(logger)
			s.logger.Debugw("WORKER ROUTINE RUNNING", "workerId", workerId)

			for {
				select {
				case <-ctx.Done():
					workerLogger.Debugw("CONTEXT DONE")
					wg.Done()
					return

				case monitorResult := <-monitorChan:
					switch monitorResult {
					case MonitorResultTypeThrottle:
						workerLogger.Errorw("Throttling due to exceeded error rate", "errorRateThreshold", ErrorThrottleThreshold)
						time.Sleep(10 * time.Second)
					case MonitorResultTypeShutdown:
						workerLogger.Errorw("Triggering shutdown due to exceeded error rate", "errorRateThreshold", ErrorShutdownThreshold)
						s.exiter.Trigger()
					}

				default:
					if err := worker.ReceiveAndDispatch(ctx); err != nil {
						workerLogger.Errorw("Error returned from worker", "error", err)
						s.errMonitor.Inc()
					}
				}
			}
		}(i, workerLogger)
	}

	wg.Wait()

	return nil
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
	return &DefaultErrorMonitor{
		counter: ratecounter.NewRateCounter(1 * time.Second),
	}
}

type DefaultErrorMonitor struct {
	counter *ratecounter.RateCounter
}

func (d *DefaultErrorMonitor) Monitor(ctx context.Context) chan MonitorResultType {
	ch := make(chan MonitorResultType)
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
			errRate := d.counter.Rate()
			if errRate >= ErrorShutdownThreshold {
				ch <- MonitorResultTypeShutdown
			} else if errRate >= ErrorThrottleThreshold {
				ch <- MonitorResultTypeThrottle
			}
		}
	}()
	return ch
}

func (d *DefaultErrorMonitor) Inc() {
	d.counter.Incr(1)
}
