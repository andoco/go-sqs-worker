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
	return &DefaultScheduler{
		config:     config,
		logger:     logger,
		exiter:     exiter,
		errMonitor: errMonitor,
		scaleChan:  make(chan scaleType),
	}
}

type DefaultScheduler struct {
	config        *DefaultSchedulerConfig
	logger        *zap.SugaredLogger
	exiter        Exiter
	errMonitor    ErrorMonitor
	workerCancels []context.CancelFunc
	scaleChan     chan scaleType
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
	wg := &sync.WaitGroup{}

	go func() {
		for {
			select {
			case <-ctx.Done():
				n := len(s.workerCancels)
				for i := 0; i < n; i++ {
					s.scaleChan <- scaleTypeDown
				}
				return
			case monitorResult := <-monitorChan:
				switch monitorResult {
				case MonitorResultTypeThrottle:
					s.logger.Errorw("Throttling due to exceeded error rate", "errorRateThreshold", ErrorThrottleThreshold)
					// add 1 to WaitGroup while throttling to prevent scheduler exiting
					wg.Add(1)
					for i := 0; i < len(s.workerCancels); i++ {
						s.scaleChan <- scaleTypeDown
					}
					time.Sleep(10 * time.Second)
					for i := 0; i < s.config.NumWorkers; i++ {
						s.scaleChan <- scaleTypeUp
					}
					wg.Done()
				case MonitorResultTypeShutdown:
					s.logger.Errorw("Exiting worker due to exceeded error rate", "errorRateThreshold", ErrorShutdownThreshold)
					n := len(s.workerCancels)
					for i := 0; i < n; i++ {
						s.scaleChan <- scaleTypeDown
					}
					return
				}
			}
		}
	}()

	s.runScaleWorkers(ctx, workerFunc, wg)

	// start the initial number of workers
	for i := 0; i < s.config.NumWorkers; i++ {
		s.scaleChan <- scaleTypeUp
	}

	s.logger.Debug("Waiting while workers are running")
	wg.Wait()
	s.logger.Debug("All worker have exited. Scheduler will now exit.")

	return nil
}

type scaleType int

const (
	scaleTypeUp scaleType = iota
	scaleTypeDown
)

func (s *DefaultScheduler) runScaleWorkers(ctx context.Context, workerFunc NewWorkerFunc, wg *sync.WaitGroup) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case scale := <-s.scaleChan:
				switch scale {
				case scaleTypeUp:
					workerLogger := s.logger
					workerCtx, cancel := context.WithCancel(ctx)
					s.workerCancels = append(s.workerCancels, cancel)
					wg.Add(1)

					go func(logger *zap.SugaredLogger) {
						worker := workerFunc(logger)
						if err := worker.Run(workerCtx); err != nil {
							s.logger.Errorw("worker returned error while running", "error", err)
						}
						wg.Done()
						workerLogger.Debug("worker has exited")
						return
					}(workerLogger)

				case scaleTypeDown:
					if len(s.workerCancels) > 0 {
						s.workerCancels[len(s.workerCancels)-1]()
						s.workerCancels = s.workerCancels[:len(s.workerCancels)-1]
					}
				}
			}
		}
	}()
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
	Channel() chan MonitorResultType
	Inc()
}

func NewDefaultErrorMonitor() *DefaultErrorMonitor {
	return &DefaultErrorMonitor{
		ch:      make(chan MonitorResultType),
		counter: ratecounter.NewRateCounter(1 * time.Second),
	}
}

type DefaultErrorMonitor struct {
	ch      chan MonitorResultType
	counter *ratecounter.RateCounter
}

func (d *DefaultErrorMonitor) Monitor(ctx context.Context) chan MonitorResultType {
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
			errRate := d.counter.Rate()
			if errRate >= ErrorShutdownThreshold {
				d.ch <- MonitorResultTypeShutdown
			} else if errRate >= ErrorThrottleThreshold {
				d.ch <- MonitorResultTypeThrottle
			}
		}
	}()
	return d.ch
}

func (d DefaultErrorMonitor) Channel() chan MonitorResultType {
	return d.ch
}

func (d *DefaultErrorMonitor) Inc() {
	d.counter.Incr(1)
}
