package sqslib

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"go.uber.org/zap"
)

type App interface {
	SetConfigLoader(loader ConfigLoader)
	SetWorker(worker Worker)
	SetScheduler(scheduler Scheduler)
	SetRouter(router Router)
	Handle(msgType string, handler MsgHandlerFunc)
	Run()
}

func NewDefaultApp(name string) *DefaultApp {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	sugar := logger.Sugar()

	exiter := &DefaultExiter{logger: sugar}

	configLoader := &DefaultConfigLoader{logger: sugar, prefix: name}

	router := NewDefaultRouter()

	sess, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
	if err != nil {
		sugar.Fatalw("Failed to create AWS session", "error", err)
	}
	svc := sqs.New(sess)

	queueConfig := NewDefaultQueueConfig()
	if err := configLoader.LoadConfig("queue", queueConfig); err != nil {
		sugar.Fatalw("Failed loading queue config", "error", err)
	}
	queue := NewDefaultQueue(queueConfig, sugar, svc)

	preHooks := []MsgHook{
		&LogPreMsgHook{},
		&MsgTypeHook{},
	}

	postHooks := []PostMsgHook{
		&CompleterPostMsgHook{logger: sugar, sender: queue, deleter: queue},
		&LogPostMsgHook{},
	}

	pipeline := NewDefaultPipeline(sugar, preHooks, postHooks, router)

	errMonitor := NewDefaultErrorMonitor()

	workerConfig := &WorkerConfig{}
	if err := configLoader.LoadConfig("worker", workerConfig); err != nil {
		sugar.Fatalw("Failed loading worker config", "error", err)
	}
	newWorker := func(logger *zap.SugaredLogger) Worker {
		return NewDefaultWorker(workerConfig, exiter, pipeline, queue, queue, queue, logger, errMonitor)
	}

	schedulerConfig := NewDefaultSchedulerConfig()
	if err := configLoader.LoadConfig("scheduler", schedulerConfig); err != nil {
		sugar.Fatalw("Failed to load scheduler config", "error", err)
	}
	scheduler := NewDefaultScheduler(schedulerConfig, sugar, exiter, errMonitor)

	app := &DefaultApp{
		name:      name,
		logger:    sugar,
		exiter:    exiter,
		Config:    configLoader,
		Router:    router,
		Worker:    newWorker,
		Scheduler: scheduler,
	}

	return app
}

type DefaultApp struct {
	name      string
	logger    *zap.SugaredLogger
	exiter    Exiter
	Config    ConfigLoader
	Router    Router
	Worker    NewWorkerFunc
	Scheduler Scheduler
}

func (a *DefaultApp) Handle(msgType string, handler MsgHandlerFunc) {
	a.Router.Route(msgType, FuncMsgHandler{f: handler})
}

func (a *DefaultApp) Run() {
	a.logger.Debug("RUNNING APP")

	ctx := a.exiter.GetContext()

	if err := a.Scheduler.Run(ctx, a.Worker); err != nil {
		a.logger.Fatalw("Scheduler failed", "error", err)
	}

	a.logger.Debug("FINISHED APP")
}
