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

	configLoader := &DefaultConfigLoader{logger: sugar, prefix: name}

	router := NewDefaultRouter()

	sess, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
	if err != nil {
		sugar.Fatalw("Failed to create AWS session", "error", err)
	}
	svc := sqs.New(sess)

	receiverConfig := NewDefaultReceiverConfig()
	if err := configLoader.LoadConfig("receiver", receiverConfig); err != nil {
		sugar.Fatalw("Failed loading receiver config", "error", err)
	}
	receiver := NewDefaultReceiver(receiverConfig, sugar, svc)

	workerConfig := &WorkerConfig{}
	if err := configLoader.LoadConfig("worker", workerConfig); err != nil {
		sugar.Fatalw("Failed loading worker config", "error", err)
	}
	newWorker := func() Worker {
		return NewDefaultWorker(workerConfig, receiver, receiver, router, sugar)
	}

	scheduler := NewDefaultScheduler(sugar)

	exiter := &DefaultExiter{logger: sugar}

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
		a.logger.Fatalw("Failed to run scheduler", "error", err)
	}

	a.logger.Debug("FINISHED APP")
}
