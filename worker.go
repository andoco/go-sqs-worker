package sqslib

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/buger/jsonparser"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Worker is an interface for managing a message processing pipeline
type Worker interface {
	Pre(hook MsgHook)
	Post(hook PostMsgHook)
	// ReceiveAndDispatch will receive a single message from the SQS queue and dispatch to the registered handler.
	ReceiveAndDispatch(ctx context.Context) error
}

type WorkerConfig struct {
	ReceiveQueue string `json:"receive-queue" split_words:"true"`
}

func NewDefaultWorker(config *WorkerConfig, receiver Receiver, dispatcher Dispatcher, logger *zap.SugaredLogger) *DefaultWorker {
	logger.Infow("Creating worker", "config", config)

	worker := &DefaultWorker{
		logger:     logger,
		config:     config,
		dispatcher: dispatcher,
		receiver:   receiver,
	}

	worker.Pre(&LogPreMsgHook{})
	worker.Pre(&MsgTypeHook{})

	worker.Post(&LogPostMsgHook{})

	return worker
}

type DefaultWorker struct {
	logger     *zap.SugaredLogger
	config     *WorkerConfig
	dispatcher Dispatcher
	preHooks   []MsgHook
	postHooks  []PostMsgHook
	receiver   Receiver
}

func (w *DefaultWorker) Pre(hook MsgHook) {
	w.preHooks = append(w.preHooks, hook)
}

func (w *DefaultWorker) Post(hook PostMsgHook) {
	w.postHooks = append(w.postHooks, hook)
}

func (w DefaultWorker) ReceiveAndDispatch(ctx context.Context) error {
	messages, err := w.receiver.Receive(ctx, w.config.ReceiveQueue, 1)
	if err != nil {
		return errors.Wrap(err, "receiving messages")
	}

	for _, msg := range messages {
		for _, hook := range w.preHooks {
			if ctx, err = hook.Handle(ctx, msg); err != nil {
				return errors.Wrap(err, "during pre hook")
			}
		}

		err = w.dispatcher.Dispatch(ctx, msg)

		for _, hook := range w.postHooks {
			if ctx, err = hook.Handle(ctx, msg, err); err != nil {
				return errors.Wrap(err, "during post hook")
			}
		}
	}

	return nil
}

// Hooks

type MsgHook interface {
	Handle(ctx context.Context, msg *sqs.Message) (context.Context, error)
}

type PostMsgHook interface {
	Handle(ctx context.Context, msg *sqs.Message, msgErr error) (context.Context, error)
}

type LogPreMsgHook struct {
}

func (_ LogPreMsgHook) Handle(ctx context.Context, msg *sqs.Message) (context.Context, error) {
	log.Printf("Started handling msg")
	return ctx, nil
}

type LogPostMsgHook struct {
}

func (_ LogPostMsgHook) Handle(ctx context.Context, msg *sqs.Message, msgErr error) (context.Context, error) {
	log.Printf("Finished handling msg. Err=%v", msgErr)
	return ctx, nil
}

type MsgTypeHook struct {
}

func (h *MsgTypeHook) Handle(ctx context.Context, msg *sqs.Message) (context.Context, error) {
	data := []byte(*msg.Body)
	field, err := jsonparser.GetString(data, "header", "msgType")
	if err != nil {
		return ctx, errors.Wrap(err, "extracting msgType from header as routingKey")
	}
	ctx = context.WithValue(ctx, RoutingKey, field)
	return ctx, nil
}
