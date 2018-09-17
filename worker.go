package sqslib

import (
	"context"
	"reflect"

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
	ReceiveQueue    string `json:"receive-queue" split_words:"true"`
	DeadletterQueue string `json:"deadletter-queue" split_words:"true"`
}

func NewDefaultWorker(config *WorkerConfig, exiter Exiter, sender Sender, receiver Receiver, deleter Deleter, dispatcher Dispatcher, logger *zap.SugaredLogger) *DefaultWorker {
	logger.Infow("Creating worker", "config", config)

	worker := &DefaultWorker{
		logger:     logger,
		config:     config,
		dispatcher: dispatcher,
		sender:     sender,
		receiver:   receiver,
		deleter:    deleter,
	}

	worker.Pre(&LogPreMsgHook{})
	worker.Pre(&MsgTypeHook{})

	worker.Post(&CompleterPostMsgHook{logger: logger, sender: sender, deleter: deleter})
	worker.Post(&LogPostMsgHook{})

	exitHook := &ExitPostMsgHook{logger: logger, exiter: exiter}
	exitHook.AddExitType(reflect.TypeOf(QueueFailureError{}))
	exitHook.AddExitType(reflect.TypeOf(DBFailureError{}))
	worker.Post(exitHook)

	return worker
}

type DefaultWorker struct {
	logger     *zap.SugaredLogger
	config     *WorkerConfig
	dispatcher Dispatcher
	preHooks   []MsgHook
	postHooks  []PostMsgHook
	sender     Sender
	receiver   Receiver
	deleter    Deleter
}

func (w *DefaultWorker) Pre(hook MsgHook) {
	w.preHooks = append(w.preHooks, hook)
}

func (w *DefaultWorker) Post(hook PostMsgHook) {
	w.postHooks = append(w.postHooks, hook)
}

func (w DefaultWorker) ReceiveAndDispatch(ctx context.Context) error {
	w.logger.Debugw("RECEIVE")
	messages, err := w.receiver.Receive(ctx, w.config.ReceiveQueue, 1)
	if err != nil {
		return errors.Wrap(err, "receiving messages")
	}
	w.logger.Debugw("RECEIVED", "numMessages", len(messages))

	ctx = context.WithValue(ctx, "receiveQueue", w.config.ReceiveQueue)
	ctx = context.WithValue(ctx, "deadletterQueue", w.config.DeadletterQueue)

	for _, msg := range messages {
		for _, hook := range w.preHooks {
			if ctx, err = hook.Handle(ctx, msg); err != nil {
				return errors.Wrap(err, "during pre hook")
			}
		}

		w.logger.Debugw("DISPATCHING", "messageId", msg.MessageId)
		msgErr := w.dispatcher.Dispatch(ctx, msg)

		for _, hook := range w.postHooks {
			if ctx, err = hook.Handle(ctx, msg, msgErr); err != nil {
				return errors.Wrap(err, "during post hook")
			}
		}

		if msgErr != nil {
			return msgErr
		}
	}

	return nil
}
