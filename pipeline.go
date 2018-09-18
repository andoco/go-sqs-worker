package sqslib

import (
	"context"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Pipeline interface {
	Process(ctx context.Context, msg *sqs.Message) error
}

type DefaultPipeline struct {
	logger     *zap.SugaredLogger
	dispatcher Dispatcher
	preHooks   []MsgHook
	postHooks  []PostMsgHook
}

func NewDefaultPipeline(logger *zap.SugaredLogger, preHooks []MsgHook, postHooks []PostMsgHook, dispatcher Dispatcher) *DefaultPipeline {
	return &DefaultPipeline{
		logger:     logger,
		preHooks:   preHooks,
		postHooks:  postHooks,
		dispatcher: dispatcher,
	}
}

func (w DefaultPipeline) Process(ctx context.Context, msg *sqs.Message) error {
	var err error

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

	return nil
}
