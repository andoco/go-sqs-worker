package sqslib

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/buger/jsonparser"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

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

// CompleterPostMsgHook is a hook that completes a successfully handled message by deleting from the queue.
type CompleterPostMsgHook struct {
	logger  *zap.SugaredLogger
	sender  Sender
	deleter Deleter
}

func (h *CompleterPostMsgHook) Handle(ctx context.Context, msg *sqs.Message, msgErr error) (context.Context, error) {
	msgLogger := h.logger.With("messageId", msg.MessageId)
	msgLogger.Debugw("COMPLETING")

	queueUrl, ok := ctx.Value("receiveQueue").(string)
	if !ok {
		return ctx, errors.New("no receiveQueue found in context")
	}

	if msgErr != nil {
		msgLogger.Debugw("DEADLETTERING", "reason", msgErr)
		deadletterQueue, ok := ctx.Value("deadletterQueue").(string)
		if !ok {
			return ctx, errors.New("no deadletterQueue found in context")
		}

		if err := h.sender.Send(ctx, *msg.Body, deadletterQueue); err != nil {
			return ctx, errors.Wrap(err, "sending message to deadletter queue")
		}
	}

	if err := h.deleter.Delete(ctx, msg, queueUrl); err != nil {
		return ctx, errors.Wrap(err, "could not delete message from queue")
	}

	return ctx, nil
}