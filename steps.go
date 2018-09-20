package sqslib

import (
	"context"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/buger/jsonparser"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// MsgType

type MsgTypeStep struct {
}

func (s MsgTypeStep) Process(ctx context.Context, msg *sqs.Message) (context.Context, error) {
	data := []byte(*msg.Body)
	field, err := jsonparser.GetString(data, "header", "msgType")
	if err != nil {
		return ctx, errors.Wrap(err, "extracting msgType from header as routingKey")
	}
	ctx = context.WithValue(ctx, RoutingKey, field)
	return ctx, nil
}

// Dispatcher

type DispatchStep struct {
	dispatcher Dispatcher
}

func (s DispatchStep) Process(ctx context.Context, msg *sqs.Message) (context.Context, error) {
	if err := s.dispatcher.Dispatch(ctx, msg); err != nil {
		ctx = context.WithValue(ctx, "msgError", err)
		ctx = context.WithValue(ctx, "msgSuccess", false)
	} else {
		ctx = context.WithValue(ctx, "msgSuccess", true)
	}

	return ctx, nil
}

// Completer

type CompleterStep struct {
	logger  *zap.SugaredLogger
	sender  Sender
	deleter Deleter
}

func (s CompleterStep) Process(ctx context.Context, msg *sqs.Message) (context.Context, error) {
	msgLogger := s.logger.With("messageId", msg.MessageId)
	msgLogger.Debugw("COMPLETING")

	queueUrl, ok := ctx.Value("receiveQueue").(string)
	if !ok {
		return ctx, errors.New("no receiveQueue found in context")
	}

	msgSuccess, ok := ctx.Value("msgSuccess").(bool)
	if !ok {
		return ctx, errors.New("no msgSuccess found in context")
	}

	if !msgSuccess {
		msgErr, ok := ctx.Value("msgError").(error)
		if !ok {
			return ctx, errors.New("no msgError found in context")
		}

		if msgErr != nil {
			msgLogger.Debugw("DEADLETTERING", "reason", msgErr)
			deadletterQueue, ok := ctx.Value("deadletterQueue").(string)
			if !ok {
				return ctx, errors.New("no deadletterQueue found in context")
			}

			if err := s.sender.Send(ctx, *msg.Body, deadletterQueue); err != nil {
				return ctx, errors.Wrap(err, "sending message to deadletter queue")
			}

			if err := s.deleter.Delete(ctx, msg, queueUrl); err != nil {
				return ctx, errors.Wrap(err, "could not delete message from queue")
			}

			return ctx, msgErr
		}
	}

	if err := s.deleter.Delete(ctx, msg, queueUrl); err != nil {
		return ctx, errors.Wrap(err, "could not delete message from queue")
	}

	return ctx, nil
}
