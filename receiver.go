package sqslib

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Receiver is an interface for receiving messages from a queue.
type Receiver interface {
	Receive(ctx context.Context, queue string, max int64) ([]*sqs.Message, error)
}

// Deleter is an interface for deleting a message from a queue.
type Deleter interface {
	Delete(ctx context.Context, msg *sqs.Message, queue string) error
}

func NewDefaultReceiver(config *DefaultReceiverConfig, logger *zap.SugaredLogger, svc *sqs.SQS) *DefaultReceiver {
	return &DefaultReceiver{config: config, logger: logger, svc: svc}
}

type DefaultReceiver struct {
	config *DefaultReceiverConfig
	svc    *sqs.SQS
	logger *zap.SugaredLogger
}

func NewDefaultReceiverConfig() *DefaultReceiverConfig {
	return &DefaultReceiverConfig{
		WaitTime: 20,
	}
}

type DefaultReceiverConfig struct {
	WaitTime int64
}

func (r DefaultReceiver) Receive(ctx context.Context, queue string, max int64) ([]*sqs.Message, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queue),
		MaxNumberOfMessages: aws.Int64(max),
		WaitTimeSeconds:     aws.Int64(r.config.WaitTime),
	}
	r.logger.Debugw("RECEIVING", "maxMessages", max)
	output, err := r.svc.ReceiveMessageWithContext(ctx, input)
	if err != nil {
		if isAwsCancelledError(err) {
			r.logger.Debugw("RECEIVE CANCELLED")
			return []*sqs.Message{}, nil
		}
		return nil, errors.Wrap(err, "receiving sqs messages")
	}

	r.logger.Debugw("RECEIVED", "numMessages", len(output.Messages))

	return output.Messages, nil
}

func (r DefaultReceiver) Delete(ctx context.Context, msg *sqs.Message, queue string) error {
	msgLogger := r.logger.With("messageId", msg.MessageId)

	input := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queue),
		ReceiptHandle: msg.ReceiptHandle,
	}
	msgLogger.Debugw("DELETING")
	_, err := r.svc.DeleteMessageWithContext(ctx, input)
	if err != nil {
		if isAwsCancelledError(err) {
			msgLogger.Debugw("DELETE CANCELLED")
			return nil
		}
		return errors.Wrap(err, "deleting sqs message")
	}

	msgLogger.Debugw("DELETED")

	return nil
}

func isAwsCancelledError(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		return awsErr.Code() == request.CanceledErrorCode
	}
	return false
}
