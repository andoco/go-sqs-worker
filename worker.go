package sqslib

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Worker is an interface for managing a message processing pipeline
type Worker interface {
	// ReceiveAndDispatch will receive a single message from the SQS queue and dispatch to the registered handler.
	ReceiveAndDispatch(ctx context.Context) error
}

type WorkerConfig struct {
	ReceiveQueue    string `json:"receive-queue" split_words:"true"`
	DeadletterQueue string `json:"deadletter-queue" split_words:"true"`
}

func NewDefaultWorker(config *WorkerConfig, exiter Exiter, pipeline Pipeline, sender Sender, receiver Receiver, deleter Deleter, logger *zap.SugaredLogger) *DefaultWorker {
	logger.Infow("Creating worker", "config", config)

	worker := &DefaultWorker{
		logger:   logger,
		config:   config,
		pipeline: pipeline,
		sender:   sender,
		receiver: receiver,
		deleter:  deleter,
	}

	return worker
}

type DefaultWorker struct {
	logger   *zap.SugaredLogger
	config   *WorkerConfig
	pipeline Pipeline
	sender   Sender
	receiver Receiver
	deleter  Deleter
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
		if err := w.pipeline.Process(ctx, msg); err != nil {
			return errors.Wrap(err, "processing message in pipeline")
		}
	}

	return nil
}
