package sqslib

import (
	"context"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Step interface {
	Process(ctx context.Context, msg *sqs.Message) (context.Context, error)
}

type Pipeline interface {
	Process(ctx context.Context, msg *sqs.Message) error
}

type DefaultPipeline struct {
	logger     *zap.SugaredLogger
	dispatcher Dispatcher
	steps      []Step
}

func NewDefaultPipeline(logger *zap.SugaredLogger, steps []Step) *DefaultPipeline {
	return &DefaultPipeline{
		logger: logger,
		steps:  steps,
	}
}

func (w DefaultPipeline) Process(ctx context.Context, msg *sqs.Message) error {
	var err error

	for _, step := range w.steps {
		if ctx, err = step.Process(ctx, msg); err != nil {
			return errors.Wrap(err, "pipeline step error")
		}
	}

	return nil
}
