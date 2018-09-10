package sqslib

import (
	"context"

	"github.com/aws/aws-sdk-go/service/sqs"
)

type MsgHandlerFunc func(ctx context.Context, msg *sqs.Message) error

type MsgHandler interface {
	Handle(ctx context.Context, msg *sqs.Message) error
}

type FuncMsgHandler struct {
	f MsgHandlerFunc
}

func (h FuncMsgHandler) Handle(ctx context.Context, msg *sqs.Message) error {
	return h.f(ctx, msg)
}
