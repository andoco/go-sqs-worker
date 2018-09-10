package sqslib

import (
	"context"
	"os"
	"os/signal"

	"go.uber.org/zap"
)

// Exiter is an interface for cancelling a context.Context when a program should exit
type Exiter interface {
	GetContext() context.Context
}

type DefaultExiter struct {
	logger *zap.SugaredLogger
}

func (e DefaultExiter) GetContext() context.Context {
	ctx := context.Background()

	// trap Ctrl+C and call cancel on the context
	ctx, cancel := context.WithCancel(ctx)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	/*defer func() {
		signal.Stop(c)
		cancel()
	}()*/
	go func() {
		select {
		case <-c:
			e.logger.Debugw("Cancelling context")
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx
}
