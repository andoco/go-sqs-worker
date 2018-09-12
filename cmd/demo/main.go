package main

import (
	"context"

	"github.com/aws/aws-sdk-go/service/sqs"
	sqslib "github.com/uswitch/sqs-lib"
)

func main() {
	workerApp := sqslib.NewDefaultApp("test")

	customConfig := &CustomConfig{}
	workerApp.Config.LoadConfig("custom", customConfig)

	workerApp.Handle("uswitch.foo", func(ctx context.Context, msg *sqs.Message) error {
		return nil
	})

	workerApp.Run()
}

type CustomConfig struct {
	Foo    string
	BarBaz string `split_words:"true"`
}
