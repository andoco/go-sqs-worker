package sqslib

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/sqs"
)

const RoutingKey = "routingKey"

// Router is an interface for routing messages to a MsgHandler
type Router interface {
	Route(msgType string, handler MsgHandler) error
}

type Dispatcher interface {
	Dispatch(ctx context.Context, msg *sqs.Message) error
}

func NewDefaultRouter() *DefaultRouter {
	return &DefaultRouter{routes: map[string]MsgHandler{}}
}

type DefaultRouter struct {
	routes map[string]MsgHandler
}

func (r *DefaultRouter) Route(msgType string, handler MsgHandler) error {
	r.routes[msgType] = handler
	return nil
}

func (r DefaultRouter) Dispatch(ctx context.Context, msg *sqs.Message) error {
	routingKey, ok := ctx.Value(RoutingKey).(string)
	if !ok {
		return fmt.Errorf("routingKey value not found in context.Context under key %s", RoutingKey)
	}

	route, found := r.routes[routingKey]
	if !found {
		return NoRouteError{RoutingKey: routingKey}
	}

	return route.Handle(ctx, msg)
}

type NoRouteError struct {
	RoutingKey string
}

func (e NoRouteError) Error() string {
	return fmt.Sprintf("no route found for %s", e.RoutingKey)
}
