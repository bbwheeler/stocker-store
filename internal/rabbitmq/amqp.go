package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// connection is the subset of *amqp.Connection the consumer and publisher use.
// It is unexported so tests can inject a fake without a broker.
type connection interface {
	Channel() (channel, error)
	Close() error
}

// confirmation is the subset of *amqp.DeferredConfirmation the publisher uses.
type confirmation interface {
	WaitContext(ctx context.Context) (bool, error)
}

// channel is the subset of *amqp.Channel the consumer and publisher use.
type channel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Qos(prefetchCount, prefetchSize int, global bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	Confirm(noWait bool) error
	PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (confirmation, error)
	IsClosed() bool
	Close() error
}

// dialFunc opens an AMQP connection. It is injectable so tests never touch a
// broker.
type dialFunc func(string) (connection, error)

// amqpConnection adapts *amqp.Connection to connection. The adapter is needed
// because (*amqp.Connection).Channel returns the concrete *amqp.Channel.
type amqpConnection struct {
	*amqp.Connection
}

func (c amqpConnection) Channel() (channel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}
	return amqpChannel{ch}, nil
}

// amqpChannel adapts *amqp.Channel to channel. The adapter is needed because
// PublishWithDeferredConfirmWithContext returns the concrete
// *amqp.DeferredConfirmation, which cannot satisfy an interface method that
// returns the confirmation interface.
type amqpChannel struct {
	*amqp.Channel
}

func (c amqpChannel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (confirmation, error) {
	dc, err := c.Channel.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil {
		return nil, err
	}
	return dc, nil
}

// defaultDial connects to the broker at url.
func defaultDial(url string) (connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return amqpConnection{conn}, nil
}
