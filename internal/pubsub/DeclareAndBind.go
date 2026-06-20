package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType struct {
	Durable   bool
	Transient bool
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // "enum" type to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {

	// Create a new `.Channel()`
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	// Declare a new Queue with `.QueueDeclare()`
	queue, err := channel.QueueDeclare(
		queueName,
		queueType.Durable,
		queueType.Transient,
		queueType.Transient,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	// Bind the queue to the exchange using `.QueueBind()`
	err = channel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return nil, queue, nil
}
