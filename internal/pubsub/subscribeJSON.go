package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {
	channel, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}

	deliveries, err := channel.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		fmt.Println("Running goRoutine...")
		for d := range deliveries {
			var data T
			err := json.Unmarshal(d.Body, &data)
			if err != nil {
				fmt.Printf("Failed to Unmarshal: %v\n", err)
				continue
			}

			handler(data)

			err = d.Ack(false)
			if err != nil {
				fmt.Printf("Failed to Ack: %v\n", err)
				continue
			}
		}
	}()
	return nil
}
