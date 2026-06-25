package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Subsribe a user to recieve notifications from given queue
func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) string, // handler that returns a "queueType"
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
		fmt.Printf("Running goRoutine for %s...\n", queueName)

		// Blocks until a delivery is received from the queue
		for d := range deliveries {
			var data T
			err := json.Unmarshal(d.Body, &data) // Decode the body
			if err != nil {
				fmt.Printf("Failed to Unmarshal: %v\n", err)
				continue
			}

			ackType := handler(data) // Process the data using the provided handler function
			switch ackType {
			case "Ack":
				err = d.Ack(false)
				if err != nil {
					fmt.Printf("Failed to Acknowlege: %v\n", err)
					continue
				}

			case "NackRequeue":
				err = d.Nack(false, true)
				if err != nil {
					fmt.Printf("Failed NackRequeue: %v\n", err)
					continue
				}

			case "NackDiscard":
				err = d.Nack(false, false)
				if err != nil {
					fmt.Printf("Failed NackDiscard: %v\n", err)
					continue
				}

			default:
				continue
			}
		}
	}()
	return nil
}
