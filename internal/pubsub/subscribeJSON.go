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
	handler func(T) string,
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
		for d := range deliveries {
			var data T
			err := json.Unmarshal(d.Body, &data)
			if err != nil {
				fmt.Printf("Failed to Unmarshal: %v\n", err)
				continue
			}

			ackType := handler(data)
			switch ackType {
			case "Ack":
				fmt.Println("Acknowleging...")
				err = d.Ack(false)
				if err != nil {
					fmt.Printf("Failed to Acknowlege: %v\n", err)
					continue
				}

			case "NackRequeue":
				fmt.Println("Negative Acknowlege: Requeueing...")
				err = d.Nack(false, true)
				if err != nil {
					fmt.Printf("Failed NackRequeue: %v\n", err)
					continue
				}

			case "NackDiscard":
				fmt.Println("Negative Acknowlege: Discarding...")
				err = d.Nack(false, false)
				if err != nil {
					fmt.Printf("Failed NackDiscard: %v\n", err)
					continue
				}

			default:
				return
			}
		}
	}()
	return nil
}
