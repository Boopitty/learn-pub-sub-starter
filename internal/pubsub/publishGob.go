package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGob[T any](
	ch *amqp.Channel,
	exchange,
	key string,
	val T,
) error {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b) // Create a new encoder with buffer
	err := enc.Encode(val)    // Encode val onto the buffer
	if err != nil {
		return err
	}

	// Publish the buffer to the exhange
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        b.Bytes(),
	})
	if err != nil {
		return err
	}
	return nil
}
