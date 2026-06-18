package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Boopitty/learn-pub-sub-starter/internal/pubsub"
	"github.com/Boopitty/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	// Create a connection
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		fmt.Printf("Connection Failed: %v", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connection Successful")

	// make a channel using the connection
	channel, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to create channel: %v", err)
		return
	}

	err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	})
	if err != nil {
		fmt.Printf("Failed to publishJSON: %v", err)
		return
	}

	// wait for ctrl+c to exit
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Program shutting down. Closing Connection.")
}
