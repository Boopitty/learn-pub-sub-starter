package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Boopitty/learn-pub-sub-starter/internal/gamelogic"
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

	// Declare and bind a queue
	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		fmt.Sprintf("%s.%s", routing.GameLogSlug, "*"),
		pubsub.SimpleQueueType{
			Durable:   true,
			Transient: false,
		},
	)
	if err != nil {
		fmt.Printf("Declare and Bind Failed: %v", err)
		return
	}

	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "pause":
			fmt.Println("Sending 'pause' Message...")

			// publish a pause message in JSON
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
			if err != nil {
				fmt.Printf("Failed to publishJSON: %v", err)
				return
			}
			continue

		case "resume":
			fmt.Println("Sending 'resume' Message...")

			// publish a resume message in JSON
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
			if err != nil {
				fmt.Printf("Failed to publishJSON: %v", err)
				return
			}
			continue

		case "quit":
			fmt.Println("Exiting...")

		default:
			fmt.Println("Command Not Available")
			continue
		}
		break
	}

	// wait for ctrl+c to exit
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Program shutting down. Closing Connection.")
}
