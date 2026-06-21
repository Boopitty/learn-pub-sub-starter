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
	fmt.Println("Starting Peril client...")

	// Create a connection
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		fmt.Printf("Connection Failed: %v", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connection Successful")

	// Username input
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Error: %v", err)
		return
	}

	// Declare and bind a queue
	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.SimpleQueueType{
			Durable:   false,
			Transient: true,
		},
	)
	if err != nil {
		fmt.Printf("Declare and Bind Failed: %v", err)
		return
	}

	gameState := gamelogic.NewGameState(username)

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "spawn":
			err = gameState.CommandSpawn(input)
			if err != nil {
				fmt.Printf("Failed to Spawn Units: %v\n", err)
			}
			continue

		case "move":
			_, err = gameState.CommandMove(input)
			if err != nil {
				fmt.Printf("Failed to Move Units: %v\n", err)
			}
			continue

		case "status":
			gameState.CommandStatus()
			continue

		case "help":
			gamelogic.PrintClientHelp()
			continue

		case "quit":
			gamelogic.PrintQuit()

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
