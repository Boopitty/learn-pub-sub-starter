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

	// Create gamestate
	gameState := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("pause.%s", username),
		routing.PauseKey,
		pubsub.SimpleQueueType{
			Durable:   false,
			Transient: true,
		},
		handlerPause(gameState),
	)
	if err != nil {
		fmt.Printf("Failed to subsribe: %v", err)
		return
	}

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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}
