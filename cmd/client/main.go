package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"

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
		fmt.Printf("Connection Failed: %v\n", err)
		return
	}
	defer conn.Close()
	fmt.Println("Connection Successful!")

	// Create a channel using the connection
	channel, err := conn.Channel()
	if err != nil {
		fmt.Printf("Error Creating Channel: %v\n", err)
		return
	}

	// Username input
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Create gamestate
	gameState := gamelogic.NewGameState(username)

	// Subsrcibe to the server pause
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.SimpleQueueType{
			IsDurable: false,
		},
		handlerPause(gameState),
	)
	if err != nil {
		fmt.Printf("Failed to subsribe: %v\n", err)
		return
	}

	// Subscribe to ArmyMoves
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.SimpleQueueType{
			IsDurable: false,
		},
		handlerMove(gameState, channel),
	)

	// Subscribe to WarReecognition
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.SimpleQueueType{
			IsDurable: true,
		},
		handlerWar(gameState, channel),
	)

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "spam":
			if len(input) < 2 {
				fmt.Println("Usage: spam <integer>")
				continue
			}
			num, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			for n := 1; n < num; n++ {
				msg := gamelogic.GetMaliciousLog()
				log := routing.GameLog{
					Message: msg,
				}

				err := pubsub.PublishGob(
					channel,
					routing.ExchangePerilTopic,
					fmt.Sprintf("%s.%s", routing.GameLogSlug, username),
					log,
				)
				if err != nil {
					fmt.Printf("Failed to publish: %v\n", err)
					break
				}
			}
			continue

		case "spawn":
			err = gameState.CommandSpawn(input)
			if err != nil {
				fmt.Printf("Failed to Spawn Units: %v\n", err)
			}
			continue

		case "move":
			// Run .CommandMove() and return an ArmyMove object
			am, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Printf("Failed to Move Units: %v\n", err)
			}

			// Publish a JSON move message to the exchange.
			err = pubsub.PublishJSON(
				channel,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username),
				am,
			)
			if err != nil {
				fmt.Printf("Failed to publish message: %v\n", err)
			} else {
				fmt.Println("Move published successfully!")
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
