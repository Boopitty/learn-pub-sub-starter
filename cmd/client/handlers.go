package main

import (
	"fmt"

	"github.com/Boopitty/learn-pub-sub-starter/internal/gamelogic"
	"github.com/Boopitty/learn-pub-sub-starter/internal/pubsub"
	"github.com/Boopitty/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) string {
	return func(ps routing.PlayingState) string {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return "Ack"
	}
}

func handlerMove(gs *gamelogic.GameState, channel *amqp.Channel) func(gamelogic.ArmyMove) string {
	return func(move gamelogic.ArmyMove) string {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)

		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return "Ack"

		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				channel,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, move.Player.Username),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				return "NackRequeue"
			}
			return "Ack"

		default:
			return "NackDiscard"

		}
	}
}

func handlerWar(gs *gamelogic.GameState, channel *amqp.Channel) func(gamelogic.RecognitionOfWar) string {
	return func(rw gamelogic.RecognitionOfWar) string {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return "NackRequeue"

		case gamelogic.WarOutcomeNoUnits:
			return "NackDiscard"

		case gamelogic.WarOutcomeOpponentWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(
				channel,
				msg,
				rw.Attacker.Username,
			)
			if err != nil {
				return "NackRequeue"
			}
			return "Ack"

		case gamelogic.WarOutcomeYouWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := publishGameLog(
				channel,
				msg,
				rw.Attacker.Username,
			)
			if err != nil {
				return "NackRequeue"
			}
			return "Ack"

		case gamelogic.WarOutcomeDraw:
			msg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := publishGameLog(
				channel,
				msg,
				rw.Attacker.Username,
			)
			if err != nil {
				return "NackRequeue"
			}
			return "Ack"

		default:
			fmt.Println("Error handling war: Discarding")
			return "NackDiscard"
		}
	}
}

func publishGameLog(channel *amqp.Channel, msg, attacker string) error {
	GameLog := struct {
		Message string
	}{
		Message: msg,
	}
	err := pubsub.PublishGob(
		channel,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.GameLogSlug, attacker),
		GameLog,
	)
	if err != nil {
		return nil
	}
	return nil
}
