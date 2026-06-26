package main

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/Boopitty/learn-pub-sub-starter/internal/gamelogic"
	"github.com/Boopitty/learn-pub-sub-starter/internal/pubsub"
	"github.com/Boopitty/learn-pub-sub-starter/internal/routing"
)

func unmarshallGameLog() func([]byte) (routing.GameLog, error) {
	return func(data []byte) (routing.GameLog, error) {
		buff := bytes.NewReader(data)
		decoder := gob.NewDecoder(buff)

		var gameLog routing.GameLog
		err := decoder.Decode(&gameLog)
		if err != nil {
			return routing.GameLog{}, err
		}
		return gameLog, nil
	}
}

func handlerGameLog() func(routing.GameLog) string {
	return func(gamelog routing.GameLog) string {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gamelog)
		if err != nil {
			fmt.Printf("Failed to write log: %v\n", err)
		}
		return pubsub.Ack
	}
}
