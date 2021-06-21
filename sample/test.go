package main

import (
	"fmt"

	consumer "github.com/tomcanham/chronicle-consumer-golang"
)

func main() {
	server, err := consumer.Create(8899, 100, "/")
	if err != nil {
		fmt.Printf("Error creating consumer: %v", err)
		return
	}

	go server.Start()

	for {
		select {
		case message := <-server.Messages:
			switch message.Type {
			case 1003:
				block := message.Block.(*consumer.Block)
				server.AckBlock(block)
				// fmt.Printf("Got block: %+v\n", message.Block.(*server.Block))
			}

		case _, ok := <-server.ShutdownFlag:
			if !ok {
				fmt.Println("SHUTTING DOWN!")
				return
			}
		}
	}
}
