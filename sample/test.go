package main

import (
	"fmt"
	"strconv"

	consumer "github.com/tomcanham/chronicle-consumer-golang"
)

func main() {
	server, err := consumer.Create(12345, 100, "/")
	if err != nil {
		fmt.Printf("Error creating consumer: %v", err)
		return
	}

	go server.Start()

	for {
		select {
		case message := <-server.Messages:
			fmt.Printf("Got message type: %d (%s) data: %+v\n", message.Type, message.TypeString, message.Data)

			fieldName := "block_num"
			offset := 0
			if message.Type == 1001 /* fork */ {
				offset = -1
			}
			blockNum, _ := strconv.Atoi(message.Data[fieldName].(string))
			server.AckBlock(blockNum + offset)
		case _, ok := <-server.ShutdownFlag:
			if !ok {
				fmt.Println("SHUTTING DOWN!")
				return
			}
		}
	}
}
