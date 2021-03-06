package consumer

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

type Message struct {
	Type       int32
	TypeString string
	Options    int32
	Data       map[string]interface{}
}

type ChronicleConsumer struct {
	Port         int
	Path         string
	QueueSize    int
	Messages     chan *Message
	conn         *websocket.Conn
	ShutdownFlag chan interface{}
	LastAck      int
}

var MessageTypes = map[int32]string{
	1001: "fork",
	1002: "block",
	1003: "tx",
	1004: "abi",
	1005: "abiRemoved",
	1006: "abiError",
	1007: "tableRow",
	1008: "encoderError",
	1009: "pause",
	1010: "blockCompleted",
	1011: "permission",
	1012: "permissionLink",
	1013: "accMetadata",
}

const (
	// Time allowed to write a message to the peer.
	writeWait = 60 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 120 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 64 * 1024
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func Create(port, queueSize int, path string) (*ChronicleConsumer, error) {
	if port == 0 {
		return nil, fmt.Errorf("missing port for consumer")
	}

	c := &ChronicleConsumer{Port: port, QueueSize: queueSize, Path: path}

	if c.QueueSize <= 0 {
		c.QueueSize = 100
	}

	if c.Path == "" {
		c.Path = "/"
	}

	// we use the channel size to force a maximum acknowledgement
	// queue size; once c.QueueSize messages are queued up, the channel
	// will block when send is called, eventually causing Chronicle
	// to pause.
	c.Messages = make(chan *Message, c.QueueSize)

	// used to signal the server to shut down
	c.ShutdownFlag = make(chan interface{})

	return c, nil
}

func (c *ChronicleConsumer) Start() error {
	log.Printf("starting server on 0.0.0.0:%d%s\n", c.Port, c.Path)
	log.Printf("max message queue size: %d\n", c.QueueSize)

	http.HandleFunc(c.Path, func(w http.ResponseWriter, r *http.Request) {
		if c.conn != nil {
			log.Print("warning: secondary connection attempt detected; shutting down existing connection")

			c.Shutdown()
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Fatalf("error upgrading socket connection; %v", err)
		} else {
			go c.messagePump(conn)
		}
	})

	err := http.ListenAndServe(fmt.Sprintf(":%d", c.Port), nil)
	if err != nil {
		return fmt.Errorf("ListenAndServe error; %v", err)
	}

	return nil
}

func (c *ChronicleConsumer) Shutdown() {
	if c.conn == nil {
		log.Fatal("attempt to shut down server that is already shut down")
	}
	close(c.ShutdownFlag)
	c.conn.Close()
	c.conn = nil
}

func (c *ChronicleConsumer) pingHandler() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			} else {
				log.Print("--> PING!")
			}

		case _, ok := <-c.ShutdownFlag:
			if !ok {
				return
			}
		}
	}
}

func (c *ChronicleConsumer) messagePump(conn *websocket.Conn) {
	c.conn = conn

	// make sure everything is cleaned up when we exit
	defer c.Shutdown()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetPongHandler(func(string) error {
		log.Print("<-- PONG!")

		return nil
	})

	oldPingHandler := c.conn.PingHandler()
	c.conn.SetPingHandler(func(appData string) error {
		log.Print("<-- PING!")
		result := oldPingHandler(appData)
		log.Print("--> PONG!")

		return result
	})

	// sends periodic pings until the shutdown signal is sent
	go c.pingHandler()

	for {
		select {
		case _, ok := <-c.ShutdownFlag:
			if !ok {
				log.Print("exiting due to shutdown flag")
				return
			}

		default:
			_, data, err := c.conn.ReadMessage()
			if err != nil {
				log.Printf("error: %v", err)
				return
			}

			reader := bytes.NewReader(data)
			var msgType, msgOptions int32

			binary.Read(reader, binary.LittleEndian, &msgType)
			binary.Read(reader, binary.LittleEndian, &msgOptions)

			payload := make([]byte, reader.Len())
			reader.Read(payload)

			var message *Message
			if message, err = c.decodeMessage(msgType, msgOptions, payload); err != nil {
				log.Printf("warning: unable to unmarshal message of type %d; %v", msgType, err)
			} else {
				c.Messages <- message
				if len(c.Messages) >= c.QueueSize/2 {
					log.Printf("message queue length now %d (max: %d)", len(c.Messages), cap(c.Messages))
				}
			}
		}
	}
}

func (c *ChronicleConsumer) decodeMessage(msgType, msgOptions int32, bytes []byte) (*Message, error) {
	var data map[string]interface{}

	if err := json.Unmarshal(bytes, &data); err != nil {
		return nil, err
	}

	typeString, ok := MessageTypes[msgType]

	if !ok {
		return nil, fmt.Errorf("could not find message type %d", msgType)
	}

	return &Message{
		Type:       msgType,
		TypeString: typeString,
		Options:    msgOptions,
		Data:       data,
	}, nil
}

func (c *ChronicleConsumer) AckBlock(blockNumber int) error {
	if blockNumber > c.LastAck {
		log.Printf("ACK: %d", blockNumber)
		err := c.conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprint(blockNumber)))
		c.LastAck = blockNumber
		return err
	} else {
		return fmt.Errorf("ack block number too small; last ack sent was %d, this block is %d", c.LastAck, blockNumber)
	}
}
