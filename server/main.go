package main

import (
	"encoding/binary"
	"log"
	"strconv"
	"time"

	nats "github.com/nats-io/go-nats"
)

const (
	connNum           = 2
	channelNumPerConn = 100
	payLoadSize       = 512
)

func callback(id uint64, nc *nats.Conn) func(m *nats.Msg) {
	payload := make([]byte, payLoadSize)
	return func(m *nats.Msg) {
		if len(m.Data) < 23 {
			log.Printf("Got incorrect data from subject: %s.", m.Subject)
		}
		clientID := binary.LittleEndian.Uint64(m.Data[:8])
		var pubTime time.Time
		pubTime.UnmarshalBinary(m.Data[8:23])
		now := time.Now()
		if now.After(pubTime.Add(2 * time.Second)) {
			log.Printf("Latency detected from client %d, pub time: %s", clientID, pubTime)
		}
		binary.LittleEndian.PutUint64(payload[:8], id)
		nc.Publish("client_"+strconv.FormatUint(clientID, 10), payload)
		// do something
	}
}

func createConn(cid uint64) {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Printf("Failed to connect to nats server: %v.", err)
		return
	}
	var id uint64
	for id = cid * channelNumPerConn; id < (cid+1)*channelNumPerConn; id++ {
		nc.QueueSubscribe("join", "channels", callback(id, nc))
	}
	<-time.NewTimer(time.Hour).C
}

func main() {
	var i uint64
	for i = 0; i < connNum; i++ {
		go createConn(i)
	}
	log.Printf("Ready.")
	<-time.NewTimer(time.Hour).C
}
