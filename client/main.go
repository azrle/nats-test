package main

import (
	"context"
	"encoding/binary"
	"log"
	"strconv"
	"sync"
	"time"

	nats "github.com/nats-io/go-nats"
)

const (
	connNum          = 100
	clientNumPerConn = 1000
	payLoadSize      = 32
)

func runClient(cid uint64) {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Printf("Failed to connect to nats server: %v.", err)
		return
	}
	var wg sync.WaitGroup
	var id uint64
	for id = cid * clientNumPerConn; id < (cid+1)*clientNumPerConn; id++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			sub, err := nc.SubscribeSync("client_" + strconv.FormatUint(id, 10))
			if err != nil {
				log.Printf("Failed to subscribe subject %s: %v.", sub.Subject, err)
				return
			}
			payload := make([]byte, payLoadSize)
			binary.LittleEndian.PutUint64(payload[:8], id)
			now := time.Now()
			encodedNow, _ := now.MarshalBinary()
			copy(payload[8:], encodedNow)
			nc.Publish("join", payload)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			// msg, err := sub.NextMsgWithContext(ctx)
			_, err = sub.NextMsgWithContext(ctx)
			if err != nil {
				log.Printf("Failed to read from nats server: %v.", err)
				return
			}
			// log.Printf("Server [%d] responsed.", binary.LittleEndian.Uint64(msg.Data[:8]))
			// do something
		}(id)
	}
	wg.Wait()
}

func main() {
	var wg sync.WaitGroup
	var id uint64
	for id = 0; id < connNum; id++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			for {
				runClient(id)
			}
		}(id)
	}
	wg.Wait()
}
