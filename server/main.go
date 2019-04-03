package main

import (
	"encoding/binary"
	"log"
	"strconv"
	"sync"
	"time"

	nats "github.com/nats-io/go-nats"
)

const (
	workerNum           = 2
	channelNumPerWorker = 100
	payLoadSize         = 512
)

type worker struct {
	id      uint64
	in, out *nats.Conn
}

func createWorker(id uint64) (*worker, error) {
	in, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		return nil, err
	}
	out, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		return nil, err
	}
	return &worker{
		id:  id,
		in:  in,
		out: out,
	}, nil
}

func (w *worker) run() {
	var id uint64
	for id = w.id * channelNumPerWorker; id < (w.id+1)*channelNumPerWorker; id++ {
		w.in.QueueSubscribe("join", "channels", w.callback(id))
	}
	<-time.NewTimer(time.Hour).C
}

func (w *worker) callback(cid uint64) func(m *nats.Msg) {
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
		// do something
		binary.LittleEndian.PutUint64(payload[:8], cid)
		w.out.Publish("client_"+strconv.FormatUint(clientID, 10), payload)
	}
}

func main() {
	var wg sync.WaitGroup
	var i uint64
	for i = 0; i < workerNum; i++ {
		w, err := createWorker(i)
		if err != nil {
			log.Fatalf("Failed to create worker: %v.", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.run()
		}()
	}
	log.Printf("Ready.")
	wg.Wait()
}
