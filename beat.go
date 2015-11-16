package main

import (
	//	"bufio"
	"encoding/json"
	"fmt"
	"log"

	"os"
	"time"

	"github.com/elastic/libbeat/logp"
	//	"github.com/elastic/libbeat/cfgfile"
	//	"encoding/json"
	"github.com/elastic/libbeat/beat"
	"github.com/elastic/libbeat/common"
	"github.com/elastic/libbeat/publisher"
	"github.com/streadway/amqp"
)

type MelllvarBeat struct {
	events publisher.Client
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func startConsumingAmqp(uri string, qName string, payloads chan<- string) {
	conn, err := amqp.Dial(uri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer conn.Close()

	q, err := ch.Consume(
		qName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register consumer")

	// TODO: remove along with Nack
	firstID := 0
	for m := range q {
		if firstID > 10 {
			break
		} else {
			firstID++
		}

		payloads <- string(m.Body)
		m.Ack(false)
		// TODO: remove
		//		m.Nack(false, true)
	}
	close(payloads)
	fmt.Println("Finished consuming")
}

func (mb *MelllvarBeat) Config(b *beat.Beat) error {
	// Config loading goes here
	//err := cfgfile.Read("/etc/Mellvar/Melllvar.cfg", "")
	//if err != nil {
	//		logp.Err("Error reading configuration file: %v", err)
	//		return err
	//	}

	logp.Debug("melllvar", " is configured")
	return nil
}

func (mb *MelllvarBeat) Setup(b *beat.Beat) error {
	mb.events = b.Events
	logp.Debug("melllvar", " is setup")
	return nil
}

func (mb *MelllvarBeat) Run(b *beat.Beat) error {
	args := os.Args
	payloads := make(chan string)
	go startConsumingAmqp(args[1], args[2], payloads)

	for p := range payloads {
		var event map[string]interface{}
		err := json.Unmarshal([]byte(p), &event)
		failOnError(err, "Failed to unmarshal value")
		event["@timestamp"] = common.Time(time.Now())
		event["type"] = "openstack"
		fmt.Println(event)
		mb.events.PublishEvent(event, publisher.Sync)
	}

	/*
		for i := 0; i < 1; i++ {
			//var event map[string]interface{}
			//scanned := []byte(scanner.Text())
			//err := json.Unmarshal(scanned, &event)
			//event["@timestamp"] = common.Time(time.Now())
			//event["type"] = "proc"
			//check(err)
			event2 := common.MapStr{
				"@timestamp": common.Time(time.Now()),
				"type":       "proc",
				"proc": common.MapStr{
					"foo": "bar",
				},
			}
			mb.events.PublishEvent(event2)
			fmt.Println("Sent one", event2)
			//			txt, err := json.Marshal(js)
			//			check(err)
			//fmt.Println(string(txt))

		}

		time.Sleep(2000 * time.Millisecond)
	*/

	return nil
}

func (mb *MelllvarBeat) Cleanup(b *beat.Beat) error {
	return nil
}

func (mb *MelllvarBeat) Stop() {
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	mb := &MelllvarBeat{}
	b := beat.NewBeat("Melllvar", "0.1", mb)
	b.CommandLineSetup()
	b.LoadConfig()
	mb.Config(b)
	b.Run()
}
