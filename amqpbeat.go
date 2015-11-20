package main

import (
	//	"bufio"
	"encoding/json"
	"fmt"

	"os"
	"time"

	"github.com/elastic/libbeat/cfgfile"
	"github.com/elastic/libbeat/logp"
	cfg "github.com/robinpercy/amqpbeat/config"
	"github.com/robinpercy/amqpbeat/utils"
	//	"github.com/elastic/libbeat/cfgfile"
	//	"encoding/json"
	"github.com/elastic/libbeat/beat"
	"github.com/elastic/libbeat/common"
	"github.com/elastic/libbeat/publisher"
	"github.com/streadway/amqp"
)

// Amqpbeat is a beat.Beater implementation that consumes events from one or
// more AMQP channels
type Amqpbeat struct {
	events publisher.Client
	config cfg.Settings
}

func startConsuming(uri string, qName string, payloads chan<- []byte) {

	conn, err := amqp.Dial(uri)
	utils.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	utils.FailOnError(err, "Failed to open a channel")
	defer conn.Close()

	q, err := ch.Consume(qName, "", false, false, false, false, nil)
	utils.FailOnError(err, "Failed to register consumer")

	// TODO: dryrun value to config
	dryrun := true
	maxMsgs := 10
	count := 1
	for m := range q {
		payloads <- m.Body
		if dryrun == true {
			m.Nack(false, true)
			count++
			if count >= maxMsgs {
				break
			}
		} else {
			m.Ack(false)
		}
	}
	close(payloads)
	fmt.Println("Finished consuming")
}

// Config extracts settings from the config file
func (mb *Amqpbeat) Config(b *beat.Beat) error {

	// Config loading goes here
	err := cfgfile.Read(&mb.config, "")
	utils.FailOnError(err, "Error reading configuration file")
	//if err != nil {
	//		logp.Err("Error reading configuration file: %v", err)
	//		return err
	//	}

	logp.Debug("amq", " is configured")
	return nil
}

// Setup ...
func (mb *Amqpbeat) Setup(b *beat.Beat) error {
	mb.events = b.Events
	logp.Debug("ampqbeat", " is setup")
	return nil
}

// Run ...
func (mb *Amqpbeat) Run(b *beat.Beat) error {
	args := os.Args
	payloads := make(chan []byte)
	fmt.Println(mb.config.AmqpInput)
	fmt.Println((*mb.config.AmqpInput.Channels)[0])
	fmt.Println((*mb.config.AmqpInput.Channels)[0].Name)
	go startConsuming(args[1], *(*mb.config.AmqpInput.Channels)[0].Name, payloads)

	for p := range payloads {
		var event map[string]interface{}
		err := json.Unmarshal(p, &event)
		utils.FailOnError(err, "Failed to unmarshal value")
		event["@timestamp"] = common.Time(time.Now())
		event["type"] = "openstack"
		fmt.Println(event)
		mb.events.PublishEvent(event, publisher.Sync)
	}

	return nil
}

// Cleanup ...
func (mb *Amqpbeat) Cleanup(b *beat.Beat) error {
	return nil
}

// Stop ...
func (mb *Amqpbeat) Stop() {
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	mb := &Amqpbeat{}
	b := beat.NewBeat("amqpbeat", "0.1", mb)
	b.CommandLineSetup()
	b.LoadConfig()
	mb.Config(b)
	b.Run()
}
