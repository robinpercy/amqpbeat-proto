package beat

import (
	"encoding/json"

	"github.com/robinpercy/amqpbeat/config"

	"github.com/robinpercy/amqpbeat/utils"
	"github.com/streadway/amqp"
)

/*
ConsumerPool ...
*/
type ConsumerPool struct {
	ServerURI string
	Consumers []*Consumer
}

func (cp *ConsumerPool) init(cfg *config.AmqpConfig) {
	cp.ServerURI = *cfg.ServerURI
	cp.Consumers = make([]*Consumer, len(*cfg.Channels))
	for i, chConfig := range *cfg.Channels {
		c := new(Consumer)
		c.Init(&chConfig, func(b []byte) Event {
			e := new(Event)
			err := json.Unmarshal(b, e)
			utils.FailOnError(err, "Failed to marshal event")
			return *e
		})
		cp.Consumers[i] = c
	}
}

func (cp *ConsumerPool) run() {
	conn, err := amqp.Dial(cp.ServerURI)
	utils.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	utils.FailOnError(err, "Failed to open a channel")
	defer conn.Close()

	events := make(chan []Event)
	for _, c := range cp.Consumers {
		c.Run(ch, events)
	}
}
