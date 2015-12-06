package beat

import (
	"github.com/robinpercy/amqpbeat/config"

	"github.com/robinpercy/amqpbeat/utils"
	"github.com/streadway/amqp"
)

/*
Event ...
*/
type Event map[string]interface{}

/*
Consumer ...
*/
type Consumer struct {
	beatChannel chan<- []Event
	decoder     func([]byte) Event
	cfg         *config.ChannelConfig
}

/*
Init ...
*/
func (c *Consumer) Init(cfg *config.ChannelConfig, decoder func([]byte) Event) {
	c.cfg = cfg
	c.decoder = decoder
}

/*
Run ...
*/
func (c *Consumer) Run(ch *amqp.Channel, events chan<- []Event) {
	_, err := ch.Consume(*c.cfg.Name, "", false, false, false, false, nil)
	utils.FailOnError(err, "Failed to register consumer")
}
