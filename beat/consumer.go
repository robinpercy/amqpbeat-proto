package beat

import (
	"time"

	"github.com/elastic/libbeat/common"
	"github.com/robinpercy/amqpbeat/config"

	"github.com/robinpercy/amqpbeat/utils"
	"github.com/streadway/amqp"
)

/*
Consumer ...
*/
type Consumer struct {
	beatChannel chan<- []common.MapStr
	decoder     func([]byte) common.MapStr
	cfg         *config.ChannelConfig
}

/*
Init ...
*/
func (c *Consumer) Init(cfg *config.ChannelConfig, decoder func([]byte) common.MapStr) {
	c.cfg = cfg
	c.decoder = decoder
}

/*
Run ...
*/
func (c *Consumer) Run(ch *amqp.Channel) chan []common.MapStr {
	events := make(chan []common.MapStr)
	// TODO: make exchange part of config
	deliveries, err := ch.Consume("test", *c.cfg.Name, false, false, false, false, nil)
	utils.FailOnError(err, "Failed to register consumer")
	go c.consume(deliveries, events)
	return events
}

func (c *Consumer) consume(deliveries <-chan amqp.Delivery, events chan []common.MapStr) {
	// TODO this is where batching and throttling will happen
	for d := range deliveries {
		e := c.decoder(d.Body)
		e["@timestamp"] = time.Now()
		e["type"] = "amqp" //TODO: this should be be dynamic and/or configurable
		events <- []common.MapStr{e}
	}
}
