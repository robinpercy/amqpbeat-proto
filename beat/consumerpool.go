package beat

import (
	"encoding/json"
	"reflect"

	"github.com/elastic/libbeat/common"
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

func newConsumerPool(cfg *config.AmqpConfig) *ConsumerPool {
	cp := new(ConsumerPool)
	cp.init(cfg)
	return cp
}

func (cp *ConsumerPool) init(cfg *config.AmqpConfig) {
	cp.ServerURI = *cfg.ServerURI
	cp.Consumers = make([]*Consumer, len(*cfg.Channels))

	for i, chConfig := range *cfg.Channels {
		c := new(Consumer)
		c.Init(&chConfig, func(b []byte) common.MapStr {
			e := new(common.MapStr)
			err := json.Unmarshal(b, e)
			utils.FailOnError(err, "Failed to marshal event")
			return *e
		})
		cp.Consumers[i] = c
	}
}

func (cp *ConsumerPool) run() chan []common.MapStr {
	conn, err := amqp.Dial(cp.ServerURI)
	utils.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	utils.FailOnError(err, "Failed to open a channel")
	defer conn.Close()

	events := make(chan []common.MapStr)
	selCases := make([]reflect.SelectCase, len(cp.Consumers))

	for i, c := range cp.Consumers {
		consumerChan := c.Run(ch)

		selCases[i].Dir = reflect.SelectRecv
		selCases[i].Chan = reflect.ValueOf(consumerChan)
	}

	go func() {
		for {
			_, recv, recvOK := reflect.Select(selCases)
			if recvOK {
				events <- recv.Interface().([]common.MapStr)
			}
		}
	}()

	return events
}
