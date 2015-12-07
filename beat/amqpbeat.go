package beat

import (
	//	"bufio"

	"time"

	"github.com/elastic/libbeat/beat"
	"github.com/elastic/libbeat/cfgfile"
	"github.com/elastic/libbeat/common"
	"github.com/elastic/libbeat/publisher"
	cfg "github.com/robinpercy/amqpbeat/config"
	"github.com/robinpercy/amqpbeat/utils"
)

// Amqpbeat is a beat.Beater implementation that consumes events from one or
// more AMQP channels
type Amqpbeat struct {
	config  cfg.Settings
	events  publisher.Client
	stopped bool
	stop    chan bool
}

// Config extracts settings from the config file
func (ab *Amqpbeat) Config(b *beat.Beat) error {
	return ab.ConfigWithFile(b, "")
}

// ConfigWithFile ...
func (ab *Amqpbeat) ConfigWithFile(b *beat.Beat, path string) error {
	// Config loading goes here
	err := cfgfile.Read(&ab.config, path)
	utils.FailOnError(err, "Error reading configuration file")
	return nil
}

// Setup ...
func (ab *Amqpbeat) Setup(b *beat.Beat) error {
	ab.events = b.Events
	ab.stop = make(chan bool)

	return nil
}

// Run ...
func (ab *Amqpbeat) Run(b *beat.Beat) error {
	event := common.MapStr{
		"@timestamp": common.Time(time.Now()),
		"type":       "test",
		"payload":    "test",
	}

	go ab.events.PublishEvents([]common.MapStr{event}, nil)
	select {
	case <-ab.stop:
	}

	//fmt.Println("ab.events: %v", ab.events)
	/*
		for p := range payloads {
			var event map[string]interface{}
			err := json.Unmarshal(p, &event)
			utils.FailOnError(err, "Failed to unmarshal value")
			event["@timestamp"] = common.Time(time.Now())
			event["type"] = "openstack"
			fmt.Println(event)
			ab.events.PublishEvent(event, publisher.Sync)
		}
	*/

	return nil
}

// Cleanup ...
func (ab *Amqpbeat) Cleanup(b *beat.Beat) error {
	return nil
}

// Stop ...
func (ab *Amqpbeat) Stop() {
	if !ab.stopped {
		ab.stopped = true
		ab.stop <- true
	}

}
