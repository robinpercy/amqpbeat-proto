package beat

import (
	//	"bufio"

	"github.com/elastic/libbeat/beat"
	"github.com/elastic/libbeat/cfgfile"
	"github.com/elastic/libbeat/logp"
	"github.com/elastic/libbeat/publisher"
	cfg "github.com/robinpercy/amqpbeat/config"
	"github.com/robinpercy/amqpbeat/utils"
)

// Amqpbeat is a beat.Beater implementation that consumes events from one or
// more AMQP channels
type Amqpbeat struct {
	events publisher.Client
	config cfg.Settings
	stop   chan bool
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
	logp.Debug("amq", " is configured")
	return nil
}

// Setup ...
func (ab *Amqpbeat) Setup(b *beat.Beat) error {
	ab.events = b.Events
	ab.stop = make(chan bool)
	logp.Debug("ampqbeat", " is setup")
	return nil
}

// Run ...
func (ab *Amqpbeat) Run(b *beat.Beat) error {
	for range ab.stop {
		break
	}
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
	ab.stop <- true
}
