package beat

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/elastic/libbeat/beat"
	"github.com/elastic/libbeat/common"
	"github.com/elastic/libbeat/publisher"
	"github.com/robinpercy/amqpbeat/utils"
	"github.com/streadway/amqp"
)

type MockClient struct {
	beat            *Amqpbeat
	eventPublished  func(event common.MapStr, beat *Amqpbeat)
	eventsPublished func(event []common.MapStr, beat *Amqpbeat)
	lastReceived    time.Time
}

func (c MockClient) PublishEvent(event common.MapStr, opts ...publisher.ClientOption) bool {
	c.eventPublished(event, c.beat)
	c.lastReceived = time.Now()
	return true
}

func (c MockClient) PublishEvents(events []common.MapStr, opts ...publisher.ClientOption) bool {
	c.eventsPublished(events, c.beat)
	c.lastReceived = time.Now()
	return true
}

func firehosePublisher(exch string, routingKey string, ch *amqp.Channel) {
	for i := 0; i < 1000; i++ {
		err := (*ch).Publish(exch, routingKey, false, true, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(fmt.Sprintf("{\"test\": %d}", i)),
		})
		utils.FailOnError(err, "Failed to send message")
	}
}

func newBeat(cfgFile string, client *MockClient) (*Amqpbeat, *beat.Beat) {
	wd, err := os.Getwd()
	utils.FailOnError(err, "Could not determine working directory")

	ab := &Amqpbeat{}
	b := beat.NewBeat("amqpbeat", "0.0.0", ab)
	ab.ConfigWithFile(b, fmt.Sprintf("%s/../test/config/%s", wd, cfgFile))

	if client == nil {
		client = &MockClient{
			eventsPublished: func(event []common.MapStr, beat *Amqpbeat) {
			},
		}
	}

	client.beat = ab
	b.Events = client
	ab.Setup(b)

	return ab, b
}

func runBeatAndWait(t *testing.T, cfgFile string, dur time.Duration,
	client *MockClient) *Amqpbeat {

	completed := make(chan bool)
	killed := make(chan bool)

	ab, b := newBeat(cfgFile, client)

	// Wait duration then send kill message
	time.AfterFunc(dur, func() {
		killed <- true
		close(killed)
	})

	// run to completion, then send completed message
	go func() {
		ab.Run(b)
		completed <- true
		close(completed)
	}()

	// Block until job either completes or is killed
	select {
	case <-completed:
	case <-killed:
		t.Error("Stopping beat since timeout exceeded ", dur)
		ab.Stop()
	}

	return ab
}

func TestCanStartAndStopBeat(t *testing.T) {
	ab, b := newBeat("throttle_test.yml", nil)

	stopped := make(chan bool)
	killed := make(chan bool)
	time.AfterFunc(5*time.Second, func() {
		killed <- true
		close(killed)
	})

	time.AfterFunc(500*time.Millisecond, func() {
		ab.Stop()
	})

	go func() {
		ab.Run(b)
		stopped <- true
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-killed:
		t.Error("Failed to stop beat in test. Ctrl+C may be necessary..")
	}
}

func TestCanReceiveMessage(t *testing.T) {
	conn, ch := amqpConnect()
	defer conn.Close()
	defer ch.Close()

	payload := "This is a test"

	p := newPublisher("", "test", ch)
	p.send(ch, payload)

	received := false
	client := &MockClient{
		eventsPublished: func(events []common.MapStr, ab *Amqpbeat) {
			received = true
			if events[0]["payload"] != payload {
				t.Errorf("Expected payload: '%s' but received '%s'",
					payload, events[0]["payload"])
			}
			ab.Stop()
		},
	}
	runBeatAndWait(t, "throttle_test.yml", 10*time.Second, client)

	if !received {
		t.Errorf("Expected a message but did not receive one")
	}
}

func newPublisher(exch string, routingKey string, ch *amqp.Channel) *Publisher {
	_, err := ch.QueueDeclare(routingKey, false, true, false, false, nil)
	utils.FailOnError(err, fmt.Sprintf("Failed to declare queue %s", routingKey))
	return &Publisher{exch: exch, routingKey: routingKey, ch: ch}
}

func amqpConnect() (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	utils.FailOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	utils.FailOnError(err, "Failed to open a channel")

	return conn, ch
}

type Publisher struct {
	ch         *amqp.Channel
	exch       string
	routingKey string
}

func (p *Publisher) send(ch *amqp.Channel, msg string) {
	err := (*ch).Publish(p.exch, p.routingKey, false, true, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(msg),
	})
	utils.FailOnError(err, "Failed to publish message")
}

/*
func TestMessagesDontExceedBatchSize(t *testing.T) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	utils.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	utils.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	qName := "TestThrottle"

	_, err = ch.QueueDeclare("TestThrottle", false, true, false, false, nil)
	utils.FailOnError(err, "Failed to declare queyue")

	exch := ""
	go firehosePublisher(exch, qName, ch)

	mb := &Amqpbeat{}
	b := beat.NewBeat("amqpbeat", "0.0.1", mb)

	batchesReceived := 0
	b.Events = MockClient{
		eventPublished: func(event common.MapStr) {
		},
		eventsPublished: func(events []common.MapStr) {
			batchesReceived++
			if len(events) != 100 {
				t.Errorf("Received batch size of %d", len(events))
			}
			if batchesReceived >= 10 {
				mb.Stop()
			}
		},
	}

	wd, err := os.Getwd()
	utils.FailOnError(err, "Could not determine working directory")
	mb.ConfigWithFile(b, fmt.Sprintf("%s/../test/config/throttle_test.yml", wd))
	mb.Setup(b)
	mb.Run(b)
	mb.Cleanup(b)
}
*/

/*
Tests:
- Will not send max batch until min Interval exceeded
- Messages will not exceed max batchSize
- Will send max batch before max Interval exceeded
- Will not send partial batch until max Interval exceeded
- Will send partial batch when max Interval exceeded
- Will send max batch as soon as possible after min Interval exceeded (before max interval)
*/

func setupQueues() {
}
