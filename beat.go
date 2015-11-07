package main

import (
	//	"bufio"
	"fmt"
	"github.com/elastic/libbeat/logp"
	//	"os"
	"time"
	//	"github.com/elastic/libbeat/cfgfile"
	//	"encoding/json"
	"github.com/elastic/libbeat/beat"
	"github.com/elastic/libbeat/common"
	"github.com/elastic/libbeat/publisher"
)

type MelllvarBeat struct {
	events publisher.Client
}

func (mb *MelllvarBeat) Config(b *beat.Beat) error {
	//err := cfgfile.Read("./melllvar.cfg", "")
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
