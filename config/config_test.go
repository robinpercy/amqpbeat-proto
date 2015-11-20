package config

import (
	"testing"

	"github.com/elastic/libbeat/cfgfile"
)

func assertErrorFor(t *testing.T, e ConfigError, key string) {
	if _, ok := e.ErrorMap[key]; !ok {
		t.Errorf("%s should be reported as missing", key)
	}
}

func TestMissingInputSectionError(t *testing.T) {
	var settings Settings
	cfgfile.Read(&settings, "")
	err := settings.CheckRequired()
	assertErrorFor(t, err, "amqpinput")
}

func TestEmptyFileShouldComplain(t *testing.T) {
	var settings Settings
	cfgfile.Read(&settings, "./test_data/empty.yaml")
	err := settings.CheckRequired()

	if _, ok := err.ErrorMap["amqpinput"]; ok {
		t.Errorf("amqp_input should not be reported as missing")
	}
	assertErrorFor(t, err, "channels")
}
