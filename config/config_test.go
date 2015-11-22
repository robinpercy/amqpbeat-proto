package config

import (
	"fmt"
	"testing"

	"github.com/elastic/libbeat/cfgfile"
)

func TestMissingInputSectionError(t *testing.T) {
	var settings Settings
	cfgfile.Read(&settings, "")
	err := settings.CheckRequired()
	assertErrorFor(t, err, "amqpinput")
}

func TestComplainsWhenChannelsMissing(t *testing.T) {
	err := checkFile("./test_data/missing_channels.yml")
	if len(err.ErrorMap) != 1 {
		t.Errorf("Expected exactly one error, got %d", len(err.ErrorMap))
	}
	fmt.Printf("%+v", err)
}

func TestComplainsWhenChannelNameMissing(t *testing.T) {
	err := checkFile("./test_data/missing_channel_name.yml")
	assertErrorFor(t, err, "channel.name")
}

func TestDefaults(t *testing.T) {
	var settings Settings
	cfgfile.Read(&settings, "./test_data/minimal.yml")
}

func TestNilChannelValidationShortCircuits(t *testing.T) {
	var c *ChannelConfig
	errors := make(errorMap)
	c.CheckRequired(errors)
	if len(errors) > 0 {
		t.Error("No errors expected")
	}
}

func TestConfigErrorMapGivesEmptyString(t *testing.T) {
	tests := []ConfigError{ConfigError{ErrorMap: make(errorMap)}, ConfigError{ErrorMap: nil}}
	for _, err := range tests {
		if err.Error() != "" {
			t.Errorf("Expected empty string, got '%s'", err.Error())
		}
	}
}

func assertErrorFor(t *testing.T, e ConfigError, key string) {
	if _, ok := e.ErrorMap[key]; !ok {
		t.Errorf("%s should be reported as missing", key)
	}
}

func loadFile(fileName string) *Settings {
	var settings Settings
	cfgfile.Read(&settings, fileName)
	return &settings
}

func checkFile(fileName string) ConfigError {
	settings := loadFile(fileName)
	return settings.CheckRequired()
}
