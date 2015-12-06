package config

import "fmt"
import "strings"

// ChannelConfig ...
type ChannelConfig struct {
	Name              *string
	Required          *bool
	MaxBatchSize      *int
	MaxIntervalMS     *int
	MinIntervalMS     *int
	MaxMessagesPerSec *int
}

// AmqpConfig ...
type AmqpConfig struct {
	ServerURI *string
	Channels  *[]ChannelConfig
}

// Settings ...
type Settings struct {
	AmqpInput *AmqpConfig
}

/*
CheckRequired ...
*/
func (s *Settings) CheckRequired() ConfigError {
	errors := make(errorMap)
	if s.AmqpInput == nil {
		errors.missing("amqpinput")
		return ErrorFor(errors)
	}

	input := s.AmqpInput
	if input.Channels == nil || len(*input.Channels) == 0 {
		errors.missing("channels")
	} else {
		for _, c := range *input.Channels {
			c.CheckRequired(errors)
		}
	}

	return ErrorFor(errors)
}

func (c *ChannelConfig) CheckRequired(errors errorMap) errorMap {
	if c == nil {
		return errors
	}

	if c.Name == nil || strings.Trim(*c.Name, " ") == "" {
		errors["channel.name"] = "All channels require a name attribute"
	}
	return errors

}

type errorMap map[string]string

func (e errorMap) missing(field string) {
	e[field] = fmt.Sprintf("%s is required in config", field)
}
