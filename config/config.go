package config

import "fmt"

// ChannelConfig ...
type ChannelConfig struct {
	Name *string
}

// AmqpConfig ...
type AmqpConfig struct {
	Channels *[]ChannelConfig
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
		errors["amqpinput"] = "amqpinput section is required in config"
		return ErrorFor(errors)
	}

	input := s.AmqpInput
	if input.Channels == nil {
		errors.missing("channels")
	}

	return ErrorFor(errors)
}

type errorMap map[string]string

func (e errorMap) missing(field string) {
	e[field] = fmt.Sprintf("%s section is required in config", field)
}
