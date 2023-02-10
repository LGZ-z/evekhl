package main

import (
	"gopkg.in/yaml.v2"
	"os"
)

func ReadYamlConfig(path string) (*Config, error) {
	conf := &Config{}
	if f, err := os.Open(path); err != nil {
		return nil, err
	} else {
		err = yaml.NewDecoder(f).Decode(conf)
		if err != nil {
			return nil, err
		}
	}
	return conf, nil
}

type Config struct {
	Token          string `yaml:"token"`
	WelcomeMessage string `yaml:"welcome_message"`
}
