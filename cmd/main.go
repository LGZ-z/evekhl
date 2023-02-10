package main

import "github.com/lgzzzz/evekhl/pkg/EVE"

func main() {

	config, err := ReadYamlConfig("./config.yaml")
	if err != nil {
		panic(err)
	}

	e := EVE.NewEveBot(config.Token, &EVE.Option{WelcomeMessage: config.WelcomeMessage})
	e.Run()
}
