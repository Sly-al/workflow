package main

import (
	"github.com/go-playground/validator/v10"
	_ "github.com/go-playground/validator/v10"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"

	"workflow/internal/app"
	"workflow/internal/config"
)

func main() {
	var cfg config.Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		log.Fatalf("Fail parsing env config %v", err)
	}

	validate := validator.New(validator.WithRequiredStructEnabled())
	if err = validate.Struct(cfg); err != nil {
		log.Fatalf("Fail config validation %v", err)
	}

	application := app.New(&cfg)
	application.Run()
}
