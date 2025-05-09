package example

import (
	"context"
	log "github.com/sirupsen/logrus"
	"workflow/internal/models"
)

type Svc struct{}

func (s *Svc) HandleExampleTask(_ context.Context, data models.Example) (models.ExampleResult, error) {
	log.Infof("Got example message %s", data.Message)

	return models.ExampleResult{
		ID: data.ID,
	}, nil
}

func NewExampleSvc() *Svc {
	return &Svc{}
}
