package handlers

import (
	"context"
	"encoding/json"
	"errors"
	log "github.com/sirupsen/logrus"
	"time"
	"workflow/internal/models"
	"workflow/internal/service/example"
)

type ExampleHandler interface {
	HandleTask(ctx context.Context, task *models.Task) (json.RawMessage, error)
	ValidateTask(task *models.Task) error
}

type exampleHandler struct {
	exampleSvc *example.Svc
}

func (h *exampleHandler) HandleTask(ctx context.Context, task *models.Task) (json.RawMessage, error) {
	startTime := time.Now()
	log.WithFields(log.Fields{
		"task_id": task.ID,
	}).Info("Starting example task")

	var data models.Example
	if err := json.Unmarshal(task.Data, &data); err != nil {
		return nil, errors.New("failed to unmarshal task data")
	}

	result, err := h.processExample(ctx, data)
	if err != nil {
		return nil, errors.New("failed to process example task")
	}

	result.Duration = time.Since(startTime).String()

	log.WithFields(log.Fields{
		"task_id":  task.ID,
		"duration": result.Duration,
	}).Info("Completed example task")

	return json.Marshal(result)
}

func (h *exampleHandler) ValidateTask(task *models.Task) error {
	var data models.Example
	if err := json.Unmarshal(task.Data, &data); err != nil {
		return errors.New("failed to unmarshal task data")
	}

	if data.Message == "" {
		return errors.New("message is required and must be non-zero")
	}

	return nil
}

func (h *exampleHandler) processExample(ctx context.Context, data models.Example) (models.ExampleResult, error) {
	result, err := h.exampleSvc.HandleExampleTask(ctx, data)
	if err != nil {
		return models.ExampleResult{}, errors.New("exampleService failed")
	}

	return result, nil
}

func NewExampleHandler(exampleSvc *example.Svc) ExampleHandler {
	return &exampleHandler{
		exampleSvc: exampleSvc,
	}
}
