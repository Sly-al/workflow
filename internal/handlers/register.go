package handlers

import (
	"workflow/internal/models"
	"workflow/internal/service/example"
)

type TaskManagerRegistrar interface {
	RegisterHandlers(handlers map[string]models.TaskHandler)
}

// RegisterAllHandlers ...
func RegisterAllHandlers(
	tm TaskManagerRegistrar,
	exampleSvc *example.Svc,
) {
	handlers := map[string]models.TaskHandler{
		"example": NewExampleHandler(exampleSvc),
	}
	tm.RegisterHandlers(handlers)
}
