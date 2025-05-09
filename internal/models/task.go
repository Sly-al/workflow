package models

import (
	"context"
	"encoding/json"
	"time"
)

// TaskStatus represents the current status of a task.
type TaskStatus string

// const ...
const (
	TaskStatusPending           TaskStatus = "pending"
	TaskStatusProcessing        TaskStatus = "processing"
	TaskStatusCompleted         TaskStatus = "completed"
	TaskStatusFailed            TaskStatus = "failed"
	TaskStatusPermanentlyFailed TaskStatus = "permanently_failed"
	TaskStatusTimedOut          TaskStatus = "timed_out"
	TaskStatusManualProcessing  TaskStatus = "manual_processing"
)

// Task represents a schedulable unit of work.
type Task struct {
	UpdatedAt   time.Time       `json:"updated_at"`
	CreatedAt   time.Time       `json:"created_at"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
	Status      TaskStatus      `json:"status"`
	Type        string          `json:"type"`
	Error       string          `json:"error,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	Data        json.RawMessage `json:"data"`
	Timeout     time.Duration   `json:"timeout"`
	ID          int64           `json:"id"`
	Priority    int             `json:"priority"`
	Attempts    uint            `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
}

// TaskHandler defines the interface for handling tasks.
type TaskHandler interface {
	HandleTask(ctx context.Context, task *Task) (json.RawMessage, error)
	ValidateTask(task *Task) error
}

// TaskHandlerWithRetry ...
type TaskHandlerWithRetry interface {
	TaskHandler
	MaxAttempts() uint
}
