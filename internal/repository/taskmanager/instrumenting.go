package taskmanager

import (
	"context"
	"strconv"
	"time"
	"workflow/internal/models"

	"github.com/go-kit/kit/metrics"
)

// instrumentingMiddleware wraps Service and enables request metrics
type instrumentingMiddleware struct {
	reqCount    metrics.Counter
	reqDuration metrics.Histogram
	svc         Repository
}

// AddTask ...
func (s *instrumentingMiddleware) AddTask(ctx context.Context, task *models.Task) (err error) {
	defer func(startTime time.Time) {
		labels := []string{
			"method", "AddTask",
			"error", strconv.FormatBool(err != nil),
		}
		s.reqCount.With(labels...).Add(1)
		s.reqDuration.With(labels...).Observe(time.Since(startTime).Seconds())
	}(time.Now())
	return s.svc.AddTask(ctx, task)
}

// GetNextTask ...
func (s *instrumentingMiddleware) GetNextTask(ctx context.Context) (task *models.Task, err error) {
	defer func(startTime time.Time) {
		labels := []string{
			"method", "GetNextTask",
			"error", strconv.FormatBool(err != nil),
		}
		s.reqCount.With(labels...).Add(1)
		s.reqDuration.With(labels...).Observe(time.Since(startTime).Seconds())
	}(time.Now())
	return s.svc.GetNextTask(ctx)
}

// UpdateTaskStatus ...
func (s *instrumentingMiddleware) UpdateTaskStatus(ctx context.Context, taskID int64, status models.TaskStatus, result []byte, errMsg string, scheduledAt *time.Time) (err error) {
	defer func(startTime time.Time) {
		labels := []string{
			"method", "UpdateTaskStatus",
			"error", strconv.FormatBool(err != nil),
		}
		s.reqCount.With(labels...).Add(1)
		s.reqDuration.With(labels...).Observe(time.Since(startTime).Seconds())
	}(time.Now())
	return s.svc.UpdateTaskStatus(ctx, taskID, status, result, errMsg, scheduledAt)
}

// GetTasksForArchiving ...
func (s *instrumentingMiddleware) GetTasksForArchiving(ctx context.Context, statuses []models.TaskStatus, limit int) (tasks []models.Task, err error) {
	defer func(startTime time.Time) {
		labels := []string{
			"method", "GetTasksForArchiving",
			"error", strconv.FormatBool(err != nil),
		}
		s.reqCount.With(labels...).Add(1)
		s.reqDuration.With(labels...).Observe(time.Since(startTime).Seconds())
	}(time.Now())
	return s.svc.GetTasksForArchiving(ctx, statuses, limit)
}

// DeleteOldArchivedTasks ...
func (s *instrumentingMiddleware) DeleteOldArchivedTasks(ctx context.Context, olderThan time.Time) (count int64, err error) {
	defer func(startTime time.Time) {
		labels := []string{
			"method", "DeleteOldArchivedTasks",
			"error", strconv.FormatBool(err != nil),
		}
		s.reqCount.With(labels...).Add(1)
		s.reqDuration.With(labels...).Observe(time.Since(startTime).Seconds())
	}(time.Now())
	return s.svc.DeleteOldArchivedTasks(ctx, olderThan)
}

// ArchiveTasks ...
func (s *instrumentingMiddleware) ArchiveTasks(ctx context.Context, tasks ...models.Task) (err error) {
	defer func(startTime time.Time) {
		labels := []string{
			"method", "ArchiveTasks",
			"error", strconv.FormatBool(err != nil),
		}
		s.reqCount.With(labels...).Add(1)
		s.reqDuration.With(labels...).Observe(time.Since(startTime).Seconds())
	}(time.Now())
	return s.svc.ArchiveTasks(ctx, tasks...)
}

// NewInstrumentingMiddleware ...
func NewInstrumentingMiddleware(
	reqCount metrics.Counter,
	reqDuration metrics.Histogram,
	svc Repository,
) Repository {
	return &instrumentingMiddleware{
		reqCount:    reqCount,
		reqDuration: reqDuration,
		svc:         svc,
	}
}
