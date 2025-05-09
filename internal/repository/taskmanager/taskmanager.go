package taskmanager

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"time"
	"workflow/internal/models"
)

// const ...
const (
	TaskStatusPending    = models.TaskStatusPending
	TaskStatusProcessing = models.TaskStatusProcessing
)

// Repository defines the interface for task storage operations.
// @gtg mp-metrics
type Repository interface {
	AddTask(ctx context.Context, task *models.Task) (err error)
	GetNextTask(ctx context.Context) (task *models.Task, err error)
	UpdateTaskStatus(ctx context.Context, taskID int64, status models.TaskStatus, result []byte, errMsg string, scheduledAt *time.Time) (err error)
	GetTasksForArchiving(ctx context.Context, statuses []models.TaskStatus, limit int) (tasks []models.Task, err error)
	DeleteOldArchivedTasks(ctx context.Context, olderThan time.Time) (count int64, err error)
	ArchiveTasks(ctx context.Context, tasks ...models.Task) (err error)
}

type repository struct {
	db *pgxpool.Pool
}

// AddTask ...
func (r *repository) AddTask(ctx context.Context, task *models.Task) error {
	query := `
        INSERT INTO tasks.outbox_pool 
        (type, priority, status, scheduled_at, data, max_attempts, timeout)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, created_at, updated_at
    `
	err := r.db.QueryRow(ctx, query,
		task.Type, task.Priority, TaskStatusPending, task.ScheduledAt, task.Data, task.MaxAttempts, task.Timeout,
	).Scan(&task.ID, &task.CreatedAt, &task.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to add task: %w", err)
	}
	return nil
}

// GetNextTask ...
func (r *repository) GetNextTask(ctx context.Context) (*models.Task, error) {
	query := `
        UPDATE tasks.outbox_pool
        SET status = $1, updated_at = NOW(), attempts = attempts + 1
        WHERE id = (
            SELECT id FROM tasks.outbox_pool
            WHERE status = $2
            AND (scheduled_at IS NULL OR scheduled_at <= NOW())
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id, type, priority, status, created_at, updated_at, scheduled_at, data, result, error, attempts, max_attempts, timeout
    `

	var task models.Task
	var scheduledAt sql.NullTime
	ErrNoTasks := errors.New("no tasks available")

	err := r.db.QueryRow(ctx, query, TaskStatusProcessing, TaskStatusPending).Scan(
		&task.ID, &task.Type, &task.Priority, &task.Status, &task.CreatedAt, &task.UpdatedAt,
		&scheduledAt, &task.Data, &task.Result, &task.Error, &task.Attempts, &task.MaxAttempts, &task.Timeout,
	)

	if err == sql.ErrNoRows {
		return nil, ErrNoTasks
	} else if err != nil {
		return nil, fmt.Errorf("failed to get next task: %w", err)
	}

	if scheduledAt.Valid {
		task.ScheduledAt = &scheduledAt.Time
	}

	return &task, nil
}

// UpdateTaskStatus ...
func (r *repository) UpdateTaskStatus(ctx context.Context, taskID int64, status models.TaskStatus, result []byte, errMsg string, scheduledAt *time.Time) error {
	query := `
        UPDATE tasks.outbox_pool
        SET status = $1, result = $2, error = $3, updated_at = NOW(), scheduled_at = $5
        WHERE id = $4
    `

	_, err := r.db.Exec(ctx, query, status, result, errMsg, taskID, scheduledAt)
	if err != nil {
		return fmt.Errorf("failed to update task status: %w", err)
	}

	return nil
}

// GetTasksForArchiving ...
func (r *repository) GetTasksForArchiving(ctx context.Context, statuses []models.TaskStatus, limit int) ([]models.Task, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			log.Errorf("failed to rollback transaction %v", rollbackErr)
		}
	}()

	query := `
        UPDATE tasks.outbox_pool
        SET status = 'archiving'
        WHERE id IN (
            SELECT id
            FROM tasks.outbox_pool
            WHERE status = ANY($1)
            ORDER BY updated_at ASC
            LIMIT $2
            FOR NO KEY UPDATE
        )
        RETURNING id, type, priority, status, created_at, updated_at, scheduled_at, data, result, error, attempts, max_attempts, timeout
    `

	rows, err := tx.Query(ctx, query, statuses, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query tasks for archiving: %w", err)
	}
	defer rows.Close()

	var tasks []models.Task
	for rows.Next() {
		var task models.Task
		var scheduledAt sql.NullTime
		if scanErr := rows.Scan(&task.ID, &task.Type, &task.Priority, &task.Status, &task.CreatedAt, &task.UpdatedAt,
			&scheduledAt, &task.Data, &task.Result, &task.Error, &task.Attempts, &task.MaxAttempts, &task.Timeout); scanErr != nil {
			return nil, fmt.Errorf("failed to scan task: %w", scanErr)
		}
		if scheduledAt.Valid {
			task.ScheduledAt = &scheduledAt.Time
		}
		tasks = append(tasks, task)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return tasks, nil
}

// ArchiveTasks ...
func (r *repository) ArchiveTasks(ctx context.Context, tasks ...models.Task) error {
	if len(tasks) == 0 {
		return nil
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	tasksJSON := make([]map[string]interface{}, len(tasks))
	for i := range tasks {
		task := &tasks[i]
		tasksJSON[i] = map[string]interface{}{
			"id":           task.ID,
			"type":         task.Type,
			"priority":     task.Priority,
			"status":       task.Status,
			"created_at":   task.CreatedAt,
			"updated_at":   task.UpdatedAt,
			"scheduled_at": task.ScheduledAt,
			"data":         task.Data,
			"result":       task.Result,
			"error":        task.Error,
			"attempts":     task.Attempts,
			"max_attempts": task.MaxAttempts,
			"timeout":      task.Timeout,
		}
	}

	jsonData, err := json.Marshal(tasksJSON)
	if err != nil {
		return fmt.Errorf("failed to marshal tasks to JSON: %w", err)
	}

	insertQuery := `
        INSERT INTO tasks.outbox_pool_archive 
        (id, type, priority, status, created_at, updated_at, scheduled_at, data, result, error, attempts, max_attempts, timeout)
        SELECT id, type, priority, status, created_at, updated_at, scheduled_at, data, result, error, attempts, max_attempts, timeout
        FROM jsonb_to_recordset($1::jsonb) AS x(
            id bigint, type text, priority int, status text, created_at timestamp, updated_at timestamp, 
            scheduled_at timestamp, data jsonb, result jsonb, error text, attempts int, max_attempts int, timeout interval
        )
    `
	_, err = tx.Exec(ctx, insertQuery, jsonData)
	if err != nil {
		return fmt.Errorf("failed to insert tasks into archive: %w", err)
	}

	deleteQuery := `
        DELETE FROM tasks.outbox_pool
        USING jsonb_to_recordset($1::jsonb) AS x(id bigint)
        WHERE utp.id = x.id AND utp.status = 'archiving'
    `
	_, err = tx.Exec(ctx, deleteQuery, jsonData)
	if err != nil {
		return fmt.Errorf("failed to delete tasks from pool: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// DeleteOldArchivedTasks ...
func (r *repository) DeleteOldArchivedTasks(ctx context.Context, olderThan time.Time) (int64, error) {
	query := `
        DELETE FROM tasks.outbox_pool_archive
        WHERE updated_at < $1
    `
	result, err := r.db.Exec(ctx, query, olderThan)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old archived tasks: %w", err)
	}
	return result.RowsAffected(), nil
}

// NewRepository creates a new instance of the task repository.
func NewRepository(db *pgxpool.Pool) Repository {
	return &repository{db: db}
}
