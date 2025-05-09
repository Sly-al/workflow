package taskmanager

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
	"workflow/internal/models"
)

// const ...
const (
	maxRetryAttempts       = uint(5)    // Количество попыток повтора
	jitterMultiplier       = 2          // Множитель для случайных задержек
	randomFactorDefault    = 0.5        // Стандартный коэффициент случайности
	defaultBatchSize       = uint(1000) // Размер пакета по умолчанию
	defaultTimeoutMinutes  = 5
	defaultWorkerCount     = 10
	defaultTaskQueueSize   = uint(1000)
	defaultDispatchMillis  = 100
	retryBackoffMultiplier = 2
	maxJitterDivisor       = 2
)

// Repository ...
type Repository interface {
	AddTask(ctx context.Context, task *models.Task) error
	GetNextTask(ctx context.Context) (*models.Task, error)
	UpdateTaskStatus(ctx context.Context, taskID int64, status models.TaskStatus, result []byte, errMsg string, scheduledAt *time.Time) error
	GetTasksForArchiving(ctx context.Context, statuses []models.TaskStatus, limit int) ([]models.Task, error)
	DeleteOldArchivedTasks(ctx context.Context, olderThan time.Time) (int64, error)
	ArchiveTasks(ctx context.Context, tasks ...models.Task) error
}

// TaskManager представляет публичный интерфейс для управления задачами
type TaskManager interface {
	RegisterHandler(taskType string, handler models.TaskHandler)
	RegisterHandlers(handlers map[string]models.TaskHandler)
	GetNextTask(ctx context.Context) (*models.Task, error)
	ProcessTask(ctx context.Context, task *models.Task) error
	UpdateTaskStatus(ctx context.Context, taskID int64, status models.TaskStatus, result json.RawMessage, errMsg string, scheduledAt *time.Time) error
	Start(ctx context.Context) error
	GetLoad() int
	Stop()
}

// Config ...
type Config struct {
	CriticalTaskTypes         map[string]struct{}
	ArchiveConfig             ArchiveConfig
	RetryConfig               RetryConfig
	DefaultTimeout            time.Duration
	DispatchPeriod            time.Duration
	RetryBackoffBase          time.Duration
	MaxRetryBackoff           time.Duration
	MaxWorkers                uint
	QueueSize                 uint
	ManualProcessingQueueSize uint
}

// taskManager ...
type taskManager struct {
	repo                  Repository
	coordinator           Coordinator
	handlers              map[string]models.TaskHandler
	manualProcessingQueue chan *models.Task
	missingHandlerCounter *prometheus.CounterVec
	stopChan              chan struct{}
	config                Config
	mu                    sync.RWMutex
	load                  atomic.Int64
}

// RetryConfig ...
type RetryConfig struct {
	MaxAttempts     uint
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	RandomFactor    float64
}

// ArchiveConfig ...
type ArchiveConfig struct {
	ArchiveStatusesMap map[models.TaskStatus]struct{}
	ArchiveStatuses    []models.TaskStatus
	RetentionPeriod    time.Duration
	ArchiveInterval    time.Duration
	BatchSize          int
}

// RegisterHandler ...
func (tm *taskManager) RegisterHandler(taskType string, handler models.TaskHandler) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.handlers[taskType] = handler
}

// GetNextTask ...
func (tm *taskManager) GetNextTask(ctx context.Context) (*models.Task, error) {
	return tm.repo.GetNextTask(ctx)
}

// ProcessTask ...
func (tm *taskManager) ProcessTask(ctx context.Context, task *models.Task) error {
	tm.mu.RLock()
	handler, ok := tm.handlers[task.Type]
	tm.mu.RUnlock()

	if !ok {
		tm.missingHandlerCounter.WithLabelValues(task.Type).Inc()
		log.WithFields(log.Fields{
			"task_type": task.Type,
			"task_id":   task.ID,
		}).Error("No handler registered for task type")
		return tm.moveToManualProcessing(ctx, task)
	}

	tm.load.Add(1)
	defer tm.load.Add(-1)

	if err := handler.ValidateTask(task); err != nil {
		return tm.moveToManualProcessing(ctx, task)
	}

	maxAttempts := tm.config.RetryConfig.MaxAttempts
	if handlerWithRetry, ok := handler.(models.TaskHandlerWithRetry); ok {
		maxAttempts = handlerWithRetry.MaxAttempts()
	}

	for attempt := uint(0); attempt < maxAttempts || tm.isCriticalTask(task.Type); attempt++ {
		taskCtx, cancel := context.WithTimeout(ctx, tm.getTimeout(task))
		result, err := handler.HandleTask(taskCtx, task)
		cancel()

		if err == nil {
			return tm.finalizeTask(ctx, task, result)
		}

		if errors.Is(err, context.DeadlineExceeded) {
			log.WithFields(log.Fields{
				"task_id":   task.ID,
				"task_type": task.Type,
				"attempt":   attempt + 1,
			}).Error("Task execution timed out")
		} else {
			log.WithFields(log.Fields{
				"task_id":   task.ID,
				"task_type": task.Type,
				"attempt":   attempt + 1,
				"error":     err,
			}).Error("Task failed, scheduling retry")
		}

		if !tm.isCriticalTask(task.Type) && attempt+1 >= maxAttempts {
			break
		}

		nextAttempt := time.Now().Add(tm.calculateBackoff(attempt))
		if updateErr := tm.UpdateTaskStatus(ctx, task.ID, models.TaskStatusPending, nil, err.Error(), &nextAttempt); updateErr != nil {
			log.WithError(updateErr).Error("Failed to update task status for retry")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Until(nextAttempt)):
			// Продолжаем следующую попытку
		}
	}

	return tm.moveToManualProcessing(ctx, task)
}

// UpdateTaskStatus ...
func (tm *taskManager) UpdateTaskStatus(ctx context.Context, taskID int64, status models.TaskStatus, result json.RawMessage, errMsg string, scheduledAt *time.Time) error {
	return tm.repo.UpdateTaskStatus(ctx, taskID, status, result, errMsg, scheduledAt)
}

// Start ...
func (tm *taskManager) Start(ctx context.Context) error {
	go tm.coordinator.Start(ctx) // Теперь передаем контекст
	go tm.startArchiveCleaner(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tm.stopChan:
			return nil
		default:
			task, err := tm.GetNextTask(ctx)
			if err != nil {
				log.WithField("error", err).Error("Failed to get next task")
				time.Sleep(tm.config.DefaultTimeout)
				continue
			}

			if task == nil {
				time.Sleep(tm.config.DefaultTimeout)
				continue
			}

			if err = tm.coordinator.SubmitTask(task); errors.Is(err, errTaskChanFull) {
				nextAttempt := time.Now().Add(time.Minute)
				if updateErr := tm.UpdateTaskStatus(ctx, task.ID, models.TaskStatusPending, nil, err.Error(), &nextAttempt); updateErr != nil {
					log.WithError(updateErr).Error("Failed to update task status for retry")
				}
			}
		}
	}
}

// GetLoad ...
func (tm *taskManager) GetLoad() int {
	return int(tm.load.Load())
}

// Stop ...
func (tm *taskManager) Stop() {
	close(tm.stopChan)
	if err := tm.coordinator.Stop(); err != nil {
		log.WithError(err).Error("Error stopping coordinator")
	}
}

// isCriticalTask ...
func (tm *taskManager) isCriticalTask(taskType string) bool {
	_, ok := tm.config.CriticalTaskTypes[taskType]
	return ok
}

// getTimeout ...
func (tm *taskManager) getTimeout(task *models.Task) time.Duration {
	if task.Timeout > 0 {
		return task.Timeout
	}
	return tm.config.DefaultTimeout
}

// calculateBackoff ...
func (tm *taskManager) calculateBackoff(attempt uint) time.Duration {
	backoff := tm.config.RetryBackoffBase * time.Duration(math.Pow(retryBackoffMultiplier, float64(attempt)))
	if backoff > tm.config.MaxRetryBackoff {
		backoff = tm.config.MaxRetryBackoff
	}

	maxJitter := int64(backoff / maxJitterDivisor)
	jitterBig, err := rand.Int(rand.Reader, big.NewInt(maxJitter))
	if err != nil {
		log.WithError(err).Error("Failed to generate jitter, using backoff without jitter")
		return backoff
	}
	jitter := time.Duration(jitterBig.Int64())

	return backoff + jitter
}

// moveToManualProcessing ...
func (tm *taskManager) moveToManualProcessing(ctx context.Context, task *models.Task) error {
	select {
	case tm.manualProcessingQueue <- task:
		log.WithFields(log.Fields{
			"task_id":   task.ID,
			"task_type": task.Type,
		}).Error("Task moved to manual processing queue")

		return tm.UpdateTaskStatus(ctx, task.ID, models.TaskStatusManualProcessing, nil, "Moved to manual processing", nil)
	default:
		log.WithFields(log.Fields{
			"task_id":   task.ID,
			"task_type": task.Type,
		}).Error("Manual processing queue is full, task rejected")
		return tm.UpdateTaskStatus(ctx, task.ID, models.TaskStatusPermanentlyFailed, nil, "Manual processing queue is full", nil)
	}
}

// startArchiveCleaner ...
func (tm *taskManager) startArchiveCleaner(ctx context.Context) {
	ticker := time.NewTicker(tm.config.ArchiveConfig.ArchiveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := tm.runArchiveCleaner(ctx); err != nil {
				log.WithError(err).Error("Failed to run archive cleaner")
			}
		}
	}
}

// runArchiveCleaner ...
func (tm *taskManager) runArchiveCleaner(ctx context.Context) error {
	if err := tm.archiveCompletedTasks(ctx); err != nil {
		return fmt.Errorf("failed to archive completed tasks: %w", err)
	}

	if err := tm.cleanOldArchivedTasks(ctx); err != nil {
		return fmt.Errorf("failed to clean old archived tasks: %w", err)
	}

	return nil
}

// archiveCompletedTasks ...
func (tm *taskManager) archiveCompletedTasks(ctx context.Context) error {
	tasks, err := tm.repo.GetTasksForArchiving(ctx, tm.config.ArchiveConfig.ArchiveStatuses, tm.config.ArchiveConfig.BatchSize)
	if err != nil {
		return fmt.Errorf("failed to get tasks for archiving: %w", err)
	}

	if len(tasks) == 0 {
		log.Info("No tasks to archive")
		return nil
	}

	err = tm.repo.ArchiveTasks(ctx, tasks...)
	if err != nil {
		return fmt.Errorf("failed to archive tasks: %w", err)
	}

	log.WithField("count", len(tasks)).Info("Archived completed tasks")
	return nil
}

// cleanOldArchivedTasks ...
func (tm *taskManager) cleanOldArchivedTasks(ctx context.Context) error {
	deletedCount, err := tm.repo.DeleteOldArchivedTasks(ctx, time.Now().Add(-tm.config.ArchiveConfig.RetentionPeriod))
	if err != nil {
		return fmt.Errorf("failed to clean old archived tasks: %w", err)
	}

	log.WithField("count", deletedCount).Info("Cleaned old archived tasks")
	return nil
}

// moveTaskToArchive ...
func (tm *taskManager) moveTaskToArchive(ctx context.Context, task *models.Task) error {
	if !tm.shouldArchiveTask(task) {
		return nil
	}

	err := tm.repo.ArchiveTasks(ctx, *task)
	if err != nil {
		return fmt.Errorf("failed to move task to archive: %w", err)
	}

	log.WithFields(log.Fields{
		"task_id":   task.ID,
		"task_type": task.Type,
	}).Info("Task archived successfully")
	return nil
}

// shouldArchiveTask ...
func (tm *taskManager) shouldArchiveTask(task *models.Task) bool {
	_, ok := tm.config.ArchiveConfig.ArchiveStatusesMap[task.Status]
	return ok
}

// finalizeTask ...
func (tm *taskManager) finalizeTask(ctx context.Context, task *models.Task, result json.RawMessage) error {
	if err := tm.UpdateTaskStatus(ctx, task.ID, models.TaskStatusCompleted, result, "", nil); err != nil {
		return err
	}
	return tm.moveTaskToArchive(ctx, task)
}

// RegisterHandlers ...
func (tm *taskManager) RegisterHandlers(handlers map[string]models.TaskHandler) {
	for taskType, handler := range handlers {
		tm.RegisterHandler(taskType, handler)
	}
}

// DefaultRetryConfig ...
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:     maxRetryAttempts,
		InitialInterval: time.Second,
		MaxInterval:     time.Minute,
		Multiplier:      jitterMultiplier,
		RandomFactor:    randomFactorDefault,
	}
}

// DefaultArchiveConfig ...
func DefaultArchiveConfig() ArchiveConfig {
	statuses := []models.TaskStatus{models.TaskStatusCompleted, models.TaskStatusPermanentlyFailed}
	statusesMap := make(map[models.TaskStatus]struct{}, len(statuses))
	for _, status := range statuses {
		statusesMap[status] = struct{}{}
	}
	return ArchiveConfig{
		ArchiveInterval:    time.Hour,
		RetentionPeriod:    30 * 24 * time.Hour,
		BatchSize:          int(defaultBatchSize),
		ArchiveStatuses:    statuses,
		ArchiveStatusesMap: statusesMap,
	}
}

// DefaultTimeout ...
func DefaultTimeout() time.Duration {
	return defaultTimeoutMinutes * time.Minute
}

// DefaultMaxWorkers ...
func DefaultMaxWorkers() uint {
	return defaultWorkerCount
}

// DefaultQueueSize ...
func DefaultQueueSize() uint {
	return defaultTaskQueueSize
}

// DefaultDispatchPeriod ...
func DefaultDispatchPeriod() time.Duration {
	return defaultDispatchMillis * time.Millisecond
}

// NextBackoff ...
func (rc RetryConfig) NextBackoff(attempt uint) time.Duration {
	// защита от дурака
	if rc.InitialInterval <= 0 || rc.MaxInterval <= 0 || rc.Multiplier <= 1 || rc.RandomFactor < 0 || rc.RandomFactor > 1 {
		return rc.InitialInterval // юзаем начальный интервал в случае некорректной конфигурации
	}

	interval := float64(rc.InitialInterval) * math.Pow(rc.Multiplier, float64(attempt))
	if interval > float64(rc.MaxInterval) {
		interval = float64(rc.MaxInterval)
	}

	maxJitter := rc.RandomFactor * interval
	var jitter float64
	if maxJitter > 0 {
		jitterBig, err := rand.Int(rand.Reader, big.NewInt(int64(maxJitter)))
		if err != nil {
			// В случае ошибки генерации случайного числа, юзаем базовый интервал
			return time.Duration(interval)
		}
		jitter = float64(jitterBig.Int64())
	}
	return time.Duration(interval + jitter)
}

// NewTaskManager ...
func NewTaskManager(repo Repository, config Config) (TaskManager, error) {
	coordConfig := CoordinatorConfig{
		MaxWorkers:     config.MaxWorkers,
		QueueSize:      config.QueueSize,
		DispatchPeriod: config.DispatchPeriod,
		TaskTimeout:    config.DefaultTimeout,
	}

	coordinator := NewCoordinator(coordConfig)
	missingHandlerCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "task_manager_missing_handler_total",
			Help: "The total number of tasks that couldn't be processed due to missing handler",
		},
		[]string{"task_type"},
	)

	if err := prometheus.Register(missingHandlerCounter); err != nil {
		return nil, fmt.Errorf("failed to register missing handler counter: %w", err)
	}

	tm := &taskManager{
		repo:                  repo,
		handlers:              make(map[string]models.TaskHandler),
		config:                config,
		load:                  atomic.Int64{},
		stopChan:              make(chan struct{}),
		coordinator:           coordinator,
		missingHandlerCounter: missingHandlerCounter,
		manualProcessingQueue: make(chan *models.Task, config.ManualProcessingQueueSize),
	}

	if config.MaxWorkers == 0 {
		return nil, errors.New("MaxWorkers must be greater than 0")
	}
	if config.QueueSize == 0 {
		return nil, errors.New("QueueSize must be greater than 0")
	}
	if config.DefaultTimeout <= 0 {
		return nil, errors.New("DefaultTimeout must be greater than 0")
	}

	if err := coordinator.AddWorker(tm); err != nil {
		return nil, fmt.Errorf("failed to add worker to coordinator: %w", err)
	}

	return tm, nil
}
