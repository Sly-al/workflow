package taskmanager

import (
	"context"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"math"
	"sync"
	"time"
	"workflow/internal/models"
)

var errTaskChanFull = errors.New("channel of tasks is full")

const (
	metricsNamespace = "scheduler"
	metricsSubsystem = "task_manager"
	sleepDuration    = 10 * time.Millisecond
	minWorkerRatio   = 0.5
)

// Coordinator manages the distribution of tasks to workers.
type Coordinator interface {
	AddWorker(worker Worker) error
	RemoveWorker(worker Worker)
	SubmitTask(task *models.Task) error
	Start(ctx context.Context)
	Stop() error
}

// CoordinatorConfig holds the configuration for the Coordinator.
type CoordinatorConfig struct {
	MaxWorkers          uint
	QueueSize           uint
	DispatchPeriod      time.Duration
	TaskTimeout         time.Duration
	MaxWorkersHardLimit uint          // Критический верхний предел для MaxWorkers
	LoadHighThreshold   float64       // Порог хай лоада (дефолт 0.8)
	LoadLowThreshold    float64       // Порог лоу лоада (дефолт 0.3)
	CooldownPeriod      time.Duration // Период холда между изменениями числа воркеров
}

// coordinatorMetrics holds Prometheus metrics for the Coordinator.
type coordinatorMetrics struct {
	taskProcessingDuration *prometheus.HistogramVec
	tasksProcessed         *prometheus.CounterVec
	queueSize              prometheus.Gauge
	workerPoolSize         prometheus.Gauge
}

// coordinator is the internal implementation of the Coordinator interface.
type coordinator struct {
	lastAdjustmentTime time.Time
	tasks              chan *models.Task
	workerGroup        *errgroup.Group
	metrics            *coordinatorMetrics
	workerSem          *semaphore.Weighted
	workers            []Worker
	config             CoordinatorConfig
	mu                 sync.RWMutex
}

// Worker ...
type Worker interface {
	ProcessTask(ctx context.Context, task *models.Task) error
	GetLoad() int
}

// AddWorker adds a new worker to the Coordinator.
func (c *coordinator) AddWorker(worker Worker) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if uint(len(c.workers)) >= c.config.MaxWorkers {
		c.adjustWorkerCount()
	}

	c.workers = append(c.workers, worker)
	c.metrics.workerPoolSize.Inc()
	log.WithField("total_workers", len(c.workers)).Info("New worker added")
	return nil
}

// adjustWorkerCount allows the system to adapt to the current load by increasing or decreasing the number of workers as needed.
func (c *coordinator) adjustWorkerCount() {
	c.mu.Lock()
	defer c.mu.Unlock()

	currentLoad := c.getCurrentLoad()
	currentWorkers := uint(len(c.workers))

	// Проверка холда
	if time.Since(c.lastAdjustmentTime) < c.config.CooldownPeriod {
		return
	}

	var shouldAdjust bool
	var newMaxWorkers uint

	if currentLoad > c.config.LoadHighThreshold && currentWorkers < c.config.MaxWorkersHardLimit {
		newMaxWorkers = currentWorkers + 1
		if newMaxWorkers > c.config.MaxWorkersHardLimit {
			newMaxWorkers = c.config.MaxWorkersHardLimit
		}
		shouldAdjust = true
	} else if currentLoad < c.config.LoadLowThreshold && currentWorkers > c.config.MaxWorkers/2 {
		newMaxWorkers = currentWorkers - 1
		minWorkers := uint(float64(c.config.MaxWorkers) * minWorkerRatio)
		if newMaxWorkers < minWorkers {
			newMaxWorkers = minWorkers
		}
		shouldAdjust = true
	}

	if shouldAdjust {
		c.config.MaxWorkers = newMaxWorkers
		c.workerSem = semaphore.NewWeighted(int64(newMaxWorkers))
		c.lastAdjustmentTime = time.Now()
		log.WithFields(log.Fields{
			"new_max_workers": newMaxWorkers,
			"current_load":    currentLoad,
		}).Info("Adjusted worker count")
	}
}

// getCurrentLoad ...
func (c *coordinator) getCurrentLoad() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.workers) == 0 {
		return 0
	}
	totalLoad := 0
	for _, worker := range c.workers {
		totalLoad += worker.GetLoad()
	}
	return float64(totalLoad) / float64(len(c.workers))
}

// RemoveWorker removes a worker from the Coordinator.
func (c *coordinator) RemoveWorker(worker Worker) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, w := range c.workers {
		if w == worker {
			c.workers = append(c.workers[:i], c.workers[i+1:]...)
			c.metrics.workerPoolSize.Dec()
			log.WithFields(log.Fields{
				"total_workers": len(c.workers),
			}).Info("Worker removed")
			return
		}
	}
}

// SubmitTask adds a new task to the queue.
func (c *coordinator) SubmitTask(task *models.Task) error {
	select {
	case c.tasks <- task:
		c.metrics.queueSize.Inc()
		return nil
	default:
		log.WithFields(log.Fields{
			"task_id": task.ID,
		}).Error("Task queue is full, task rejected")
		return errTaskChanFull
	}
}

// Stop gracefully shuts down the Coordinator.
func (c *coordinator) Stop() error {
	log.Info("Initiating coordinator shutdown")
	return c.workerGroup.Wait()
}

func (c *coordinator) Start(ctx context.Context) {
	log.Info("Coordinator starting")
	go c.dispatchTasks(ctx)
}

// dispatchTasks distributes tasks to available workers.
func (c *coordinator) dispatchTasks(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Info("Coordinator stopping")
			return
		default:
			if err := c.workerSem.Acquire(ctx, 1); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.WithError(err).Error("Failed to acquire semaphore")
				continue
			}

			select {
			case task := <-c.tasks:
				c.metrics.queueSize.Dec()
				worker := c.selectWorker()
				c.workerGroup.Go(func() error {
					defer c.workerSem.Release(1)
					return c.processTask(ctx, worker, task)
				})
			default:
				c.workerSem.Release(1)
				// Режим ждуна перед проверкой задач
				time.Sleep(sleepDuration)
			}
		}
	}
}

// processTask handles the processing of a single task by a worker.
func (c *coordinator) processTask(ctx context.Context, w Worker, t *models.Task) error {
	taskCtx, cancel := context.WithTimeout(ctx, c.config.TaskTimeout)
	defer cancel()

	start := time.Now()
	err := w.ProcessTask(taskCtx, t)
	duration := time.Since(start)

	status := "success"
	if err != nil {
		status = "error"
		if errors.Is(err, context.DeadlineExceeded) {
			log.WithFields(log.Fields{
				"task_id":  t.ID,
				"duration": duration,
			}).Error("Task processing timed out", err)
		} else {
			log.WithFields(log.Fields{
				"task_id":  t.ID,
				"duration": duration,
			}).Error("Failed to process task", err)
		}
	} else {
		log.WithFields(log.Fields{
			"task_id":  t.ID,
			"duration": duration,
		}).Info("Task processed successfully")
	}

	// Update metrics
	c.metrics.taskProcessingDuration.WithLabelValues(t.Type, status).Observe(duration.Seconds())
	c.metrics.tasksProcessed.WithLabelValues(t.Type, status).Inc()

	return err
}

// selectWorker chooses the worker with the lowest load.
func (c *coordinator) selectWorker() Worker {
	var selectedWorker Worker
	minLoad := math.MaxInt // Maximum int value

	for _, worker := range c.workers {
		load := worker.GetLoad()
		if load < minLoad {
			minLoad = load
			selectedWorker = worker
		}
	}

	return selectedWorker
}

// newCoordinatorMetrics initializes and registers Prometheus metrics.
func newCoordinatorMetrics() *coordinatorMetrics {
	metrics := &coordinatorMetrics{
		taskProcessingDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: metricsNamespace,
				Subsystem: metricsSubsystem,
				Name:      "task_processing_duration_seconds",
				Help:      "Duration of task processing in seconds",
			},
			[]string{"task_type", "status"},
		),
		tasksProcessed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: metricsNamespace,
				Subsystem: metricsSubsystem,
				Name:      "tasks_processed_total",
				Help:      "Total number of processed tasks",
			},
			[]string{"task_type", "status"},
		),
		queueSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "task_queue_size",
			Help:      "Current number of tasks in the queue",
		}),
		workerPoolSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "worker_pool_size",
			Help:      "Current size of the worker pool",
		}),
	}

	// Register metrics with Prometheus
	prometheus.MustRegister(
		metrics.taskProcessingDuration,
		metrics.tasksProcessed,
		metrics.queueSize,
		metrics.workerPoolSize,
	)

	return metrics
}

// NewCoordinator creates a new Coordinator instance.
func NewCoordinator(config CoordinatorConfig) Coordinator {
	return &coordinator{
		config:             config,
		workers:            make([]Worker, 0, config.MaxWorkers),
		tasks:              make(chan *models.Task, config.QueueSize),
		metrics:            newCoordinatorMetrics(),
		workerGroup:        &errgroup.Group{},
		workerSem:          semaphore.NewWeighted(int64(config.MaxWorkers)),
		lastAdjustmentTime: time.Now(),
	}
}
