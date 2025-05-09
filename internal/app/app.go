package app

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
	"os"
	"os/signal"
	"syscall"
	"workflow/internal/config"
	"workflow/internal/handlers"
	"workflow/internal/logger"
	taskmanagerRepo "workflow/internal/repository/taskmanager"
	"workflow/internal/service/example"
	"workflow/internal/taskmanager"

	"github.com/fasthttp/router"
	kitprometheus "github.com/go-kit/kit/metrics/prometheus"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

var (
	methodErrorDB = []string{"method", "error"}
)

type App struct {
	cfg *config.Config
}

func New(cfg *config.Config) App {
	return App{cfg: cfg}
}

func (app *App) Run() {
	ctx, cancelProcesses := context.WithCancel(context.Background())
	defer cancelProcesses()

	logger.Init()

	db := app.initDB(ctx)
	defer db.Close()

	dbReqCount := kitprometheus.NewCounterFrom(
		prometheus.CounterOpts{
			Namespace: app.cfg.Metrics.Namespace,
			Subsystem: app.cfg.Metrics.Subsystem,
			Name:      "db_request_count",
			Help:      "db request count",
		}, methodErrorDB,
	)
	dbReqDuration := kitprometheus.NewSummaryFrom(
		prometheus.SummaryOpts{
			Namespace: app.cfg.Metrics.Namespace,
			Subsystem: app.cfg.Metrics.Subsystem,
			Name:      "db_request_duration",
			Help:      "db request duration",
		},
		methodErrorDB,
	)

	taskManagerRepo := taskmanagerRepo.NewRepository(db)
	taskManagerRepo = taskmanagerRepo.NewInstrumentingMiddleware(dbReqCount, dbReqDuration, taskManagerRepo)

	taskManagerConfig := taskmanager.Config{
		RetryConfig:    taskmanager.DefaultRetryConfig(),
		ArchiveConfig:  taskmanager.DefaultArchiveConfig(),
		DefaultTimeout: taskmanager.DefaultTimeout(),
		MaxWorkers:     taskmanager.DefaultMaxWorkers(),
		QueueSize:      taskmanager.DefaultQueueSize(),
		DispatchPeriod: taskmanager.DefaultDispatchPeriod(),
	}

	taskManager, err := taskmanager.NewTaskManager(taskManagerRepo, taskManagerConfig)
	if err != nil {
		log.WithError(err).Error("Failed to create TaskManager")
		return
	}

	exampleService := example.NewExampleSvc()

	handlers.RegisterAllHandlers(
		taskManager,
		exampleService,
	)

	// Start TaskManager
	go func() {
		if startErr := taskManager.Start(context.Background()); startErr != nil {
			log.WithError(err).Error("TaskManager stopped with error")
		}
	}()

	metricsRouter := router.New()
	metricsRouter.GET("/metrics", fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler()))
	metricsServer := &fasthttp.Server{
		Handler:            metricsRouter.Handler,
		MaxRequestBodySize: app.cfg.System.ReadBufferSize,
		ReadTimeout:        app.cfg.System.ReadTimeout,
		ReadBufferSize:     app.cfg.System.ReadBufferSize,
	}

	go func() {
		log.WithFields(log.Fields{
			"port": app.cfg.Metrics.Port,
		}).Info("starting metrics server")
		if err = metricsServer.ListenAndServe(":" + app.cfg.Metrics.Port); err != nil {
			log.WithError(err).Error("metrics server run failure")
			return
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	defer func(sig os.Signal) {
		log.WithFields(log.Fields{
			"signal": sig.String(),
		}).Info("received signal, exiting")

		taskManager.Stop()

		_ = metricsServer.Shutdown()
		log.Info("goodbye")
	}(<-c)
}

func (app *App) initDB(ctx context.Context) *pgxpool.Pool {
	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		app.cfg.DB.UserName, app.cfg.DB.Password, app.cfg.DB.Address(), app.cfg.DB.DataBase)

	dbpool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("Unable to create connection pool: %v\n", err)
	}

	return dbpool
}
