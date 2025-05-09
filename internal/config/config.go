package config

import (
	"fmt"
	"time"
)

type DB struct {
	Host string `envconfig:"DB_HOST" validate:"required"`
	Port uint64 `envconfig:"DB_PORT" validate:"required"`

	UserName string `envconfig:"DB_USER_NAME" validate:"required"`
	Password string `envconfig:"DB_PASSWORD" validate:"required"`
	DataBase string `envconfig:"DB_NAME" validate:"required"`
}

// Metrics ...
type Metrics struct {
	Port      string `envconfig:"METRICS_PORT" default:"9090"`
	Namespace string `envconfig:"METRICS_NAMESPACE" default:"system"`
	Subsystem string `envconfig:"METRICS_SUBSYSTEM" default:"workflow"`
}

type System struct {
	ReadTimeout       time.Duration `envconfig:"READ_TIMEOUT" default:"300s"`
	ReadBufferSize    int           `envconfig:"READ_BUFFER_SIZE" default:"16384"`
	DefaultClientName string        `envconfig:"DEFAULT_CLIENT_NAME" required:"true"`
}

func (d DB) Address() string {
	return fmt.Sprintf("%s:%d", d.Host, d.Port)
}

type Config struct {
	DB      DB
	Metrics Metrics
	System  System
}
