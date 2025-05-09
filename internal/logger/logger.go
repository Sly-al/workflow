package logger

import (
	log "github.com/sirupsen/logrus"
	"os"
)

func Init() {
	log.SetFormatter(&log.JSONFormatter{
		PrettyPrint: true,
	})
	log.SetReportCaller(true)
	log.SetLevel(log.Level(5))

	log.SetOutput(os.Stdout)
}
