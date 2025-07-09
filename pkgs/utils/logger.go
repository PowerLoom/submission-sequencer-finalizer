package utils

import (
	"os"

	log "github.com/sirupsen/logrus"
)

func InitLogger() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		ForceColors:   true,
	})
	log.SetOutput(os.Stdout)
}
