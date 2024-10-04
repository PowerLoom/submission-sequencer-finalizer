package main

import (
	"submission-sequencer-finalizer/config"
	"submission-sequencer-finalizer/pkgs/batcher"
	"submission-sequencer-finalizer/pkgs/redis"
	"submission-sequencer-finalizer/pkgs/service"
	"submission-sequencer-finalizer/pkgs/utils"
	"sync"
	"time"
)

func main() {
	// Initiate logger
	utils.InitLogger()

	// Load the config object
	config.LoadConfig()

	// Initialize reporting service
	service.InitializeReportingClient(config.SettingsObj.SlackReportingUrl, 5*time.Second)

	// Setup redis
	redis.RedisClient = redis.NewRedisClient()

	var wg sync.WaitGroup

	wg.Add(1)
	go batcher.StartSubmissionFinalizer() // Start the submission finalizer
	wg.Wait()
}
