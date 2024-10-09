package main

import (
	"submission-sequencer-finalizer/config"
	"submission-sequencer-finalizer/pkgs/batcher"
	"submission-sequencer-finalizer/pkgs/clients"
	"submission-sequencer-finalizer/pkgs/redis"
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
	clients.InitializeReportingClient(config.SettingsObj.SlackReportingUrl, 5*time.Second)

	// Initialize tx relayer service
	clients.InitializeTxClient(config.SettingsObj.TxRelayerUrl, time.Duration(config.SettingsObj.HttpTimeout)*time.Second)

	// Setup redis
	redis.RedisClient = redis.NewRedisClient()

	var wg sync.WaitGroup

	wg.Add(1)
	go batcher.StartSubmissionProcessor() // Start the submission processor
	wg.Wait()
}
