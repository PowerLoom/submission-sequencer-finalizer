package main

import (
	"context"
	"submission-sequencer-finalizer/config"
	"submission-sequencer-finalizer/pkgs/batcher"
	"submission-sequencer-finalizer/pkgs/clients"
	"submission-sequencer-finalizer/pkgs/ipfs"
	"submission-sequencer-finalizer/pkgs/prost"
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

	// Connect to IPFS node
	ipfs.ConnectIPFSNode()

	// Setup blockchain client
	ctx := context.Background()
	if err := prost.ConfigureClient(ctx); err != nil {
		panic(err)
	}

	// Add cleanup for RPC helper
	defer func() {
		if prost.RPCHelper != nil {
			prost.RPCHelper.Close()
		}
	}()

	// Setup contract instance
	if err := prost.ConfigureContractInstance(); err != nil {
		panic(err)
	}

	// Load contract state variables
	prost.LoadContractStateVariables()

	// Load lua script
	prost.LoadLuaScript()

	var wg sync.WaitGroup

	// Start the submission processor
	wg.Add(1)
	go func() {
		defer wg.Done()
		batcher.StartSubmissionProcessor()
	}()

	// Wait for all goroutines to complete
	wg.Wait()
}
