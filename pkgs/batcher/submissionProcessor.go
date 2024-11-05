package batcher

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"submission-sequencer-finalizer/config"
	"submission-sequencer-finalizer/pkgs"
	"submission-sequencer-finalizer/pkgs/clients"
	"submission-sequencer-finalizer/pkgs/redis"
	"time"

	log "github.com/sirupsen/logrus"
)

type SubmissionDetails struct {
	DataMarketAddress string
	EpochID           *big.Int
	Batch             map[string][]string // ProjectID -> SubmissionKeys
}

func StartSubmissionProcessor() {
	log.Println("Submission Processor started")

	for {
		// Fetch submission details from Redis queue
		result, err := redis.RedisClient.BRPop(context.Background(), 0, "finalizerQueue").Result()
		if err != nil {
			log.Println("Error fetching from Redis queue: ", err)
			continue
		}

		if len(result) < 2 {
			log.Println("Invalid data fetched from Redis queue")
			continue
		}

		// Unmarshal the queue data directly into SubmissionDetails
		var submissionDetails SubmissionDetails
		err = json.Unmarshal([]byte(result[1]), &submissionDetails)
		if err != nil {
			log.Println("Error unmarshalling queue data: ", err)
			continue
		}

		// Log the details of the batch being processed for the given epochID in the specified data market
		log.Printf("🔄 Processing batch %v of epoch ID %s for data market: %s", submissionDetails.Batch, submissionDetails.EpochID.String(), submissionDetails.DataMarketAddress)

		if !config.SettingsObj.ProcessSwitch {
			log.Println("Process switch is off, skipping processing")
			continue
		}

		// Call the method to finalize the batch submission
		finalizedBatchSubmission, err := submissionDetails.FinalizeBatch()
		if err != nil {
			errorMsg := fmt.Sprintf("Error finalizing batch %v of epochID %s for data market address %s: %v", submissionDetails.Batch, submissionDetails.EpochID.String(), submissionDetails.DataMarketAddress, err)
			clients.SendFailureNotification(pkgs.FinalizeBatches, errorMsg, time.Now().String(), "High")
			log.Errorf("Batch finalization error: %s", errorMsg)
			continue
		}

		// Log the finalized batch submission
		log.Debugf("✅ Finalized batch submission for epochID %s in data market address %s: %v", submissionDetails.EpochID.String(), submissionDetails.DataMarketAddress, finalizedBatchSubmission)
	}
}
