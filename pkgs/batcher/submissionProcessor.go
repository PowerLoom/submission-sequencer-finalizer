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
	BatchID           int
	Batch             map[string][]string // ProjectID -> SubmissionKeys
}

func StartSubmissionProcessor() {
	log.Info("🚀 Submission Processor started...")

	for {
		// Fetch submission details from Redis queue
		result, err := redis.RedisClient.BRPop(context.Background(), 0, "finalizerQueue").Result()
		if err != nil {
			log.Errorf("Error fetching data from Redis queue: %v", err)
			continue
		}

		if len(result) < 2 {
			log.Println("Invalid data received from Redis queue, skipping this entry")
			continue
		}

		// Unmarshal the queue data directly into SubmissionDetails
		var submissionDetails SubmissionDetails
		err = json.Unmarshal([]byte(result[1]), &submissionDetails)
		if err != nil {
			log.Errorf("Error unmarshalling data from Redis queue: %v", err)
			continue
		}

		// Log the details of the batch being processed for the given epochID in the specified data market
		log.Infof("🔄 Processing batch %d for epoch %s in data market %s", submissionDetails.BatchID, submissionDetails.EpochID.String(), submissionDetails.DataMarketAddress)

		if !config.SettingsObj.ProcessSwitch {
			log.Println("Process switch is off, skipping processing")
			continue
		}

		// Call the method to finalize the batch submission
		finalizedBatchSubmission, err := submissionDetails.FinalizeBatch()
		if err != nil {
			errorMsg := fmt.Sprintf("Error finalizing batch %d for epoch %s in data market %s: %v", submissionDetails.BatchID, submissionDetails.EpochID.String(), submissionDetails.DataMarketAddress, err)
			clients.SendFailureNotification(pkgs.FinalizeBatches, errorMsg, time.Now().String(), "High")
			log.Errorf("Batch finalization failed: %s", errorMsg)
			continue
		}

		// Log the finalized batch submission
		log.Infof("✅ Successfully finalized batch %d for epoch %s in data market %s: %+v", submissionDetails.BatchID, submissionDetails.EpochID.String(), submissionDetails.DataMarketAddress, finalizedBatchSubmission)
	}
}
