package batcher

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"submission-sequencer-finalizer/config"
	"submission-sequencer-finalizer/pkgs"
	"submission-sequencer-finalizer/pkgs/clients"
	"submission-sequencer-finalizer/pkgs/ipfs"
	"submission-sequencer-finalizer/pkgs/redis"
	"time"

	log "github.com/sirupsen/logrus"
)

type SubmissionDetails struct {
	EpochID    *big.Int
	BatchID    int
	ProjectMap map[string][]string // ProjectID -> SubmissionKeys
}

func StartSubmissionProcessor() {
	log.Println("Submission Processor started")

	for {
		// Fetch submission details from Redis queue
		result, err := redis.RedisClient.BRPop(context.Background(), 0, "batchQueue").Result()
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

		// Log the details of the batch being processed
		log.Printf("Processing batch: %d with epoch ID: %s\n", submissionDetails.BatchID, submissionDetails.EpochID.String())

		// Call the method to build and finalize the batch submissions
		if _, err := submissionDetails.BuildBatchSubmissions(); err != nil {
			log.Printf("Error building batch submissions for batch %d: %v\n", submissionDetails.BatchID, err)
			continue
		}
	}
}

// BuildBatchSubmissions organizes project submission keys into batches and finalizes them for processing.
func (s *SubmissionDetails) BuildBatchSubmissions() ([]*ipfs.BatchSubmission, error) {
	// Step 1: Organize the projectMap into batches of submission keys
	batchedSubmissionKeys := arrangeSubmissionKeysInBatches(s.ProjectMap)
	log.Debugf("Arranged %d batches of submission keys for processing: %v", len(batchedSubmissionKeys), batchedSubmissionKeys)

	// Step 2: Finalize the batch submissions based on the EpochID and batched submission keys
	finalizedBatches, err := s.finalizeBatches(batchedSubmissionKeys)
	if err != nil {
		errorMsg := fmt.Sprintf("Error finalizing batches for epoch %s: %v", s.EpochID.String(), err)
		clients.SendFailureNotification(pkgs.BuildBatchSubmissions, errorMsg, time.Now().String(), "High")
		log.Errorf("Batch finalization error: %s", errorMsg)
		return nil, err
	}

	// Step 3: Log and return the finalized batch submissions
	log.Debugf("Successfully finalized %d batch submissions for epoch %s", len(finalizedBatches), s.EpochID.String())

	return finalizedBatches, nil
}

func arrangeSubmissionKeysInBatches(projectMap map[string][]string) []map[string][]string {
	var (
		batchSize = config.SettingsObj.BatchSize              // Maximum number of batches
		batches   = make([]map[string][]string, 0, batchSize) // Preallocate slice with capacity of batchSize
		count     = 0                                         // Keeps track of the number of batches
	)

	// Iterate over each project's submission keys
	for projectID, submissionKeys := range projectMap {
		if count >= batchSize { // Stop if batchSize is reached
			break
		}

		// Create a new batch for the current project
		batch := make(map[string][]string)
		batch[projectID] = submissionKeys

		// Add the batch to the list
		batches = append(batches, batch)

		count++
	}

	return batches
}
