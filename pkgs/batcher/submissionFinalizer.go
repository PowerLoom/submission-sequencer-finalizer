package batcher

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"submission-sequencer-finalizer/config"
	"submission-sequencer-finalizer/pkgs"
	"submission-sequencer-finalizer/pkgs/ipfs"
	"submission-sequencer-finalizer/pkgs/merkle"
	"submission-sequencer-finalizer/pkgs/redis"
	"submission-sequencer-finalizer/pkgs/service"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
)

type SubmissionDetails struct {
	EpochID    *big.Int
	BatchID    int
	ProjectMap map[string][]string // ProjectID -> SubmissionKeys
}

func StartSubmissionFinalizer() {
	log.Println("Submission Finalizer started")

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
		submissionDetails.BuildBatchSubmissions()
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
		service.SendFailureNotification(pkgs.BuildBatchSubmissions, errorMsg, time.Now().String(), "High")
		log.Errorf("Batch finalization error: %s", errorMsg)
		return nil, err
	}

	// Step 3: Log and return the finalized batch submissions
	log.Debugf("Successfully finalized %d batch submissions for epoch %s", len(finalizedBatches), s.EpochID.String())

	return finalizedBatches, nil
}

func (s *SubmissionDetails) finalizeBatches(batches []map[string][]string) ([]*ipfs.BatchSubmission, error) {
	// Slice to store the processed batch submissions
	finalizedBatchSubmissions := make([]*ipfs.BatchSubmission, 0)

	// Channel to collect finalized batch submissions
	finalizedBatchChan := make(chan *ipfs.BatchSubmission)

	// WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Iterate over each batch of keys
	for _, batch := range batches {
		wg.Add(1)
		go func(batch map[string][]string) {
			defer wg.Done()

			log.Debugln("Processing batch: ", batch)

			// Initialize slices to store submission IDs and submission data
			submissionIDs := []string{}
			submissionData := []string{}

			// Maps to track the most frequent CIDs for each project and frequency count of submissions
			projectMostFrequentCID := make(map[string]string)
			projectCIDFrequencies := make(map[string]map[string]int)

			// Process each key in the current batch
			for projectID, submissionKeys := range batch {
				// Iterate over the submission keys
				for _, submissionKey := range submissionKeys {
					// Fetch the value associated with the key from Redis
					submissionValue, err := redis.Get(context.Background(), submissionKey)
					if err != nil {
						log.Errorln("Error fetching data from Redis: ", err.Error())
						continue
					}

					log.Debugln(fmt.Sprintf("Processing key %s with value %s", submissionKey, submissionValue))

					// Skip the key if the value has expired or is empty
					if len(submissionValue) == 0 {
						log.Errorln("Expired value for key: ", submissionKey)
						continue
					}

					// Split the submission value into ID and submission data parts
					submissionDataParts := strings.Split(submissionValue, ".")
					if len(submissionDataParts) != 2 {
						log.Errorln("Invalid value format: ", submissionValue)
						continue
					}

					// Parse the submission data using the SnapshotSubmission structure
					submissionDetails := pkgs.SnapshotSubmission{}
					err = protojson.Unmarshal([]byte(submissionDataParts[1]), &submissionDetails)
					if err != nil {
						log.Errorln("Error unmarshalling submission data: ", err)
						continue
					}

					// Retrieve the snapshot CID from the submission data
					snapshotCID := submissionDetails.Request.SnapshotCid

					// Initialize the frequency map for the project if not already present
					if projectCIDFrequencies[projectID] == nil {
						projectCIDFrequencies[projectID] = make(map[string]int)
					}

					// Increment the frequency of this snapshot CID for the current project
					projectCIDFrequencies[projectID][snapshotCID] += 1

					// Update the most frequent snapshot CID for the project
					if projectCIDFrequencies[projectID][snapshotCID] > projectCIDFrequencies[projectID][projectMostFrequentCID[projectID]] {
						projectMostFrequentCID[projectID] = snapshotCID
					}

					// Add the submission ID and data to their respective lists
					submissionIDs = append(submissionIDs, submissionDataParts[0])
					submissionData = append(submissionData, submissionDataParts[1])
				}
			}

			// Prepare the list of project IDs and their corresponding most frequent CIDs
			projectIDList := []string{}
			mostFrequentCIDList := []string{}
			for projectID := range projectMostFrequentCID {
				projectIDList = append(projectIDList, projectID)
				mostFrequentCIDList = append(mostFrequentCIDList, projectMostFrequentCID[projectID])
			}

			log.Debugln("Finalizing PIDs and CIDs for epoch: ", s.EpochID, projectIDList, mostFrequentCIDList)

			// Build the Merkle tree for the current batch and generate the IPFS BatchSubmission
			batchSubmission, err := merkle.BuildMerkleTree(submissionIDs, submissionData, s.BatchID, s.EpochID, projectIDList, mostFrequentCIDList)
			if err != nil {
				log.Errorln("Error building batch: ", err)
				return
			}

			// Send the batch submission to the channel
			finalizedBatchChan <- batchSubmission

			log.Debugf("CID: %s Batch: %d", batchSubmission.Cid, s.BatchID)

			// Increment the BatchID for the next batch
			s.BatchID++
		}(batch)
	}

	// Start a goroutine to wait for all goroutines to finish and then close the channel
	go func() {
		wg.Wait()
		close(finalizedBatchChan)
	}()

	// Collect results from the channel
	for batchSubmission := range finalizedBatchChan {
		finalizedBatchSubmissions = append(finalizedBatchSubmissions, batchSubmission)
	}

	// Return the finalized batch submissions
	return finalizedBatchSubmissions, nil
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
