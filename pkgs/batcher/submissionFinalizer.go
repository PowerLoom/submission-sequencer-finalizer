package batcher

import (
	"context"
	"fmt"
	"strings"
	"submission-sequencer-finalizer/config"
	"submission-sequencer-finalizer/pkgs"
	"submission-sequencer-finalizer/pkgs/clients"
	"submission-sequencer-finalizer/pkgs/ipfs"
	"submission-sequencer-finalizer/pkgs/merkle"
	"submission-sequencer-finalizer/pkgs/prost"
	"submission-sequencer-finalizer/pkgs/redis"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
)

func (s *SubmissionDetails) finalizeBatches(batches []map[string][]string) ([]*ipfs.BatchSubmission, error) {
	// Slice to store the processed batch submissions
	finalizedBatchSubmissions := make([]*ipfs.BatchSubmission, 0)

	// WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Mutex to protect shared resources and ensure safe concurrent access
	var mu sync.Mutex

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

			// Map to store snapshotter identity (key) and snapshot CID for each submission
			submissionSnapshotCIDMap := make(map[string]string)

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

					log.Debugf("Processing key %s with value %s", submissionKey, submissionValue)

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

					// Store the snapshot CID for this submission key
					submissionSnapshotCIDMap[submissionKey] = snapshotCID

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

			// Update eligible submission counts for each snapshotter identity in the batch based on the most frequent snapshot CID
			if err := s.UpdateEligibleSubmissionCounts(batch, projectMostFrequentCID, submissionSnapshotCIDMap); err != nil {
				log.Errorf("Error updating eligible submission counts for data market address %s: %v", s.DataMarketAddress, err)
				return
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
			batchSubmission, err := merkle.BuildMerkleTree(submissionIDs, submissionData, s.EpochID, projectIDList, mostFrequentCIDList)
			if err != nil {
				log.Errorln("Error building batch: ", err)
				return
			}

			mu.Lock()
			finalizedBatchSubmissions = append(finalizedBatchSubmissions, batchSubmission)
			mu.Unlock()

			log.Debugf("CID: %s EpochID: %s", batchSubmission.CID, s.EpochID.String())
		}(batch)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// After finalizing all batches, send them to the external tx relayer service
	if err := clients.SendSubmissionBatchSize(s.EpochID, len(finalizedBatchSubmissions)); err != nil {
		errorMsg := fmt.Sprintf("Error sending submission batch size for epoch %s: %v", s.EpochID.String(), err)
		clients.SendFailureNotification(pkgs.SendSubmissionBatchSize, errorMsg, time.Now().String(), "Medium")
		log.Errorln(errorMsg)
	}

	// Submit finalized batch submissions to the external Tx Relayer service
	for _, submission := range finalizedBatchSubmissions {
		log.Debugf("Submitting CID %s for EpochID: %s", submission.CID, s.EpochID.String())

		if err := clients.SubmitSubmissionBatch(
			s.DataMarketAddress,
			submission.CID,
			s.EpochID,
			submission.Batch.PIDs,
			submission.Batch.CIDs,
			common.Bytes2Hex(submission.FinalizedCIDsRootHash),
		); err != nil {
			log.Errorf("Batch submission failed for CID %s: %v", submission.CID, err)
			continue
		}

		time.Sleep(time.Duration(config.SettingsObj.BlockTime) * 500 * time.Millisecond)
	}

	// Return the finalized batch submissions
	return finalizedBatchSubmissions, nil
}

func (s *SubmissionDetails) UpdateEligibleSubmissionCounts(batch map[string][]string, projectMostFrequentCID, submissionSnapshotCIDMap map[string]string) error {
	dataMarketAddress := s.DataMarketAddress
	eligibleSubmissionCounts := make(map[string]int)

	// Process each key in the current batch
	for projectID, submissionKeys := range batch {
		mostFrequentCID, found := projectMostFrequentCID[projectID]
		if !found {
			log.Errorf("No most frequent CID found for project %s", projectID)
			continue
		}

		// Iterate over the submission keys
		for _, submissionKey := range submissionKeys {
			// Fetch the snapshotCID for the given submission key
			snapshotCID, exists := submissionSnapshotCIDMap[submissionKey]
			if !exists {
				log.Errorln("Snapshot CID not found for key: ", submissionKey)
				continue
			}

			// Check if the snapshot CID matches the most frequent CID
			if snapshotCID == mostFrequentCID {
				eligibleSubmissionCounts[submissionKey] += 1
			}
		}
	}

	// Fetch the current day
	currentDay, err := prost.FetchCurrentDay(common.HexToAddress(dataMarketAddress), s.EpochID.Int64())
	if err != nil {
		log.Errorf("Failed to fetch current day for data market address %s: %v", dataMarketAddress, err)
		return err
	}

	// Update eligible submission counts in Redis for each snapshotter identity
	for submissionKey, submissionCount := range eligibleSubmissionCounts {
		// Set the eligible submission count in Redis
		key := redis.GetEligibleSubmissionCountsKey(currentDay.String(), s.DataMarketAddress, submissionKey)
		updatedCount, err := redis.IncrBy(context.Background(), key, int64(submissionCount))
		if err != nil {
			log.Errorf("Failed to update eligible submission count for snapshotter %s in data market address %s: %v", submissionKey, dataMarketAddress, err)
			return err
		}

		log.Debugf("Eligible submission count for snapshotter %s in data market address %s: %d", submissionKey, dataMarketAddress, updatedCount)
	}

	return nil
}
