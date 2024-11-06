package batcher

import (
	"context"
	"strings"
	"submission-sequencer-finalizer/pkgs"
	"submission-sequencer-finalizer/pkgs/clients"
	"submission-sequencer-finalizer/pkgs/ipfs"
	"submission-sequencer-finalizer/pkgs/merkle"
	"submission-sequencer-finalizer/pkgs/prost"
	"submission-sequencer-finalizer/pkgs/redis"

	"github.com/cenkalti/backoff"
	"github.com/ethereum/go-ethereum/common"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
)

func (s *SubmissionDetails) FinalizeBatch() (*ipfs.BatchSubmission, error) {
	// Initialize slices to store submission IDs and submission data
	submissionIDs := []string{}
	submissionData := []string{}

	// Maps to track the most frequent CIDs for each project and frequency count of submissions
	projectMostFrequentCID := make(map[string]string)
	projectCIDFrequencies := make(map[string]map[string]int)

	// Map to store snapshotter identity (key) and snapshot CID for each submission
	submissionSnapshotCIDMap := make(map[string]string)

	// Process each key in the current batch
	for projectID, submissionKeys := range s.Batch {
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
	if err := s.UpdateEligibleSubmissionCounts(s.Batch, projectMostFrequentCID, submissionSnapshotCIDMap); err != nil {
		log.Errorf("Error updating eligible submission counts for data market address %s: %v", s.DataMarketAddress, err)
		return nil, err
	}

	// Prepare the list of project IDs and their corresponding most frequent CIDs
	projectIDList := []string{}
	mostFrequentCIDList := []string{}
	for projectID := range projectMostFrequentCID {
		projectIDList = append(projectIDList, projectID)
		mostFrequentCIDList = append(mostFrequentCIDList, projectMostFrequentCID[projectID])
	}

	log.Debugf("🔄 Finalizing PIDs and CIDs for batch %v of epochID %s in data market address %s:\nProject IDs: %v\nMost Frequent CIDs: %v", s.Batch, s.EpochID, s.DataMarketAddress, projectIDList, mostFrequentCIDList)

	// Build the Merkle tree for the current batch and generate the IPFS BatchSubmission
	finalizedBatchSubmission, err := merkle.BuildMerkleTree(submissionIDs, submissionData, s.EpochID, projectIDList, mostFrequentCIDList)
	if err != nil {
		log.Errorf("Error building batch: %v", err)
		return nil, err
	}

	log.Infof("Successfully built Merkle tree. CID: %s, EpochID: %s", finalizedBatchSubmission.CID, s.EpochID.String())

	// Submit finalized batch submission to the external tx Relayer service with retry mechanism
	err = s.submitWithRetries(finalizedBatchSubmission)
	if err != nil {
		log.Errorf("❌ Failed to send batch submission with CID %s: %v", finalizedBatchSubmission.CID, err)
		return nil, err
	}

	log.Infof("Submitted finalized batch submission. CID: %s, EpochID: %s", finalizedBatchSubmission.CID, s.EpochID.String())

	// Return the finalized batch submission
	return finalizedBatchSubmission, nil
}

func (s *SubmissionDetails) submitWithRetries(finalizedBatchSubmission *ipfs.BatchSubmission) error {
	// Define the operation that will be retried
	operation := func() error {
		// Attempt to submit the batch
		err := clients.SubmitSubmissionBatch(
			s.DataMarketAddress,
			finalizedBatchSubmission.CID,
			s.EpochID,
			finalizedBatchSubmission.Batch.PIDs,
			finalizedBatchSubmission.Batch.CIDs,
			common.Bytes2Hex(finalizedBatchSubmission.FinalizedCIDsRootHash),
		)
		if err != nil {
			log.Errorf("Batch submission failed for CID %s: %v. Retrying...", finalizedBatchSubmission.CID, err)
			return err // Return error to trigger retry
		}

		log.Infof("Successfully submitted batch. CID: %s, EpochID: %s", finalizedBatchSubmission.CID, s.EpochID.String())
		return nil // Successful submission, no need for further retries
	}

	// Retry submission with exponential backoff
	backoffConfig := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5) // Retry up to 5 times
	if err := backoff.Retry(operation, backoffConfig); err != nil {
		log.Errorf("Failed to submit batch after retries. CID: %s, EpochID: %s, Error: %v", finalizedBatchSubmission.CID, s.EpochID.String(), err)
		return err
	}

	return nil
}

func (s *SubmissionDetails) UpdateEligibleSubmissionCounts(batch map[string][]string, projectMostFrequentCID, submissionSnapshotCIDMap map[string]string) error {
	dataMarketAddress := s.DataMarketAddress
	eligibleSubmissionCounts := make(map[string]int)

	// Process each submission in the batch by extracting slotID from submission keys
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
				// Extract slotID from the submission key
				parts := strings.Split(submissionKey, ".")
				if len(parts) < 4 {
					log.Errorf("Invalid submission key stored in redis: %s", submissionKey)
					continue
				}

				slotID := parts[3]

				// Update the eligible submission count for the slotID
				eligibleSubmissionCounts[slotID] += 1
			}
		}
	}

	// Fetch the current day
	currentDay, err := prost.FetchCurrentDay(common.HexToAddress(dataMarketAddress), s.EpochID.Int64())
	if err != nil {
		log.Errorf("Failed to fetch current day for data market address %s: %v", dataMarketAddress, err)
		return err
	}

	// Update eligible submission counts in Redis for each slotID
	for slotID, submissionCount := range eligibleSubmissionCounts {
		// Set the eligible submission count in Redis
		key := redis.EligibleSlotSubmissionKey(s.DataMarketAddress, currentDay.String(), slotID)
		updatedCount, err := redis.IncrBy(context.Background(), key, int64(submissionCount))
		if err != nil {
			log.Errorf("Failed to update eligible submission count for slotID %s in data market address %s: %v", slotID, dataMarketAddress, err)
			return err
		}

		log.Debugf("Eligible submission count for slotID %s in data market address %s: %d", slotID, dataMarketAddress, updatedCount)
	}

	return nil
}
