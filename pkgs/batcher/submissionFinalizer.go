package batcher

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"submission-sequencer-finalizer/pkgs"
	"submission-sequencer-finalizer/pkgs/clients"
	"submission-sequencer-finalizer/pkgs/ipfs"
	"submission-sequencer-finalizer/pkgs/merkle"
	"submission-sequencer-finalizer/pkgs/prost"
	"submission-sequencer-finalizer/pkgs/redis"
	"time"

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

	log.Infof("🛠️ Starting finalization for batch %d of epoch %s, data market %s with %d projects", s.BatchID, s.EpochID.String(), s.DataMarketAddress, len(s.Batch))

	totalSubmissions := 0 // Track the total number of processed submissions

	// Process each key in the current batch
	for projectID, submissionKeys := range s.Batch {
		// Increment total submissions count by the number of submission keys
		totalSubmissions += len(submissionKeys)

		// Iterate over the submission keys
		for _, submissionKey := range submissionKeys {
			// Fetch the value associated with the key from Redis
			submissionValue, err := redis.Get(context.Background(), submissionKey)
			if err != nil {
				log.Errorf("Error fetching submission value from Redis for key %s: %s", submissionKey, err.Error())
				continue
			}

			log.Debugf("🔑 Processing submission key %s with value %s", submissionKey, submissionValue)

			// Skip the key if the value has expired or is empty
			if len(submissionValue) == 0 {
				log.Warnf("Expired or empty value for key: %s", submissionKey)
				continue
			}

			// Split the submission value into ID and submission data parts
			submissionDataParts := strings.Split(submissionValue, ".")
			if len(submissionDataParts) != 2 {
				log.Errorln("Invalid format for submission value: ", submissionValue)
				continue
			}

			// Parse the submission data using the SnapshotSubmission structure
			submissionDetails := pkgs.SnapshotSubmission{}
			err = protojson.Unmarshal([]byte(submissionDataParts[1]), &submissionDetails)
			if err != nil {
				log.Errorf("Error unmarshalling data for key %s: %v", submissionKey, err)
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

	log.Infof("✅ Completed processing batch %d for epoch %s in data market %s with %d submissions across %d projects", s.BatchID, s.EpochID, s.DataMarketAddress, totalSubmissions, len(s.Batch))

	// Update eligible submission counts for each snapshotter identity in the batch based on the most frequent snapshot CID
	if err := s.UpdateEligibleSubmissionCounts(s.Batch, projectMostFrequentCID, submissionSnapshotCIDMap); err != nil {
		log.Errorf("Error updating eligible submission counts for batch %d of epoch %s, data market %s: %v", s.BatchID, s.EpochID.String(), s.DataMarketAddress, err)
		return nil, err
	}

	// Prepare the list of project IDs and their corresponding most frequent CIDs
	projectIDList := []string{}
	mostFrequentCIDList := []string{}
	for projectID := range projectMostFrequentCID {
		projectIDList = append(projectIDList, projectID)
		mostFrequentCIDList = append(mostFrequentCIDList, projectMostFrequentCID[projectID])
	}

	log.Debugf("🔄 Finalizing PIDs and CIDs for batch %d of epoch %s in data market %s", s.BatchID, s.EpochID, s.DataMarketAddress)

	// Build the Merkle tree for the current batch and generate the IPFS BatchSubmission
	finalizedBatchSubmission, err := merkle.BuildMerkleTree(submissionIDs, submissionData, s.EpochID, projectIDList, mostFrequentCIDList, s.DataMarketAddress, s.BatchID)
	if err != nil {
		log.Errorf("Merkle tree error: Failed to build merkle tree for batch %d, epoch %s within data market %s: %v", s.BatchID, s.EpochID.String(), s.DataMarketAddress, err)
		return nil, err
	}

	log.Infof("🌳 Merkle tree successfully constructed for batch %d, CID %s, epoch %s in data market %s", s.BatchID, finalizedBatchSubmission.CID, s.EpochID.String(), s.DataMarketAddress)

	// Submit finalized batch submission to the external tx Relayer service with retry mechanism
	err = s.sendSubmissionBatchToRelayer(finalizedBatchSubmission)
	if err != nil {
		errorMsg := fmt.Sprintf(
			"🚨 Relayer submission failed: Batch %d submission with CID %s for epoch %s in data market %s: %v",
			s.BatchID,
			finalizedBatchSubmission.CID,
			s.EpochID.String(),
			s.DataMarketAddress,
			err,
		)
		clients.SendFailureNotification(pkgs.SendSubmissionBatchToRelayer, errorMsg, time.Now().String(), "High")
		log.Errorf(errorMsg)
		return nil, err
	}

	log.Infof("📤 Batch %d submission with CID %s successfully relayed for epoch %s in data market %s", s.BatchID, finalizedBatchSubmission.CID, s.EpochID.String(), s.DataMarketAddress)

	// Return the finalized batch submission
	return finalizedBatchSubmission, nil
}

func (s *SubmissionDetails) UpdateEligibleSubmissionCounts(batch map[string][]string, projectMostFrequentCID, submissionSnapshotCIDMap map[string]string) error {
	dataMarketAddress := s.DataMarketAddress
	eligibleSubmissionCounts := make(map[string]int)

	// Process each submission in the batch by extracting slotID from submission keys
	for projectID, submissionKeys := range batch {
		mostFrequentCID, found := projectMostFrequentCID[projectID]
		if !found {
			log.Errorf("Most frequent CID missing for project %s in batch %d of epoch %s within data market %s", projectID, s.BatchID, s.EpochID.String(), s.DataMarketAddress)
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
		log.Errorf("Failed to fetch current day for data market %s: %v", dataMarketAddress, err)
		return err
	}

	// Fetch daily snapshot quota for the specified data market address from Redis
	dailySnapshotQuota, err := redis.GetDailySnapshotQuota(context.Background(), dataMarketAddress)
	if err != nil {
		log.Errorf("Failed to fetch daily snapshot quota for data market %s: %v", dataMarketAddress, err)
		return err
	}

	// Update eligible submission counts in Redis and prepare data for relayer
	slotIDs := make([]*big.Int, 0)
	submissionsList := make([]*big.Int, 0)

	// Update eligible submission counts in Redis for each slotID
	for slotID, submissionCount := range eligibleSubmissionCounts {
		// Set the eligible submission count in Redis
		key := redis.EligibleSlotSubmissionKey(s.DataMarketAddress, slotID, currentDay.String())
		updatedCount, err := redis.IncrBy(context.Background(), key, int64(submissionCount))
		if err != nil {
			log.Errorf("Failed to update eligible submission count for slotID %s in batch %d, epoch %s within data market %s: %v", slotID, s.BatchID, s.EpochID.String(), dataMarketAddress, err)
			return err
		}

		log.Debugf("✅ Successfully updated eligible submission count for slotID %s in batch %d, epoch %s within data market %s: %d", slotID, s.BatchID, s.EpochID, dataMarketAddress, updatedCount)

		eligibleSlotSubmissionByEpochKey := redis.EligibleSlotSubmissionsByEpochKey(s.DataMarketAddress, currentDay.String(), s.EpochID.String())
		if err := redis.RedisClient.HSet(context.Background(), eligibleSlotSubmissionByEpochKey, slotID, updatedCount).Err(); err != nil {
			log.Errorf("Failed to add eligible submission count for slotID %s to epoch %s hashtable for data market %s: %v", slotID, s.EpochID.String(), dataMarketAddress, err)
			return err
		}

		log.Debugf("✅ Successfully added eligible submission count for slotID %s to epoch %s hashtable for data market %s: %d", slotID, s.EpochID.String(), dataMarketAddress, updatedCount)

		// If the eligible submission count for a slotID exceeds the daily snapshot quota, add the slotID to the set
		if updatedCount >= dailySnapshotQuota.Int64() {
			if err := redis.AddToSet(context.Background(), redis.EligibleSlotSubmissionsByDayKey(dataMarketAddress, currentDay.String()), slotID); err != nil {
				log.Errorf("Failed to add slotID %s to eligible submissions set for data market %s on day %s: %v", slotID, dataMarketAddress, currentDay.String(), err)
				continue
			}

			log.Infof("✅ Successfully added slotID %s to eligible submissions set for data market %s on day %s", slotID, dataMarketAddress, currentDay.String())
		}

		// Prepare data for relayer
		slotIDBigInt, ok := new(big.Int).SetString(slotID, 10)
		if !ok {
			log.Errorf("Failed to convert slotID %s to big.Int", slotID)
			continue
		}

		slotIDs = append(slotIDs, slotIDBigInt)
		submissionsList = append(submissionsList, big.NewInt(updatedCount))
	}

	// Send submission count to relayer only if the current epoch is a multiple of 5
	if s.EpochID.Int64()%5 == 0 {
		// Send the updateRewards request to the relayer
		if err := s.sendUpdateRewardsToRelayer(slotIDs, submissionsList, currentDay); err != nil {
			errorMsg := fmt.Sprintf(
				"🚨 Relayer rewards update failed: Batch %d, epoch %s in data market %s: %v",
				s.BatchID,
				s.EpochID.String(),
				s.DataMarketAddress,
				err,
			)
			clients.SendFailureNotification(pkgs.SendUpdateRewardsToRelayer, errorMsg, time.Now().String(), "High")
			log.Errorf(errorMsg)
			return err
		}
	}

	return nil
}

func (s *SubmissionDetails) sendUpdateRewardsToRelayer(slotIDs, submissionsList []*big.Int, day *big.Int) error {
	// Define the operation that will be retried
	operation := func() error {
		// Attempt to send the updateRewards request
		err := clients.SendUpdateRewardsRequest(s.DataMarketAddress, slotIDs, submissionsList, day, 0)
		if err != nil {
			log.Errorf("Error sending updateRewards request for batch %d, epoch %s in data market %s: %v. Retrying...", s.BatchID, s.EpochID.String(), s.DataMarketAddress, err)
			return err // Return error to trigger retry
		}

		log.Infof("📤 Successfully sent updateRewards request for batch %d, epoch %s to relayer in data market %s", s.BatchID, s.EpochID.String(), s.DataMarketAddress)
		return nil // Successful submission, no need for further retries
	}

	// Customize the backoff configuration
	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.InitialInterval = 1 * time.Second // Start with a 1-second delay
	backoffConfig.Multiplier = 1.5                  // Increase interval by 1.5x after each retry
	backoffConfig.MaxInterval = 4 * time.Second     // Set max interval between retries
	backoffConfig.MaxElapsedTime = 10 * time.Second // Retry for a maximum of 10 seconds

	// Limit retries to a maximum of 3 attempts within 10 seconds
	if err := backoff.Retry(operation, backoff.WithMaxRetries(backoffConfig, 3)); err != nil {
		log.Errorf("Failed to send updateRewards request after retries for batch %d, epoch %s in data market %s: %v", s.BatchID, s.EpochID.String(), s.DataMarketAddress, err)
		return err
	}

	return nil
}

func (s *SubmissionDetails) sendSubmissionBatchToRelayer(finalizedBatchSubmission *ipfs.BatchSubmission) error {
	// Define the operation that will be retried
	operation := func() error {
		// Attempt to submit the batch
		err := clients.SubmitSubmissionBatch(
			s.DataMarketAddress,
			finalizedBatchSubmission.CID,
			s.EpochID,
			finalizedBatchSubmission.Batch.PIDs,
			finalizedBatchSubmission.Batch.CIDs,
			string(finalizedBatchSubmission.FinalizedCIDsRootHash),
		)
		if err != nil {
			log.Errorf("Batch %d submission failed for epoch %s in data market %s: %v. Retrying...", s.BatchID, s.EpochID.String(), s.DataMarketAddress, err)
			return err // Return error to trigger retry
		}

		log.Infof("📤 Successfully submitted batch %d with CID %s to relayer for epoch %s in data market %s", s.BatchID, finalizedBatchSubmission.CID, s.EpochID.String(), s.DataMarketAddress)
		return nil // Successful submission, no need for further retries
	}

	// Customize the backoff configuration
	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.InitialInterval = 1 * time.Second // Start with a 1-second delay
	backoffConfig.Multiplier = 1.5                  // Increase interval by 1.5x after each retry
	backoffConfig.MaxInterval = 4 * time.Second     // Set max interval between retries
	backoffConfig.MaxElapsedTime = 10 * time.Second // Retry for a maximum of 10 seconds

	// Limit retries to 3 times within 10 seconds
	if err := backoff.Retry(operation, backoff.WithMaxRetries(backoffConfig, 3)); err != nil {
		log.Errorf("Failed to submit batch %d submission with CID %s to relayer for epoch %s in data market %s after retries: %v", s.BatchID, finalizedBatchSubmission.CID, s.EpochID.String(), s.DataMarketAddress, err)
		return err
	}

	return nil
}
