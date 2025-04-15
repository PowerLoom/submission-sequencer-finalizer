package batcher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"submission-sequencer-finalizer/pkgs"
	"submission-sequencer-finalizer/pkgs/clients"
	"submission-sequencer-finalizer/pkgs/ipfs"
	"submission-sequencer-finalizer/pkgs/merkle"
	"submission-sequencer-finalizer/pkgs/prost"
	"submission-sequencer-finalizer/pkgs/redis"
	"time"

	"github.com/ethereum/go-ethereum/common"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
)

type DiscardedSubmissionDetails struct {
	MostFrequentSnapshotCID  string
	DiscardedSubmissionCount int
	DiscardedSubmissions     map[string][]string // map of slotID -> all snapshotCID's that are invalid
}

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
		log.Error(errorMsg)
		return nil, err
	}

	log.Infof("📤 Batch %d submission with CID %s successfully relayed for epoch %s in data market %s", s.BatchID, finalizedBatchSubmission.CID, s.EpochID.String(), s.DataMarketAddress)

	// Return the finalized batch submission
	return finalizedBatchSubmission, nil
}

func (s *SubmissionDetails) UpdateEligibleSubmissionCounts(batch map[string][]string, projectMostFrequentCID, submissionSnapshotCIDMap map[string]string) error {
	dataMarketAddress := s.DataMarketAddress
	eligibleSubmissionCounts := make(map[string]int)

	// Initialize map to track discarded submissions per project
	discardedSubmissionsMap := make(map[string]*DiscardedSubmissionDetails)

	// Process each submission in the batch by extracting slotID from submission keys
	for projectID, submissionKeys := range batch {
		mostFrequentCID, found := projectMostFrequentCID[projectID]
		if !found {
			errMsg := fmt.Sprintf("Most frequent CID missing for project %s in batch %d of epoch %s within data market %s", projectID, s.BatchID, s.EpochID.String(), s.DataMarketAddress)
			clients.SendFailureNotification(pkgs.UpdateEligibleSubmissionCounts, errMsg, time.Now().String(), "High")
			log.Error(errMsg)
			continue
		}

		discardedDetails := &DiscardedSubmissionDetails{
			MostFrequentSnapshotCID:  mostFrequentCID,
			DiscardedSubmissionCount: 0,
			DiscardedSubmissions:     make(map[string][]string),
		}

		// Iterate over the submission keys
		for _, submissionKey := range submissionKeys {
			// Fetch the snapshotCID for the given submission key
			snapshotCID, exists := submissionSnapshotCIDMap[submissionKey]
			if !exists {
				log.Errorln("Snapshot CID not found for key: ", submissionKey)
				continue
			}

			// Extract slotID from the submission key
			parts := strings.Split(submissionKey, ".")
			if len(parts) < 4 {
				log.Errorf("Invalid submission key stored in redis: %s", submissionKey)
				continue
			}

			slotID := parts[3]

			// If the snapshotCID is not the most frequent one, mark it as discarded
			if !exists || (found && snapshotCID != mostFrequentCID) {
				// Increment the discarded submissions count
				discardedDetails.DiscardedSubmissionCount++

				// Add the snapshotCID to the discarded submissions map
				discardedDetails.DiscardedSubmissions[slotID] = append(discardedDetails.DiscardedSubmissions[slotID], snapshotCID)
			}

			// Check if the snapshot CID matches the most frequent CID
			if snapshotCID == mostFrequentCID {
				// Update the eligible submission count for the slotID
				eligibleSubmissionCounts[slotID] += 1
			}
		}

		// Store the discarded details for this project
		if discardedDetails.DiscardedSubmissionCount > 0 {
			discardedSubmissionsMap[projectID] = discardedDetails
		}
	}

	// Fetch the current day
	currentDay, err := prost.FetchCurrentDay(common.HexToAddress(dataMarketAddress), s.EpochID.Int64())
	if err != nil {
		log.Errorf("Failed to fetch current day for data market %s: %v", dataMarketAddress, err)
		return err
	}

	if err := s.storeDiscardedSubmissionDetails(currentDay.String(), discardedSubmissionsMap); err != nil {
		log.Errorf("Failed to store discarded submission details for epoch %s, data market %s: %v", s.EpochID.String(), dataMarketAddress, err)
		return err
	}

	log.Debugf("✅ Successfully stored discarded submission details for epoch %s, data market %s in Redis", s.EpochID.String(), dataMarketAddress)

	// Fetch daily snapshot quota for the specified data market address from Redis
	dailySnapshotQuota, err := redis.GetDailySnapshotQuota(context.Background(), dataMarketAddress)
	if err != nil {
		log.Errorf("Failed to fetch daily snapshot quota for data market %s: %v", dataMarketAddress, err)
		return err
	}

	// Update eligible submission counts in Redis for each slotID
	for slotID, submissionCount := range eligibleSubmissionCounts {
		// Define Redis keys for eligible slot submission and set of eligible nodes for the day
		key := redis.EligibleSlotSubmissionKey(s.DataMarketAddress, slotID, currentDay.String())
		eligibleNodesKey := redis.EligibleNodesByDayKey(dataMarketAddress, currentDay.String())

		expiry := 259200 // Expiry set to 3 days (in seconds)

		// Execute the Lua script
		result, err := redis.RedisClient.EvalSha(context.Background(), prost.LuaScriptHash, []string{key, eligibleNodesKey}, submissionCount, dailySnapshotQuota.Int64(), slotID, expiry).Result()
		if err != nil {
			// Check if the error is due to NOSCRIPT
			if strings.Contains(err.Error(), "NOSCRIPT") {
				log.Warnf("Lua script not found (NOSCRIPT). Reloading Lua script...")

				// Attempt to reload the Lua script
				prost.LoadLuaScript()

				// Retry executing the Lua script after reloading
				_, err = redis.RedisClient.EvalSha(context.Background(), prost.LuaScriptHash, []string{key, eligibleNodesKey}, submissionCount, dailySnapshotQuota.Int64(), slotID, expiry).Result()
				if err != nil {
					errMsg := fmt.Sprintf("Failed to execute Lua script for slotID %s in batch %d, epoch %s within data market %s after reloading: %v", slotID, s.BatchID, s.EpochID.String(), dataMarketAddress, err)
					clients.SendFailureNotification(pkgs.UpdateEligibleSubmissionCounts, errMsg, time.Now().String(), "High")
					log.Error(errMsg)
					return err
				}
			}

			errMsg := fmt.Sprintf("Failed to execute Lua script for slotID %s in batch %d, epoch %s within data market %s: %v", slotID, s.BatchID, s.EpochID.String(), dataMarketAddress, err)
			clients.SendFailureNotification(pkgs.UpdateEligibleSubmissionCounts, errMsg, time.Now().String(), "High")
			log.Error(errMsg)
			return err
		}

		// Convert the result to integer
		updatedCount, ok := result.(int64)
		if !ok {
			errMsg := fmt.Sprintf("Failed to convert lua script result to integer, got: %v", result)
			clients.SendFailureNotification(pkgs.UpdateEligibleSubmissionCounts, errMsg, time.Now().String(), "High")
			log.Error(errMsg)
		}

		log.Debugf("✅ Successfully updated eligible submission count for slotID %s in batch %d, epoch %s within data market %s: %d", slotID, s.BatchID, s.EpochID, dataMarketAddress, updatedCount)

		// Define the Redis key for storing the eligible submission count by epoch
		eligibleSlotSubmissionByEpochKey := redis.EligibleSlotSubmissionsByEpochKey(s.DataMarketAddress, currentDay.String(), s.EpochID.String())

		// Store the updated submission count for the given epoch in Redis hashtable
		if err := redis.RedisClient.HSet(context.Background(), eligibleSlotSubmissionByEpochKey, slotID, updatedCount).Err(); err != nil {
			log.Errorf("Failed to add eligible submission count for slotID %s to epoch %s hashtable for data market %s: %v", slotID, s.EpochID.String(), dataMarketAddress, err)
			return err
		}

		log.Debugf("✅ Successfully added eligible submission count for slotID %s to epoch %s hashtable for data market %s: %d", slotID, s.EpochID.String(), dataMarketAddress, updatedCount)

		// Set an expiry for storing the eligible submission counts by epoch
		if err := redis.Expire(context.Background(), eligibleSlotSubmissionByEpochKey, pkgs.Day*3); err != nil {
			log.Errorf("Unable to set expiry for %s in Redis: %s", eligibleSlotSubmissionByEpochKey, err.Error())
			return err
		}
	}

	return nil
}

func (s *SubmissionDetails) storeDiscardedSubmissionDetails(currentDay string, discardedSubmissionsMap map[string]*DiscardedSubmissionDetails) error {
	// Construct the Redis main key for discarded submission details
	discardedKey := redis.DiscardedSubmissionsKey(s.DataMarketAddress, currentDay, s.EpochID.String())

	// Write discarded submission details to Redis as a hashtable
	for projectID, details := range discardedSubmissionsMap {
		// Serialize the DiscardedSubmissionDetails struct
		detailsJSON, err := json.Marshal(details)
		if err != nil {
			return fmt.Errorf("failed to serialize discarded submission details for project %s: %v", projectID, err)
		}

		// Store the details in the Redis hashtable
		if err := redis.RedisClient.HSet(context.Background(), discardedKey, projectID, detailsJSON).Err(); err != nil {
			return fmt.Errorf("failed to write discarded submission details for project %s to Redis: %v", projectID, err)
		}
	}

	// Set the expiry for the Redis key
	if err := redis.RedisClient.Expire(context.Background(), discardedKey, pkgs.Day*3).Err(); err != nil {
		return fmt.Errorf("failed to set expiry for key %s: %v", discardedKey, err)
	}

	return nil
}
