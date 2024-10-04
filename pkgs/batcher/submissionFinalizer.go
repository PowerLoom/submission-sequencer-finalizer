package batcher

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"submission-sequencer-finalizer/config"
	"submission-sequencer-finalizer/pkgs/ipfs"
	"submission-sequencer-finalizer/pkgs/redis"
	"submission-sequencer-finalizer/pkgs/service"
	"time"

	log "github.com/sirupsen/logrus"
)

type SubmissionDetails struct {
	epochID    *big.Int
	batchID    int
	projectMap map[string][]string // ProjectID -> SubmissionKeys
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
		log.Printf("Processing batch: %d with epoch ID: %s\n", submissionDetails.batchID, submissionDetails.epochID.String())

		// Call the method to build and finalize the batch submissions
		submissionDetails.BuildBatchSubmissions()
	}
}

// BuildBatchSubmissions organizes project submission keys into batches and finalizes them for processing.
func (s *SubmissionDetails) BuildBatchSubmissions() ([]*ipfs.BatchSubmission, error) {
	// Step 1: Organize the projectMap into batches of submission keys
	batchedSubmissionKeys := arrangeSubmissionKeysInBatches(s.projectMap)
	log.Debugf("Arranged %d batches of submission keys for processing: %v", len(batchedSubmissionKeys), batchedSubmissionKeys)

	// Step 2: Finalize the batch submissions based on the epochID and batched submission keys
	finalizedBatches, err := finalizeBatches(batchedSubmissionKeys, s.epochID)
	if err != nil {
		errorMsg := fmt.Sprintf("Error finalizing batches for epoch %s: %v", s.epochID.String(), err)
		service.SendFailureNotification("BuildBatchSubmissions", errorMsg, time.Now().String(), "High")
		log.Errorf("Batch finalization error: %s", errorMsg)
		return nil, err
	}

	// Step 3: Log and return the finalized batch submissions
	log.Debugf("Successfully finalized %d batch submissions for epoch %s", len(finalizedBatches), s.epochID.String())

	return finalizedBatches, nil
}

/*func finalizeBatches(batchedKeys [][]string, epochID *big.Int) ([]*ipfs.BatchSubmission, error) {
	batchSubmissions := make([]*ipfs.BatchSubmission, 0)
	var mu sync.Mutex

	for _, batch := range batchedKeys {
		wg.Add(1)

		go func(batch []string) {
			defer wg.Done()

			log.Debugln("Processing batch: ", batch)
			submissionIDs := []string{}
			submissionData := []string{}
			localProjectMostFrequent := make(map[string]string)
			localProjectValueFrequencies := make(map[string]map[string]int)

			for _, key := range batch {
				val, err := redis.Get(context.Background(), key)

				if err != nil {
					clients.SendFailureNotification("finalizeBatches", fmt.Sprintf("Error fetching data from redis: %s", err.Error()), time.Now().String(), "High")
					log.Errorln("Error fetching data from redis: ", err.Error())
					continue
				}

				log.Debugln(fmt.Sprintf("Processing key %s and value %s", key, val))

				if len(val) == 0 {
					clients.SendFailureNotification("finalizeBatches", fmt.Sprintf("Value has expired for key, not being counted in batch: %s", key), time.Now().String(), "High")
					log.Errorln("Value has expired for key:  ", key)
					continue
				}

				parts := strings.Split(key, ".")
				if len(parts) != 3 {
					clients.SendFailureNotification("finalizeBatches", fmt.Sprintf("Key should have three parts, invalid key: %s", key), time.Now().String(), "High")
					log.Errorln("Key should have three parts, invalid key: ", key)
					continue // skip malformed keys
				}
				projectId := parts[1]

				if localProjectValueFrequencies[projectId] == nil {
					localProjectValueFrequencies[projectId] = make(map[string]int)
				}

				idSubPair := strings.Split(val, ".")
				if len(idSubPair) != 2 {
					clients.SendFailureNotification("finalizeBatches", fmt.Sprintf("Value should have two parts, invalid value: %s", val), time.Now().String(), "High")
					log.Errorln("Value should have two parts, invalid value: ", val)
					continue // skip malformed keys
				}

				subHolder := pkgs.SnapshotSubmission{}
				err = protojson.Unmarshal([]byte(idSubPair[1]), &subHolder)
				if err != nil {
					clients.SendFailureNotification("finalizeBatches", fmt.Sprintf("Unmarshalling %s error: %s", idSubPair[1], err.Error()), time.Now().String(), "High")
					log.Errorln("Unable to unmarshal submission: ", err)
					continue
				}

				value := subHolder.Request.SnapshotCid

				localProjectValueFrequencies[projectId][value] += 1

				if count, exists := localProjectValueFrequencies[projectId][value]; exists {
					if count > localProjectValueFrequencies[projectId][localProjectMostFrequent[projectId]] {
						localProjectMostFrequent[projectId] = value
					}
				}

				submissionIDs = append(submissionIDs, idSubPair[0])
				submissionData = append(submissionData, idSubPair[1])
			}

			var keys []string
			for pid := range localProjectMostFrequent {
				keys = append(keys, pid)
			}

			pids := []string{}
			cids := []string{}
			sort.Strings(keys)
			for _, pid := range keys {
				pids = append(pids, pid)
				cids = append(cids, localProjectMostFrequent[pid])
			}

			log.Debugln("PIDs and CIDs for epoch: ", epochID, pids, cids)

			batchSubmission, err := merkle.BuildMerkleTree(submissionIDs, submissionData, BatchId, epochID, pids, cids)
			if err != nil {
				clients.SendFailureNotification("finalizeBatches", fmt.Sprintf("Batch building error: %s", err.Error()), time.Now().String(), "High")
				log.Errorln("Error storing the batch: ", err.Error())
				return
			}

			mu.Lock()
			batchSubmissions = append(batchSubmissions, batchSubmission)
			BatchId++
			mu.Unlock()

			log.Debugf("CID: %s Batch: %d", batchSubmission.Cid, BatchId-1)
		}(batch)
	}

	for _, batch := range batchedKeys {
		log.Debugln("Processing batch: ", batch)

		submissionIDs := []string{}
		submissionData := []string{}
		localProjectMostFrequent := make(map[string]string)
		localProjectValueFrequencies := make(map[string]map[string]int)

		for _, key := range batch {
			val, err := redis.Get(context.Background(), key)
			if err != nil {
				clients.SendFailureNotification(pkgs.FinalizeBatches, fmt.Sprintf("Error fetching data from redis: %s", err.Error()), time.Now().String(), "High")
				log.Errorln("Error fetching data from redis: ", err.Error())
				continue
			}

			log.Debugln(fmt.Sprintf("Processing key %s and value %s", key, val))

			if len(val) == 0 {
				clients.SendFailureNotification(pkgs.FinalizeBatches, fmt.Sprintf("Value has expired for key, not being counted in batch: %s", key), time.Now().String(), "High")
				log.Errorln("Value has expired for key: ", key)
				continue
			}

		}
	}

	finalizedBatchIDs := []string{}
	for _, batchSubmission := range batchSubmissions {
		finalizedBatchIDs = append(finalizedBatchIDs, batchSubmission.Batch.ID.String())
	}

	// Set finalized batches in redis for epochId
	logEntry := map[string]interface{}{
		"epoch_id":                epochID.String(),
		"finalized_batches_count": len(batchSubmissions),
		"finalized_batch_ids":     finalizedBatchIDs,
		"timestamp":               time.Now().Unix(),
	}

	if err := redis.SetProcessLog(context.Background(), redis.TriggeredProcessLog(FinalizeBatches, epochID.String()), logEntry, 4*time.Hour); err != nil {
		service.SendFailureNotification(FinalizeBatches, err.Error(), time.Now().String(), "High")
		log.Errorln("finalizeBatches process log error: ", err.Error())
	}

	return batchSubmissions, nil
}*/

// arrangeSubmissionKeysInBatches organizes submission keys from the project map into batches
func arrangeSubmissionKeysInBatches(projectMap map[string][]string) [][]string {
	var (
		batches      [][]string                     // Holds the final list of batches
		batchSize    = config.SettingsObj.BatchSize // Maximum size for each batch
		currentBatch = make([]string, 0, batchSize)
	)

	// Iterate over each project's submission keys
	for _, submissionKeys := range projectMap {
		// Determine the total number of keys if we add the current project's keys
		totalKeys := len(currentBatch) + len(submissionKeys)

		// If the current batch can accommodate all submission keys from this project, add them
		if totalKeys <= batchSize {
			currentBatch = append(currentBatch, submissionKeys...)
		} else {
			// If the current batch is not empty, add it to batches
			if len(currentBatch) > 0 {
				batches = append(batches, currentBatch)
			}

			currentBatch = submissionKeys
		}

		// If the current batch reaches the maximum size, finalize it
		if len(currentBatch) >= batchSize {
			batches = append(batches, currentBatch)
			currentBatch = make([]string, 0, batchSize) // Start a new batch
		}
	}

	// Add the final batch if there are any leftover keys
	if len(currentBatch) > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}
