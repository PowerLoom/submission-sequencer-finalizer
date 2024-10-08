package batcher

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"submission-sequencer-finalizer/config"
	"submission-sequencer-finalizer/pkgs"
	"submission-sequencer-finalizer/pkgs/ipfs"
	"submission-sequencer-finalizer/pkgs/merkle"
	"submission-sequencer-finalizer/pkgs/redis"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
)

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

	// After finalizing all batches, send them to the external service
	err := SendBatchSubmissions(finalizedBatchSubmissions)
	if err != nil {
		log.Errorln("Error sending batch submissions to relay service: ", err)
		return nil, err
	}

	// Return the finalized batch submissions
	return finalizedBatchSubmissions, nil
}

// SendBatchSubmissions sends all finalized batch submissions to the external transaction relay service.
func SendBatchSubmissions(batchSubmissions []*ipfs.BatchSubmission) error {
	client := &http.Client{
		Timeout: time.Duration(config.SettingsObj.HttpTimeout) * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	for _, batch := range batchSubmissions {
		// Marshal each batch submission to JSON
		batchJSON, err := json.Marshal(batch)
		if err != nil {
			log.Printf("Failed to marshal batch submission for CID %s: %v", batch.Cid, err)
			continue
		}

		// Create an HTTP request
		req, err := http.NewRequest("POST", config.SettingsObj.TransactionRelayUrl, bytes.NewBuffer(batchJSON))
		if err != nil {
			log.Printf("Failed to create HTTP request for CID %s: %v", batch.Cid, err)
			continue
		}

		// Set the appropriate headers (e.g., Content-Type)
		req.Header.Set("Content-Type", "application/json")

		// Send the request
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Failed to send batch submission to relay service for CID %s: %v", batch.Cid, err)
			continue
		}

		// Ensure that the response body is closed
		defer resp.Body.Close()

		// Check for a successful response
		if resp.StatusCode != http.StatusOK {
			log.Printf("Relay service returned an error for CID %s: %s", batch.Cid, resp.Status)
			continue
		}

		log.Printf("Successfully sent batch CID: %s to the relay service", batch.Cid)
	}

	return nil
}
