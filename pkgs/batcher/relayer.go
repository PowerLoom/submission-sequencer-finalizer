package batcher

import (
	"submission-sequencer-finalizer/pkgs/clients"
	"submission-sequencer-finalizer/pkgs/ipfs"
	"time"

	"github.com/cenkalti/backoff"
	log "github.com/sirupsen/logrus"
)

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
