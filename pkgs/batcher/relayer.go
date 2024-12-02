package batcher

import (
	"math/big"
	"submission-sequencer-finalizer/pkgs/clients"
	"submission-sequencer-finalizer/pkgs/ipfs"
	"time"

	"github.com/cenkalti/backoff"
	log "github.com/sirupsen/logrus"
)

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
