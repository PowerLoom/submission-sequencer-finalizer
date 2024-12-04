package pkgs

import "time"

// Process Name Constants
// process : identifier
const (
	FinalizeBatches                = "SequencerFinalizer: FinalizeBatches"
	BuildMerkleTree                = "SequencerFinalizer: BuildMerkleTree"
	BuildBatchSubmissions          = "SequencerFinalizer: BuildBatchSubmissions"
	ArrangeKeysInBatches           = "SequencerFinalizer: ArrangeKeysInBatches"
	SendSubmissionBatchSize        = "SequencerFinalizer: SendSubmissionBatchSize"
	SendUpdateRewardsToRelayer     = "SequencerFinalizer: SendUpdateRewardsToRelayer"
	SendSubmissionBatchToRelayer   = "SequencerFinalizer: SendSubmissionBatchToRelayer"
	UpdateEligibleSubmissionCounts = "SequencerFinalizer: UpdateEligibleSubmissionCounts"
)

// General Key Constants
const (
	CurrentDayKey                    = "CurrentDayKey"
	DaySizeTableKey                  = "DaySizeTableKey"
	ProcessTriggerKey                = "TriggeredSequencerProcess"
	EligibleSlotSubmissionsKey       = "EligibleSlotSubmissionsKey"
	DailySnapshotQuotaTableKey       = "DailySnapshotQuotaTableKey"
	EligibleSlotSubmissionByDayKey   = "EligibleSlotSubmissionByDayKey"
	EligibleSlotSubmissionByEpochKey = "EligibleSlotSubmissionByEpochKey"
	DiscardedSubmissionKey           = "DiscardedSubmissionKey"
)

// General Constants
const (
	Day = 24 * time.Hour
)
