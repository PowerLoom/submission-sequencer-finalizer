package pkgs

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
	CurrentDayKey              = "CurrentDayKey"
	DaySizeTableKey            = "DaySizeTableKey"
	ProcessTriggerKey          = "TriggeredSequencerProcess"
	EligibleSlotSubmissionsKey = "EligibleSlotSubmissionsKey"
	DailySnapshotQuotaTableKey = "DailySnapshotQuotaTableKey"
	EligibleNodeByDayKey       = "EligibleNodeByDayKey"
)
