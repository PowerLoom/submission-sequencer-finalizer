package redis

import (
	"fmt"
	"strings"
	"submission-sequencer-finalizer/pkgs"
)

func GetDaySizeTableKey() string {
	return pkgs.DaySizeTableKey
}

func GetDailySnapshotQuotaTableKey() string {
	return pkgs.DailySnapshotQuotaTableKey
}

func GetCurrentDayKey(dataMarketAddress string) string {
	return fmt.Sprintf("%s.%s", pkgs.CurrentDayKey, strings.ToLower(dataMarketAddress))
}

func TriggeredProcessLog(process, identifier string) string {
	return fmt.Sprintf("%s.%s.%s", pkgs.ProcessTriggerKey, process, identifier)
}

func EligibleSlotSubmissionKey(dataMarketAddress string, slotID, currentDay string) string {
	return fmt.Sprintf("%s.%s.%s.%s", pkgs.EligibleSlotSubmissionsKey, strings.ToLower(dataMarketAddress), currentDay, slotID)
}

func EligibleSlotSubmissionsByDayKey(dataMarketAddress, currentDay string) string {
	return fmt.Sprintf("%s.%s.%s", pkgs.EligibleSlotSubmissionByDayKey, strings.ToLower(dataMarketAddress), currentDay)
}

func EligibleNodesCountByDayKey(dataMarketAddress, currentDay string) string {
	return fmt.Sprintf("%s.%s.%s", pkgs.EligibleNodesCountKey, strings.ToLower(dataMarketAddress), currentDay)
}
