package redis

import (
	"fmt"
	"strings"
	"submission-sequencer-finalizer/pkgs"
)

func GetDaySizeTableKey() string {
	return pkgs.DaySizeTableKey
}

func GetCurrentDayKey(dataMarketAddress string) string {
	return fmt.Sprintf("%s.%s", pkgs.CurrentDayKey, strings.ToLower(dataMarketAddress))
}

func TriggeredProcessLog(process, identifier string) string {
	return fmt.Sprintf("%s.%s.%s", pkgs.ProcessTriggerKey, process, identifier)
}

func GetEligibleSubmissionCountsKey(currentDay, dataMarketAddress, snapshotter string) string {
	return fmt.Sprintf("%s.%s.%s.%s", pkgs.EligibleSubmissionCountsKey, currentDay, strings.ToLower(dataMarketAddress), snapshotter)
}
