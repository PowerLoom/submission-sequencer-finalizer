package prost

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/big"
	"net/http"
	"submission-sequencer-finalizer/config"
	"submission-sequencer-finalizer/pkgs/clients"
	"submission-sequencer-finalizer/pkgs/contract"
	"submission-sequencer-finalizer/pkgs/redis"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	log "github.com/sirupsen/logrus"
)

var (
	Client       *ethclient.Client
	Instance     *contract.Contract
	epochsInADay = 720
)

func ConfigureClient() {
	rpcClient, err := rpc.DialOptions(context.Background(), config.SettingsObj.ClientUrl, rpc.WithHTTPClient(&http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}))
	if err != nil {
		log.Errorf("Failed to connect to client: %s", err)
		log.Fatal(err)
	}

	Client = ethclient.NewClient(rpcClient)
}

func ConfigureContractInstance() {
	Instance, _ = contract.NewContract(common.HexToAddress(config.SettingsObj.ContractAddress), Client)
}

func MustQuery[K any](ctx context.Context, call func() (val K, err error)) (K, error) {
	expBackOff := backoff.NewConstantBackOff(1 * time.Second)

	var val K
	operation := func() error {
		var err error
		val, err = call()
		return err
	}
	// Use the retry package to execute the operation with backoff
	err := backoff.Retry(operation, backoff.WithMaxRetries(expBackOff, 3))
	if err != nil {
		clients.SendFailureNotification("Contract query error [MustQuery]", err.Error(), time.Now().String(), "High")
		return *new(K), err
	}
	return val, err
}

func LoadContractStateVariables() {
	// Iterate over each data market contract address in the config
	for _, dataMarketAddress := range config.SettingsObj.DataMarketContractAddresses {
		// Fetch the day size for the specified data market address from contract
		if output, err := MustQuery(context.Background(), func() (*big.Int, error) {
			return Instance.DAYSIZE(&bind.CallOpts{}, dataMarketAddress)
		}); err == nil {
			// Convert the day size to a string for storage in Redis
			daySize := output.String()

			// Store the day size in the Redis hash table
			err := redis.RedisClient.HSet(context.Background(), redis.GetDaySizeTableKey(), dataMarketAddress.Hex(), daySize).Err()
			if err != nil {
				log.Errorf("Failed to set day size for data market %s in Redis: %v", dataMarketAddress.Hex(), err)
			}
		}
	}
}

func getExpirationTime(epochID, daySize int64) time.Time {
	// DAY_SIZE in microseconds
	updatedDaySize := time.Duration(daySize) * time.Microsecond

	// Calculate the duration of each epoch
	epochDuration := updatedDaySize / time.Duration(epochsInADay)

	// Calculate the number of epochs left for the day
	remainingEpochs := epochID % int64(epochsInADay)

	// Calculate the expiration duration
	expirationDuration := epochDuration * time.Duration(remainingEpochs)

	// Set a buffer of 10 seconds to expire slightly earlier
	bufferDuration := 10 * time.Second

	// Calculate the expiration time by subtracting the buffer duration
	expirationTime := time.Now().Add(expirationDuration - bufferDuration)

	return expirationTime
}

func FetchCurrentDay(dataMarketAddress common.Address, epochID int64) (*big.Int, error) {
	// Fetch the current day for the given data market address from Redis
	value, err := redis.Get(context.Background(), redis.GetCurrentDayKey(dataMarketAddress.Hex()))
	if err != nil {
		log.Errorf("Error fetching day value for data market %s from Redis: %v", dataMarketAddress.Hex(), err)
		return nil, err
	}

	if value != "" {
		// Cache hit: return the current day value
		currentDay := new(big.Int)
		currentDay.SetString(value, 10)
		return currentDay, nil
	}

	// Cache miss: fetch the current day for the specified data market address from contract
	var currentDay *big.Int
	if output, err := MustQuery(context.Background(), func() (*big.Int, error) {
		return Instance.DayCounter(&bind.CallOpts{}, dataMarketAddress)
	}); err == nil {
		currentDay = output
	}

	// Fetch day size for the specified data market address from Redis
	daySize, err := redis.GetDaySize(context.Background(), dataMarketAddress.Hex())
	if err != nil {
		log.Errorf("Failed to fetch day size for data market %s: %v", dataMarketAddress, err)
		return nil, err
	}

	// Calculate expiration time
	expirationTime := getExpirationTime(epochID, daySize.Int64())

	// Set the current day in Redis with the calculated expiration duration
	if err := redis.SetWithExpiration(context.Background(), redis.GetCurrentDayKey(dataMarketAddress.Hex()), currentDay.String(), time.Until(expirationTime)); err != nil {
		return nil, fmt.Errorf("failed to cache day value for data market %s in Redis: %v", dataMarketAddress, err)
	}

	return currentDay, nil
}
