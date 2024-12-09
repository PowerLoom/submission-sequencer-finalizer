-- Keys
local slotKey = KEYS[1]
local eligibleNodesKey = KEYS[2]

-- Arguments
local submissionCount = tonumber(ARGV[1])
local dailyQuota = tonumber(ARGV[2])
local slotID = ARGV[3]
local expiry = tonumber(ARGV[4]) 

-- Increment the submission count for the slot using INCRBY
local newCount = redis.call('INCRBY', slotKey, submissionCount)

-- Get current TTL of the slotKey
local keyTTL = redis.call('TTL', slotKey)

-- If the key does not exist or has no TTL, set the expiry to 3 days
if keyTTL == -2 then
    redis.call('EXPIRE', slotKey, expiry)
-- If the key has a TTL, reapply it to preserve existing expiry
elseif keyTTL ~= -1 then
    redis.call('EXPIRE', slotKey, keyTTL)
end

-- If the new count exceeds or equals the daily quota, add the slot to the eligible nodes set
if newCount >= dailyQuota then
    redis.call('SADD', eligibleNodesKey, slotID)

    -- Set the expiry for the eligibleNodesKey to 3 days
    redis.call('EXPIRE', eligibleNodesKey, expiry)
end

-- Return the new submission count
return newCount
