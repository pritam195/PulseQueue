-- KEYS[1] = delayed key (ZSET)
-- KEYS[2] = active:high (LIST)
-- KEYS[3] = active:default (LIST)
-- KEYS[4] = active:low (LIST)
-- KEYS[5] = notify (LIST)
-- ARGV[1] = current timestamp
-- ARGV[2] = max jobs to promote

local delayedKey = KEYS[1]
local activeHigh = KEYS[2]
local activeDefault = KEYS[3]
local activeLow = KEYS[4]
local notifyKey = KEYS[5]
local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

-- Find jobs that are due
local jobs = redis.call("ZRANGEBYSCORE", delayedKey, "-inf", now, "LIMIT", 0, limit)

if #jobs > 0 then
    for i, jobId in ipairs(jobs) do
        -- Fetch priority from the job's hash
        -- Using HGET on job:<id>
        local priority = redis.call("HGET", "job:" .. jobId, "priority")
        
        -- Default to 'default' if no priority is set
        if priority == "high" then
            redis.call("RPUSH", activeHigh, jobId)
        elseif priority == "low" then
            redis.call("RPUSH", activeLow, jobId)
        else
            redis.call("RPUSH", activeDefault, jobId)
        end
        
        -- Notify blocked workers
        redis.call("LPUSH", notifyKey, "1")
        
        -- Remove from delayed queue
        redis.call("ZREM", delayedKey, jobId)
    end
end

return #jobs
