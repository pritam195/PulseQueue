-- KEYS[1] = active:high
-- KEYS[2] = active:default
-- KEYS[3] = active:low
-- KEYS[4] = processing queue

-- Try high priority
local jobId = redis.call("RPOPLPUSH", KEYS[1], KEYS[4])
if jobId then return jobId end

-- Try default priority
jobId = redis.call("RPOPLPUSH", KEYS[2], KEYS[4])
if jobId then return jobId end

-- Try low priority
jobId = redis.call("RPOPLPUSH", KEYS[3], KEYS[4])
return jobId
