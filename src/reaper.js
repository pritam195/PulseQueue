const Redis = require('ioredis');
const mongoose = require('mongoose');
const JobRecord = require('./models/JobRecord');

class Reaper {
    constructor(queueName, redisOptions = {}) {
        this.queueName = queueName;
        this.redis = new Redis(redisOptions);
        
        this.keys = {
            processing: `queue:${queueName}:processing`,
            delayed: `queue:${queueName}:delayed`,
            notify: `queue:${queueName}:notify`
        };
    }
    
    start(intervalMs = 30000) {
        if (this.interval) return;
        this.interval = setInterval(() => this.reap(), intervalMs);
    }
    
    stop() {
        if (this.interval) clearInterval(this.interval);
        return this.redis.quit();
    }
    
    async reap() {
        try {
            // Get all jobs currently in processing
            const jobs = await this.redis.lrange(this.keys.processing, 0, -1);
            if (!jobs || jobs.length === 0) return;
            
            for (const jobId of jobs) {
                const lockKey = `job:${jobId}:lock`;
                const jobKey = `job:${jobId}`;
                
                // Check if worker lock still exists
                const lockExists = await this.redis.exists(lockKey);
                
                if (!lockExists) {
                    // Lock expired. Worker died. Recover the job.
                    console.log(`[Reaper] Job ${jobId} lock expired! Worker crashed. Recovering...`);
                    
                    const rawHash = await this.redis.hgetall(jobKey);
                    if (!rawHash || !rawHash.id) {
                        // Job metadata is completely gone, just clear it from processing
                        await this.redis.lrem(this.keys.processing, 1, jobId);
                        continue;
                    }
                    
                    let attemptsMade = (parseInt(rawHash.attemptsMade) || 0) + 1;
                    const maxAttempts = parseInt(rawHash.maxAttempts) || 1;
                    
                    if (attemptsMade < maxAttempts) {
                        // Re-queue to delayed for immediate or backoff retry
                        const pipeline = this.redis.pipeline();
                        pipeline.hset(jobKey, 'status', 'delayed', 'attemptsMade', attemptsMade, 'failedReason', 'Worker crashed');
                        pipeline.zadd(this.keys.delayed, Date.now(), jobId); // Run immediately
                        pipeline.lrem(this.keys.processing, 1, jobId);
                        // Notify so promoter can grab it if it's immediate
                        pipeline.lpush(this.keys.notify, '1');
                        await pipeline.exec();
                        console.log(`[Reaper] Requeued Job ${jobId} for retry ${attemptsMade}/${maxAttempts}`);
                    } else {
                        // Exhausted retries due to crashes -> DLQ
                        console.log(`[Reaper] Job ${jobId} exhausted attempts. Moving to DLQ.`);
                        const pipeline = this.redis.pipeline();
                        pipeline.lrem(this.keys.processing, 1, jobId);
                        pipeline.hset(jobKey, 'status', 'failed', 'failedReason', 'Worker crashed repeatedly', 'attemptsMade', attemptsMade);
                        await pipeline.exec();
                        
                        try {
                            if (mongoose.connection.readyState === 1) {
                                await JobRecord.create({
                                    jobId,
                                    queueName: this.queueName,
                                    data: rawHash.data ? JSON.parse(rawHash.data) : null,
                                    priority: rawHash.priority,
                                    status: 'failed_dlq',
                                    attemptsMade,
                                    maxAttempts,
                                    failedReason: 'Worker crashed repeatedly'
                                });
                                await this.redis.del(jobKey);
                            }
                        } catch (e) {
                            console.error(`[Reaper] Failed to save DLQ for Job ${jobId}:`, e.message);
                        }
                    }
                }
            }
        } catch (err) {
            console.error("[Reaper] Error during reap:", err);
        }
    }
}

module.exports = Reaper;
