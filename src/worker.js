const Redis = require('ioredis');
const fs = require('fs');
const path = require('path');
const mongoose = require('mongoose');
const JobRecord = require('./models/JobRecord');

class Worker {
    constructor(queueName, processFn, redisOptions = {}) {
        this.queueName = queueName;
        this.processFn = processFn;
        this.redis = new Redis(redisOptions);
        this.blockingRedis = new Redis(redisOptions);
        
        this.keys = {
            active: {
                high: `queue:${queueName}:active:high`,
                default: `queue:${queueName}:active:default`,
                low: `queue:${queueName}:active:low`,
            },
            delayed: `queue:${queueName}:delayed`,
            processing: `queue:${queueName}:processing`,
            notify: `queue:${queueName}:notify`,
        };
        
        this.stopped = false;
        
        const scriptPath = path.join(__dirname, 'lua', 'dequeuePriority.lua');
        const dequeueScript = fs.readFileSync(scriptPath, 'utf8');
        
        this.redis.defineCommand('dequeuePriority', {
            numberOfKeys: 4,
            lua: dequeueScript,
        });
    }
    
    async start() {
        this.stopped = false;
        this.loop();
    }
    
    async stop() {
        this.stopped = true;
        await this.redis.quit();
        await this.blockingRedis.quit();
    }
    
    async loop() {
        while (!this.stopped) {
            try {
                const jobId = await this.redis.dequeuePriority(
                    this.keys.active.high,
                    this.keys.active.default,
                    this.keys.active.low,
                    this.keys.processing
                );
                
                if (jobId) {
                    await this.executeJobWithHeartbeat(jobId);
                } else {
                    if (!this.stopped) {
                        await this.blockingRedis.brpop(this.keys.notify, 2);
                    }
                }
            } catch (err) {
                if (this.stopped && err.message.includes("Connection is closed")) break;
                console.error("Worker loop error:", err);
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    }
    
    async executeJobWithHeartbeat(jobId) {
        const lockKey = `job:${jobId}:lock`;
        let heartbeatInterval;
        
        try {
            // 1. Acquire Lease (30 seconds)
            await this.redis.set(lockKey, 'locked', 'EX', 30);
            
            // 2. Start Heartbeat (Renew every 15 seconds)
            heartbeatInterval = setInterval(async () => {
                try {
                    await this.redis.expire(lockKey, 30);
                } catch (e) {
                    console.error(`Failed to renew lock for job ${jobId}`, e);
                }
            }, 15000);

            // 3. Execute
            await this.executeJob(jobId);

        } finally {
            // 4. Clean up lock and interval regardless of success/failure
            if (heartbeatInterval) clearInterval(heartbeatInterval);
            await this.redis.del(lockKey).catch(() => {});
        }
    }
    
    async executeJob(jobId) {
        const jobKey = `job:${jobId}`;
        const rawHash = await this.redis.hgetall(jobKey);
        
        if (!rawHash || !rawHash.id) {
            // Job data is missing, just remove from processing
            await this.redis.lrem(this.keys.processing, 1, jobId);
            return;
        }

        let jobData = null;
        try {
            if (rawHash.data) jobData = JSON.parse(rawHash.data);
        } catch(e) {}

        await this.redis.hset(jobKey, 'status', 'active');
        
        try {
            await this.processFn({ id: jobId, data: jobData });
            await this.handleSuccess(jobId, rawHash);
        } catch (err) {
            await this.handleFailure(jobId, rawHash, err);
        }
    }

    async handleSuccess(jobId, rawHash) {
        const jobKey = `job:${jobId}`;
        const pipeline = this.redis.pipeline();
        pipeline.lrem(this.keys.processing, 1, jobId);
        pipeline.hset(jobKey, 'status', 'completed');
        await pipeline.exec();

        // Optional: Save to MongoDB for audit history
        try {
            if (mongoose.connection.readyState === 1) {
                await JobRecord.create({
                    jobId,
                    queueName: this.queueName,
                    data: rawHash.data ? JSON.parse(rawHash.data) : null,
                    priority: rawHash.priority,
                    status: 'completed',
                    attemptsMade: parseInt(rawHash.attemptsMade) || 1,
                    maxAttempts: parseInt(rawHash.maxAttempts) || 1
                });
                // Remove from Redis if stored durably
                await this.redis.del(jobKey);
            }
        } catch (dbErr) {
            console.error(`Failed to save completed job ${jobId} to DB:`, dbErr.message);
        }
    }

    async handleFailure(jobId, rawHash, err) {
        console.error(`Job ${jobId} failed:`, err.message);
        const jobKey = `job:${jobId}`;
        let attemptsMade = (parseInt(rawHash.attemptsMade) || 0) + 1;
        const maxAttempts = parseInt(rawHash.maxAttempts) || 1;
        const baseBackoff = parseInt(rawHash.backoff) || 1000;

        if (attemptsMade < maxAttempts) {
            // Retry: Exponential Backoff + Jitter
            const jitter = Math.floor(Math.random() * 500); // 0-500ms jitter
            const delay = (baseBackoff * Math.pow(2, attemptsMade - 1)) + jitter;
            const runAt = Date.now() + delay;

            const pipeline = this.redis.pipeline();
            pipeline.hset(jobKey, 'attemptsMade', attemptsMade, 'status', 'delayed', 'failedReason', err.message);
            pipeline.zadd(this.keys.delayed, runAt, jobId);
            pipeline.lrem(this.keys.processing, 1, jobId);
            await pipeline.exec();
            
            console.log(`Job ${jobId} scheduled for retry ${attemptsMade}/${maxAttempts} in ${delay}ms`);
        } else {
            // Dead Letter Queue
            console.log(`Job ${jobId} exhausted all ${maxAttempts} attempts. Moving to DLQ.`);
            const pipeline = this.redis.pipeline();
            pipeline.lrem(this.keys.processing, 1, jobId);
            pipeline.hset(jobKey, 'status', 'failed', 'failedReason', err.message, 'attemptsMade', attemptsMade);
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
                        failedReason: err.message
                    });
                    // Clean up Redis as it's now safely in MongoDB
                    await this.redis.del(jobKey);
                }
            } catch (dbErr) {
                console.error(`Failed to save DLQ job ${jobId} to DB:`, dbErr.message);
            }
        }
    }
}

module.exports = Worker;
