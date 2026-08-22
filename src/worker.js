const Redis = require('ioredis');
const fs = require('fs');
const path = require('path');
const mongoose = require('mongoose');
const JobRecord = require('./models/JobRecord');

class Worker {
    constructor(queueName, processFn, options = {}) {
        this.queueName = queueName;
        this.processFn = processFn;
        this.concurrency = options.concurrency || 1;
        
        const redisOptions = options.redis || {};
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
        this.activeJobs = new Set();
        
        const scriptPath = path.join(__dirname, 'lua', 'dequeuePriority.lua');
        const dequeueScript = fs.readFileSync(scriptPath, 'utf8');
        
        this.redis.defineCommand('dequeuePriority', {
            numberOfKeys: 4,
            lua: dequeueScript,
        });
    }
    
    async start() {
        this.stopped = false;
        
        // Spawn N concurrent loops
        for (let i = 0; i < this.concurrency; i++) {
            this.loop(i);
        }
    }
    
    async stop(timeoutMs = 25000) {
        this.stopped = true;
        
        // Disconnecting blockingRedis instantly aborts any pending BRPOP
        await this.blockingRedis.quit().catch(() => {});
        
        if (this.activeJobs.size > 0) {
            console.log(`[Worker] Shutting down. Waiting for ${this.activeJobs.size} active jobs to finish...`);
            
            // Wait for jobs to finish OR timeout
            const timeoutPromise = new Promise(resolve => setTimeout(resolve, timeoutMs));
            const jobsPromise = Promise.allSettled(Array.from(this.activeJobs));
            
            await Promise.race([jobsPromise, timeoutPromise]);
            
            if (this.activeJobs.size > 0) {
                console.warn(`[Worker] Shutdown timeout reached. Abandoning ${this.activeJobs.size} jobs to the Reaper.`);
            }
        }
        
        await this.redis.quit().catch(() => {});
    }
    
    async loop(workerId) {
        while (!this.stopped) {
            try {
                const jobId = await this.redis.dequeuePriority(
                    this.keys.active.high,
                    this.keys.active.default,
                    this.keys.active.low,
                    this.keys.processing
                );
                
                if (jobId) {
                    const jobPromise = this.executeJobWithHeartbeat(jobId);
                    this.activeJobs.add(jobPromise);
                    
                    try {
                        await jobPromise;
                    } finally {
                        this.activeJobs.delete(jobPromise);
                    }
                } else {
                    if (!this.stopped) {
                        await this.blockingRedis.brpop(this.keys.notify, 2);
                    }
                }
            } catch (err) {
                if (this.stopped && err.message.includes("Connection is closed")) break;
                console.error(`[Worker-${workerId}] loop error:`, err);
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    }
    
    async executeJobWithHeartbeat(jobId) {
        const lockKey = `job:${jobId}:lock`;
        let heartbeatInterval;
        
        try {
            await this.redis.set(lockKey, 'locked', 'EX', 30);
            
            heartbeatInterval = setInterval(async () => {
                try {
                    await this.redis.expire(lockKey, 30);
                } catch (e) {
                    // Ignore expire errors on shutdown
                }
            }, 15000);

            await this.executeJob(jobId);

        } finally {
            if (heartbeatInterval) clearInterval(heartbeatInterval);
            await this.redis.del(lockKey).catch(() => {});
        }
    }
    
    async executeJob(jobId) {
        const jobKey = `job:${jobId}`;
        const rawHash = await this.redis.hgetall(jobKey);
        
        if (!rawHash || !rawHash.id) {
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
                await this.redis.del(jobKey);
            }
        } catch (dbErr) {
            console.error(`Failed to save completed job ${jobId} to DB:`, dbErr.message);
        }
    }

    async handleFailure(jobId, rawHash, err) {
        const jobKey = `job:${jobId}`;
        let attemptsMade = (parseInt(rawHash.attemptsMade) || 0) + 1;
        const maxAttempts = parseInt(rawHash.maxAttempts) || 1;
        const baseBackoff = parseInt(rawHash.backoff) || 1000;

        if (attemptsMade < maxAttempts) {
            const jitter = Math.floor(Math.random() * 500);
            const delay = (baseBackoff * Math.pow(2, attemptsMade - 1)) + jitter;
            const runAt = Date.now() + delay;

            const pipeline = this.redis.pipeline();
            pipeline.hset(jobKey, 'attemptsMade', attemptsMade, 'status', 'delayed', 'failedReason', err.message);
            pipeline.zadd(this.keys.delayed, runAt, jobId);
            pipeline.lrem(this.keys.processing, 1, jobId);
            await pipeline.exec();
        } else {
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
                    await this.redis.del(jobKey);
                }
            } catch (dbErr) {
                console.error(`Failed to save DLQ job ${jobId} to DB:`, dbErr.message);
            }
        }
    }
}

module.exports = Worker;
