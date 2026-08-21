const Redis = require('ioredis');
const fs = require('fs');
const path = require('path');

class Worker {
    constructor(queueName, processFn, redisOptions = {}) {
        this.queueName = queueName;
        this.processFn = processFn;
        this.redis = new Redis(redisOptions);
        // We use a separate blocking connection for BRPOP
        this.blockingRedis = new Redis(redisOptions);
        
        this.keys = {
            active: {
                high: `queue:${queueName}:active:high`,
                default: `queue:${queueName}:active:default`,
                low: `queue:${queueName}:active:low`,
            },
            processing: `queue:${queueName}:processing`,
            notify: `queue:${queueName}:notify`,
        };
        
        this.stopped = false;
        
        // Load dequeue Lua script
        const scriptPath = path.join(__dirname, 'lua', 'dequeuePriority.lua');
        const dequeueScript = fs.readFileSync(scriptPath, 'utf8');
        
        this.redis.defineCommand('dequeuePriority', {
            numberOfKeys: 4,
            lua: dequeueScript,
        });
    }
    
    async start() {
        this.stopped = false;
        this.loop(); // Start processing loop asynchronously
    }
    
    async stop() {
        this.stopped = true;
        // Close connections, which will abort any pending blocking pops
        await this.redis.quit();
        await this.blockingRedis.quit();
    }
    
    async loop() {
        while (!this.stopped) {
            try {
                // 1. Try to atomically dequeue from highest priority to processing list
                const jobId = await this.redis.dequeuePriority(
                    this.keys.active.high,
                    this.keys.active.default,
                    this.keys.active.low,
                    this.keys.processing
                );
                
                if (jobId) {
                    await this.executeJob(jobId);
                } else {
                    // 2. If no jobs, block on the notify queue for up to 2 seconds
                    // BLPOP/BRPOP takes an array of keys and a timeout
                    if (!this.stopped) {
                        await this.blockingRedis.brpop(this.keys.notify, 2);
                    }
                }
            } catch (err) {
                // Handle aborted connection errors on stop, or general redis errors
                if (this.stopped && err.message.includes("Connection is closed")) {
                    break;
                }
                console.error("Worker loop error:", err);
                await new Promise(resolve => setTimeout(resolve, 1000)); // Sleep before retry on error
            }
        }
    }
    
    async executeJob(jobId) {
        const jobKey = `job:${jobId}`;
        
        try {
            // Fetch job data
            const jobDataStr = await this.redis.hget(jobKey, 'data');
            const jobData = jobDataStr ? JSON.parse(jobDataStr) : null;
            
            // Mark as active
            await this.redis.hset(jobKey, 'status', 'active');
            
            // Execute the user provided function
            await this.processFn({ id: jobId, data: jobData });
            
            // On Success: Remove from processing and clean up job data (or mark completed)
            // For Phase 1, we just remove it and mark completed.
            const pipeline = this.redis.pipeline();
            pipeline.lrem(this.keys.processing, 1, jobId);
            pipeline.hset(jobKey, 'status', 'completed');
            await pipeline.exec();
            
        } catch (err) {
            console.error(`Job ${jobId} failed:`, err);
            // On Failure: In Phase 2 we will handle retries and DLQ.
            // For now, mark failed and remove from processing.
            const pipeline = this.redis.pipeline();
            pipeline.lrem(this.keys.processing, 1, jobId);
            pipeline.hset(jobKey, 'status', 'failed');
            pipeline.hset(jobKey, 'failedReason', err.message);
            await pipeline.exec();
        }
    }
}

module.exports = Worker;
