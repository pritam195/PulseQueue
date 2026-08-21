const Redis = require('ioredis');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

class Queue {
    constructor(queueName, redisOptions = {}) {
        this.queueName = queueName;
        this.redis = new Redis(redisOptions);
        
        this.keys = {
            delayed: `queue:${queueName}:delayed`,
            active: {
                high: `queue:${queueName}:active:high`,
                default: `queue:${queueName}:active:default`,
                low: `queue:${queueName}:active:low`,
            },
            processing: `queue:${queueName}:processing`,
            notify: `queue:${queueName}:notify`,
        };
        
        // Define Lua script for promoting delayed jobs
        const scriptPath = path.join(__dirname, 'lua', 'promoteDelayed.lua');
        const promoteScript = fs.readFileSync(scriptPath, 'utf8');
        
        this.redis.defineCommand('promoteDelayed', {
            numberOfKeys: 5,
            lua: promoteScript,
        });
    }

    /**
     * Submit a job to the queue
     * @param {Object} data - The job payload
     * @param {Object} options - { delay (ms), priority ('high', 'default', 'low') }
     * @returns {string} jobId
     */
    async add(data, options = {}) {
        const jobId = crypto.randomUUID();
        const priority = options.priority || 'default';
        const delay = options.delay || 0;
        const maxAttempts = options.maxAttempts || 1;
        const backoff = options.backoff || 1000; // Base backoff in ms
        
        const jobKey = `job:${jobId}`;
        const createdAt = Date.now();
        
        // Use a pipeline to ensure atomic creation
        const pipeline = this.redis.pipeline();
        
        // 1. Store job metadata and payload
        pipeline.hset(jobKey, 
            'id', jobId,
            'data', JSON.stringify(data),
            'priority', priority,
            'createdAt', createdAt,
            'status', delay > 0 ? 'delayed' : 'waiting',
            'attemptsMade', 0,
            'maxAttempts', maxAttempts,
            'backoff', backoff
        );
        
        // 2. Add to appropriate queue
        if (delay > 0) {
            const runAt = createdAt + delay;
            pipeline.zadd(this.keys.delayed, runAt, jobId);
        } else {
            const activeKey = this.keys.active[priority];
            // Push to the left, workers will pop from the right using RPOPLPUSH
            pipeline.lpush(activeKey, jobId);
            // Notify blocked workers
            pipeline.lpush(this.keys.notify, '1');
        }
        
        await pipeline.exec();
        return jobId;
    }

    /**
     * Poll the delayed ZSET and move ready jobs to active queues
     * @param {number} maxJobs - Maximum jobs to promote at once
     * @returns {number} number of promoted jobs
     */
    async promoteDelayed(maxJobs = 100) {
        const now = Date.now();
        const numPromoted = await this.redis.promoteDelayed(
            this.keys.delayed,
            this.keys.active.high,
            this.keys.active.default,
            this.keys.active.low,
            this.keys.notify,
            now,
            maxJobs
        );
        return numPromoted;
    }
    
    /**
     * Start an interval to automatically promote delayed jobs
     * @param {number} intervalMs 
     */
    startPromoter(intervalMs = 1000) {
        if (this.promoterInterval) return;
        this.promoterInterval = setInterval(async () => {
            try {
                await this.promoteDelayed();
            } catch (err) {
                console.error("Promoter error:", err);
            }
        }, intervalMs);
    }
    
    stopPromoter() {
        if (this.promoterInterval) {
            clearInterval(this.promoterInterval);
            this.promoterInterval = null;
        }
    }

    async close() {
        this.stopPromoter();
        await this.redis.quit();
    }
}

module.exports = Queue;
