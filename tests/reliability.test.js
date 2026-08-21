const mongoose = require('mongoose');
const { MongoMemoryServer } = require('mongodb-memory-server');
const Redis = require('ioredis');
const Queue = require('../src/queue');
const Worker = require('../src/worker');
const Reaper = require('../src/reaper');
const JobRecord = require('../src/models/JobRecord');

describe('Phase 2: Reliability (Problems 3 & 4)', () => {
    let mongoServer;
    let redis;
    let queue;
    let reaper;

    beforeAll(async () => {
        // Start in-memory MongoDB
        mongoServer = await MongoMemoryServer.create();
        const uri = mongoServer.getUri();
        await mongoose.connect(uri);

        redis = new Redis(); // Expects localhost:6379
    });

    beforeEach(async () => {
        await redis.flushdb();
        await JobRecord.deleteMany({});
        queue = new Queue('relqueue');
        reaper = new Reaper('relqueue');
    });

    afterAll(async () => {
        await mongoose.disconnect();
        await mongoServer.stop();
        await redis.quit();
    });

    test('should retry job with backoff when it throws an error', async () => {
        const jobId = await queue.add({ task: 'fail-once' }, { maxAttempts: 3, backoff: 100 });
        
        let attempt = 0;
        let lastExecTime = 0;
        
        const worker = new Worker('relqueue', async (job) => {
            attempt++;
            if (attempt === 1) {
                lastExecTime = Date.now();
                throw new Error("Temporary DB failure");
            }
            // Second attempt succeeds
        });
        
        worker.start();
        
        // Wait for first attempt to fail
        await new Promise(r => setTimeout(r, 100));
        
        // Verify it was moved to delayed ZSET with a backoff
        const delayedCount = await redis.zcard(queue.keys.delayed);
        expect(delayedCount).toBe(1);
        
        // Wait for backoff to expire and promoter to move it
        await queue.promoteDelayed(); // might be too early
        
        // Fast forward
        await new Promise(r => setTimeout(r, 200)); 
        await queue.promoteDelayed();
        
        // Wait for it to process successfully
        await new Promise(r => setTimeout(r, 100));
        
        await worker.stop();
        
        expect(attempt).toBe(2);
        
        // Should be in MongoDB as completed
        const record = await JobRecord.findOne({ jobId });
        expect(record).toBeTruthy();
        expect(record.status).toBe('completed');
    });

    test('should route to DLQ (MongoDB) after exhausting maxAttempts', async () => {
        const jobId = await queue.add({ task: 'fail-always' }, { maxAttempts: 2, backoff: 10 });
        
        const worker = new Worker('relqueue', async (job) => {
            throw new Error("Permanent Error");
        });
        
        worker.start();
        queue.startPromoter(50); // fast promoter for tests
        
        // Wait for both attempts
        await new Promise(r => setTimeout(r, 500));
        
        await worker.stop();
        queue.stopPromoter();
        
        // Redis should be clean
        const exists = await redis.exists(`job:${jobId}`);
        expect(exists).toBe(0);
        
        // MongoDB should have it as failed_dlq
        const record = await JobRecord.findOne({ jobId });
        expect(record).toBeTruthy();
        expect(record.status).toBe('failed_dlq');
        expect(record.attemptsMade).toBe(2);
        expect(record.failedReason).toBe('Permanent Error');
    });

    test('Reaper should recover a job when a worker crashes (loses lock)', async () => {
        const jobId = await queue.add({ task: 'crash-job' }, { maxAttempts: 2 });
        
        const worker = new Worker('relqueue', async (job) => {
            // Simulate crash: Delete lock manually from Redis
            await redis.del(`job:${jobId}:lock`);
            
            // Hang forever (worker is "dead")
            await new Promise(() => {});
        });
        
        worker.start();
        
        // Give it time to pick up the job and delete lock
        await new Promise(r => setTimeout(r, 100));
        
        // Ensure job is in processing
        const processingLen = await redis.llen(queue.keys.processing);
        expect(processingLen).toBe(1);
        
        // Run reaper manually
        await reaper.reap();
        
        // Job should be requeued to delayed (immediate retry)
        const delayedLen = await redis.zcard(queue.keys.delayed);
        expect(delayedLen).toBe(1);
        
        // Promotor brings it back
        await queue.promoteDelayed();
        
        // Clean up
        worker.stopped = true; // force stop since it's hung
        await worker.stop();
    });
});
