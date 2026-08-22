const mongoose = require('mongoose');
const { MongoMemoryServer } = require('mongodb-memory-server');
const Redis = require('ioredis');
const Queue = require('../src/queue');
const Worker = require('../src/worker');

describe('Phase 3: Concurrency & Graceful Shutdown', () => {
    let mongoServer;
    let redis;
    let queue;

    beforeAll(async () => {
        mongoServer = await MongoMemoryServer.create();
        const uri = mongoServer.getUri();
        await mongoose.connect(uri);
        redis = new Redis();
    });

    beforeEach(async () => {
        await redis.flushdb();
        queue = new Queue('lifecycle');
    });

    afterAll(async () => {
        await mongoose.disconnect();
        await mongoServer.stop();
        await redis.quit();
    });

    test('should process exactly N jobs concurrently', async () => {
        // Add 10 jobs
        for (let i = 0; i < 10; i++) {
            await queue.add({ task: `job-${i}` });
        }
        
        let concurrentExecutions = 0;
        let maxObservedConcurrency = 0;
        let completedJobs = 0;
        
        const worker = new Worker('lifecycle', async (job) => {
            concurrentExecutions++;
            if (concurrentExecutions > maxObservedConcurrency) {
                maxObservedConcurrency = concurrentExecutions;
            }
            
            // Hold the job open for 100ms
            await new Promise(r => setTimeout(r, 100));
            
            concurrentExecutions--;
            completedJobs++;
        }, { concurrency: 3 });
        
        worker.start();
        
        // Wait until all 10 jobs are finished
        while (completedJobs < 10) {
            await new Promise(r => setTimeout(r, 50));
        }
        
        await worker.stop();
        
        // At no point should it have exceeded 3, but it should have hit 3
        expect(maxObservedConcurrency).toBeLessThanOrEqual(3);
        // It's highly likely it hit exactly 3 given 10 jobs and 100ms processing time
        expect(maxObservedConcurrency).toBe(3);
    });

    test('should gracefully shutdown and finish active jobs without pulling new ones', async () => {
        for (let i = 0; i < 5; i++) {
            await queue.add({ task: `job-${i}` });
        }
        
        let startedJobs = 0;
        let finishedJobs = 0;
        
        const worker = new Worker('lifecycle', async (job) => {
            startedJobs++;
            // Each job takes 200ms
            await new Promise(r => setTimeout(r, 200));
            finishedJobs++;
        }, { concurrency: 2 });
        
        worker.start();
        
        // Let it pull exactly 2 jobs (since concurrency is 2)
        await new Promise(r => setTimeout(r, 50));
        
        // Issue stop signal immediately!
        // This should block until the 2 currently active jobs finish (takes ~150ms more)
        const stopStartTime = Date.now();
        await worker.stop(5000); // 5s timeout
        const stopDuration = Date.now() - stopStartTime;
        
        // Worker should have waited for the jobs to finish
        expect(stopDuration).toBeGreaterThanOrEqual(100);
        
        // Only 2 jobs should have been started because it stopped pulling
        expect(startedJobs).toBe(2);
        
        // And those 2 jobs should have finished successfully
        expect(finishedJobs).toBe(2);
        
        // 3 jobs should still be lingering in the active queue untouched
        const remaining = await redis.llen(queue.keys.active.default);
        expect(remaining).toBe(3);
    });
});
