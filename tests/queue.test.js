const Queue = require('../src/queue');
const Redis = require('ioredis');

describe('Queue Core Engine', () => {
    let queue;
    let redis;

    beforeAll(() => {
        redis = new Redis();
    });

    beforeEach(async () => {
        queue = new Queue('testqueue');
        await redis.flushdb(); // Clear out all keys for clean state
    });

    afterAll(async () => {
        await queue.close();
        await redis.quit();
    });

    test('should add jobs to correct priority queue', async () => {
        const jobId1 = await queue.add({ task: 'low-task' }, { priority: 'low' });
        const jobId2 = await queue.add({ task: 'high-task' }, { priority: 'high' });
        const jobId3 = await queue.add({ task: 'default-task' });

        // Check active lists directly
        const highJobs = await redis.lrange(queue.keys.active.high, 0, -1);
        const lowJobs = await redis.lrange(queue.keys.active.low, 0, -1);
        const defaultJobs = await redis.lrange(queue.keys.active.default, 0, -1);

        expect(highJobs).toContain(jobId2);
        expect(lowJobs).toContain(jobId1);
        expect(defaultJobs).toContain(jobId3);
        
        // Notify key should have 3 items
        const notifies = await redis.llen(queue.keys.notify);
        expect(notifies).toBe(3);
    });

    test('should add delayed job to delayed ZSET', async () => {
        const jobId = await queue.add({ task: 'delayed-task' }, { delay: 1000 });
        
        const delayedCount = await redis.zcard(queue.keys.delayed);
        expect(delayedCount).toBe(1);
        
        const notifies = await redis.llen(queue.keys.notify);
        expect(notifies).toBe(0); // Delayed jobs shouldn't notify until promoted
    });

    test('should promote delayed jobs when time is reached', async () => {
        // Add job with no delay but specify it manually to simulate time passing
        const jobId = await queue.add({ task: 'delayed-task' }, { delay: 1000 });
        
        // Try promoting immediately, should promote 0
        let promoted = await queue.promoteDelayed();
        expect(promoted).toBe(0);
        
        // Fast forward time in redis by modifying the score of the job in ZSET
        const now = Date.now();
        await redis.zadd(queue.keys.delayed, now - 1000, jobId);
        
        promoted = await queue.promoteDelayed();
        expect(promoted).toBe(1);
        
        // It should now be in the default active list
        const defaultJobs = await redis.lrange(queue.keys.active.default, 0, -1);
        expect(defaultJobs).toContain(jobId);
        
        // And notify should have been called
        const notifies = await redis.llen(queue.keys.notify);
        expect(notifies).toBe(1);
    });
});
