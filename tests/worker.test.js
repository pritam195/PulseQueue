const Queue = require('../src/queue');
const Worker = require('../src/worker');
const Redis = require('ioredis');

describe('Worker Core Engine (Problem 1 & 2)', () => {
    let queue;
    let redis;

    beforeAll(() => {
        redis = new Redis();
    });

    beforeEach(async () => {
        queue = new Queue('testqueue');
        await redis.flushdb();
    });

    afterAll(async () => {
        await queue.close();
        await redis.quit();
    });

    test('Concurrent workers should not duplicate jobs', async () => {
        const numJobs = 100;
        const numWorkers = 10;
        
        // Add 100 jobs
        for (let i = 0; i < numJobs; i++) {
            await queue.add({ idx: i });
        }
        
        const processedJobs = new Set();
        let duplicatesFound = false;
        let processedCount = 0;
        
        // Create a process function that simulates async work
        const processFn = async (job) => {
            // Simulate work
            await new Promise(resolve => setTimeout(resolve, Math.random() * 10));
            
            if (processedJobs.has(job.id)) {
                duplicatesFound = true;
            }
            processedJobs.add(job.id);
            processedCount++;
        };
        
        // Start 10 concurrent workers
        const workers = [];
        for (let i = 0; i < numWorkers; i++) {
            const worker = new Worker('testqueue', processFn);
            workers.push(worker);
            worker.start();
        }
        
        // Wait until all jobs are processed
        while (processedCount < numJobs) {
            await new Promise(resolve => setTimeout(resolve, 50));
        }
        
        // Stop all workers
        for (const worker of workers) {
            await worker.stop();
        }
        
        expect(duplicatesFound).toBe(false);
        expect(processedJobs.size).toBe(numJobs);
        
        // Active queues should be empty
        const defaultJobs = await redis.llen(queue.keys.active.default);
        expect(defaultJobs).toBe(0);
        
        // Processing queue should be empty
        const processingJobs = await redis.llen(queue.keys.processing);
        expect(processingJobs).toBe(0);
    }, 10000); // 10s timeout
    
    test('Workers process jobs in priority order', async () => {
        const processedOrder = [];
        const processFn = async (job) => {
            processedOrder.push(job.data.task);
        };
        
        // Submit jobs in reverse priority order
        await queue.add({ task: 'low1' }, { priority: 'low' });
        await queue.add({ task: 'default1' });
        await queue.add({ task: 'high1' }, { priority: 'high' });
        await queue.add({ task: 'high2' }, { priority: 'high' });
        
        const worker = new Worker('testqueue', processFn);
        worker.start();
        
        // Wait for all 4 to be processed
        while (processedOrder.length < 4) {
            await new Promise(resolve => setTimeout(resolve, 50));
        }
        
        await worker.stop();
        
        // High priority first, then default, then low
        expect(processedOrder).toEqual(['high1', 'high2', 'default1', 'low1']);
    });
});
