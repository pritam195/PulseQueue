const { parentPort, workerData } = require('worker_threads');
const Redis = require('ioredis');
const Worker = require('../src/worker');

// Prevent Mongoose from complaining about unhandled connections in workers
const mongoose = require('mongoose');
mongoose.set('strictQuery', false);

async function run() {
    const queueName = workerData.queueName || 'benchmark_test';
    const concurrency = workerData.concurrency || 1;
    const redis = new Redis();
    
    // We use a small fake delay to simulate real I/O work, or just 0 for pure engine throughput
    const worker = new Worker(queueName, async (job) => {
        const startTimestamp = job.data.t;
        const now = Date.now();
        const latency = now - startTimestamp;
        
        // Push latency metric to a Redis list so the main thread can aggregate them
        await redis.lpush(`metrics:${queueName}:latencies`, latency);
    }, { concurrency });

    worker.start();
    
    parentPort.on('message', async (msg) => {
        if (msg === 'STOP') {
            await worker.stop(5000);
            await redis.quit();
            process.exit(0);
        }
    });
}

run();
