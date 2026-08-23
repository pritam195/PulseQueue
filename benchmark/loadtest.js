const { Worker: NodeWorker } = require('worker_threads');
const path = require('path');
const os = require('os');
const fs = require('fs');
const Redis = require('ioredis');
const Queue = require('../src/queue');

// Parse simple CLI args: --workers=10 --jobs=5000
const args = process.argv.slice(2).reduce((acc, arg) => {
    const [key, val] = arg.split('=');
    if (key.startsWith('--')) {
        acc[key.slice(2)] = parseInt(val, 10);
    }
    return acc;
}, {});

const NUM_WORKERS = args.workers || 4;
const NUM_JOBS = args.jobs || 1000;
const CONCURRENCY_PER_WORKER = 5;
const QUEUE_NAME = 'benchmark_test';

const redis = new Redis();
const queue = new Queue(QUEUE_NAME);

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Simple CPU measurement using process.cpuUsage() over a time window
async function measureIdleCPU(durationMs) {
    console.log(`\n[Idle Test] Measuring idle CPU for ${durationMs / 1000}s with ${NUM_WORKERS} workers connected via BRPOP...`);
    
    // Spawn workers early
    const threads = [];
    for (let i = 0; i < NUM_WORKERS; i++) {
        const t = new NodeWorker(path.join(__dirname, 'worker_thread.js'), {
            workerData: { queueName: QUEUE_NAME, concurrency: CONCURRENCY_PER_WORKER }
        });
        threads.push(t);
    }

    // Wait 2 seconds for workers to connect
    await sleep(2000);
    
    const startUsage = process.cpuUsage();
    await sleep(durationMs);
    const endUsage = process.cpuUsage(startUsage);
    
    // Total CPU time in microseconds
    const totalCpuTimeMs = (endUsage.user + endUsage.system) / 1000;
    // CPU % = (Total CPU Time / Elapsed Wall Clock Time) * 100
    // Multiplied by 100 to get percentage
    const cpuPercent = (totalCpuTimeMs / durationMs) * 100;
    
    console.log(`[Idle Test] Average CPU Usage: ${cpuPercent.toFixed(2)}%`);
    return { threads, cpuPercent };
}

async function runLoadTest(threads) {
    console.log(`\n[Load Test] Enqueuing ${NUM_JOBS} jobs across ${NUM_WORKERS} workers...`);
    
    const startTime = Date.now();
    
    // Fire off all jobs asynchronously
    const enqueuePromises = [];
    for (let i = 0; i < NUM_JOBS; i++) {
        // Embed the exact enqueue time into the job payload for latency tracking
        enqueuePromises.push(queue.add({ t: Date.now() }));
    }
    await Promise.all(enqueuePromises);
    const enqueueTime = Date.now() - startTime;
    console.log(`[Load Test] Finished enqueuing in ${enqueueTime}ms.`);
    
    // Wait until all jobs are processed (metrics list will have NUM_JOBS elements)
    const metricsKey = `metrics:${QUEUE_NAME}:latencies`;
    let processed = 0;
    
    const processStart = Date.now();
    while (processed < NUM_JOBS) {
        processed = await redis.llen(metricsKey);
        process.stdout.write(`\r[Load Test] Progress: ${processed}/${NUM_JOBS} ...`);
        await sleep(100);
    }
    
    const totalTimeMs = Date.now() - startTime;
    console.log(`\n[Load Test] All jobs finished in ${totalTimeMs}ms!`);
    
    // Stop all worker threads
    for (const t of threads) {
        t.postMessage('STOP');
    }
    
    // Calculate Percentiles
    const rawLatencies = await redis.lrange(metricsKey, 0, -1);
    const latencies = rawLatencies.map(Number).sort((a, b) => a - b);
    
    const p50 = latencies[Math.floor(NUM_JOBS * 0.5)];
    const p95 = latencies[Math.floor(NUM_JOBS * 0.95)];
    const p99 = latencies[Math.floor(NUM_JOBS * 0.99)];
    
    const throughput = Math.floor((NUM_JOBS / totalTimeMs) * 1000);
    
    return {
        totalTimeMs,
        throughput,
        p50,
        p95,
        p99
    };
}

async function main() {
    // Clean slate
    await redis.flushdb();
    
    // 1. Idle Test (e.g. 10 seconds to prove BRPOP efficiency)
    const { threads, cpuPercent } = await measureIdleCPU(10000);
    
    // 2. Load Test
    const results = await runLoadTest(threads);
    
    const report = {
        config: { workers: NUM_WORKERS, jobs: NUM_JOBS },
        idle_cpu_percent: cpuPercent.toFixed(2),
        throughput_jobs_per_sec: results.throughput,
        total_wall_time_ms: results.totalTimeMs,
        latency_ms: {
            p50: results.p50,
            p95: results.p95,
            p99: results.p99
        }
    };
    
    console.log('\n================ RESULTS ================');
    console.log(JSON.stringify(report, null, 2));
    
    // Save to artifact
    fs.writeFileSync(path.join(__dirname, 'loadtest_results.json'), JSON.stringify(report, null, 2));
    console.log('\nResults saved to benchmark/loadtest_results.json');
    
    await queue.close();
    await redis.quit();
    process.exit(0);
}

main().catch(console.error);
