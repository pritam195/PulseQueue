const { fork } = require('child_process');
const path = require('path');
const fs = require('fs');
const Redis = require('ioredis');
const Queue = require('../src/queue');
const Reaper = require('../src/reaper');
const mongoose = require('mongoose');

const QUEUE_NAME = 'chaos_test';
const redis = new Redis();

const logTrace = [];
function log(msg) {
    const timestamp = new Date().toISOString().split('T')[1].slice(0, -1); // e.g. 10:25:30.123
    const line = `[${timestamp}] ${msg}`;
    console.log(line);
    logTrace.push(line);
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function spawnWorker(name) {
    const workerProcess = fork(path.join(__dirname, 'chaos_worker.js'));
    
    workerProcess.on('message', (msg) => {
        if (msg.event === 'JOB_STARTED') {
            log(`[${name}] Acquired lease and started job ${msg.jobId}`);
            
            // If this is Worker A, we murder it!
            if (name === 'Worker A') {
                setTimeout(() => {
                    log(`💥 MURDERING Worker A (PID: ${workerProcess.pid}) with SIGKILL mid-job...`);
                    process.kill(workerProcess.pid, 'SIGKILL');
                }, 2000); // Let it process for 2 seconds before killing
            }
        }
        if (msg.event === 'JOB_FINISHED') {
            log(`[${name}] Successfully finished job ${msg.jobId}! Data is SAFE.`);
            finishTest();
        }
    });

    workerProcess.on('exit', (code, signal) => {
        if (signal === 'SIGKILL') {
            log(`[${name}] Process died via SIGKILL.`);
        }
    });
    
    return workerProcess;
}

let workerB;

async function finishTest() {
    log('--- CHAOS TEST SUCCESSFUL ---');
    
    fs.writeFileSync(path.join(__dirname, 'chaostest_trace.log'), logTrace.join('\n'));
    console.log('\nTrace saved to benchmark/chaostest_trace.log');
    
    if (workerB) workerB.kill();
    await redis.quit();
    process.exit(0);
}

async function main() {
    await redis.flushdb();
    
    log('Starting Chaos Test...');
    
    // 1. Start Reaper with a very aggressive interval (2 seconds)
    // It will look for locks that have expired (our chaos worker uses 5s locks)
    const reaper = new Reaper(QUEUE_NAME);
    setInterval(() => reaper.reap(), 2000);
    log('Reaper started (Checking every 2s for expired locks).');
    
    // 2. Start Worker A
    log('Spawning Worker A...');
    const workerA = spawnWorker('Worker A');
    
    // 3. Queue a Job
    await sleep(1000); // Give worker A a second to boot
    const queue = new Queue(QUEUE_NAME);
    queue.startPromoter(1000); // Check delayed queue every 1s
    const jobId = await queue.add({ task: 'Process extremely important payment' }, { maxAttempts: 3 });
    log(`Enqueued job ${jobId}`);
    
    // 4. Wait for the murder...
    await sleep(4000);
    
    // 5. Spawn Worker B as the backup
    log('Spawning Worker B (The backup node)...');
    workerB = spawnWorker('Worker B');
    
    // Now we wait for the Reaper to detect Worker A's expired lock (approx 5s after start)
    // and push the job back to the delayed queue/active queue for Worker B.
}

main().catch(console.error);
