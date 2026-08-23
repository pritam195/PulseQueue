const Worker = require('../src/worker');

// Fast lock TTL so the Reaper picks it up in 5 seconds instead of 30
const worker = new Worker('chaos_test', async (job) => {
    console.log(`[Worker ${process.pid}] Started processing job ${job.id}`);
    
    // Tell orchestrator we started
    process.send({ event: 'JOB_STARTED', jobId: job.id, pid: process.pid });
    
    // Simulate a long running task (10 seconds)
    // The orchestrator will murder us halfway through this!
    await new Promise(resolve => setTimeout(resolve, 10000));
    
    console.log(`[Worker ${process.pid}] Finished job ${job.id}`);
    process.send({ event: 'JOB_FINISHED', jobId: job.id });
}, { lockTTL: 5 });

worker.start();
console.log(`[Worker ${process.pid}] Booted and listening for jobs...`);
