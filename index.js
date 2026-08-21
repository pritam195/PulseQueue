const Queue = require('./src/queue');
const Worker = require('./src/worker');

async function main() {
    console.log("Starting PulseQueue Phase 1 Demonstration...");

    // 1. Initialize Queue
    const queue = new Queue('myqueue');
    queue.startPromoter(1000); // Check for delayed jobs every 1s
    console.log("Queue connected and promoter started.");

    // 2. Initialize Worker
    const worker = new Worker('myqueue', async (job) => {
        console.log(`[Worker] Processing Job ID: ${job.id} | Task: ${job.data.task}`);
        // Simulate async work
        await new Promise(resolve => setTimeout(resolve, 500));
        console.log(`[Worker] Finished Job ID: ${job.id}`);
    });
    
    worker.start();
    console.log("Worker started and listening for jobs...\n");

    // 3. Submit Jobs
    console.log("Submitting standard job...");
    await queue.add({ task: 'Standard Task 1' });

    console.log("Submitting high priority job...");
    await queue.add({ task: 'High Priority Task' }, { priority: 'high' });

    console.log("Submitting delayed job (runs in 3 seconds)...");
    await queue.add({ task: 'Delayed Task' }, { delay: 3000 });

    // Let it run for 5 seconds then shut down
    setTimeout(async () => {
        console.log("\nShutting down...");
        await worker.stop();
        await queue.close();
        console.log("Done.");
        process.exit(0);
    }, 5000);
}

main().catch(err => {
    console.error("Fatal Error:", err);
    process.exit(1);
});
