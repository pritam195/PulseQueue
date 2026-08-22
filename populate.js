const Queue = require('./src/queue');

async function run() {
    const queue = new Queue('myqueue');
    
    // Add completed jobs
    for (let i = 0; i < 5; i++) {
        await queue.add({ task: `Completed ${i}` });
    }
    
    // Add active jobs
    await queue.add({ task: 'Waiting Job 1' });
    await queue.add({ task: 'High Priority Job' }, { priority: 'high' });
    
    // Add delayed job
    await queue.add({ task: 'Run later' }, { delay: 60000 }); // 60s
    
    console.log("Populated.");
    await queue.close();
}

run();
