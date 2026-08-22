const express = require('express');
const Redis = require('ioredis');
const mongoose = require('mongoose');
const Queue = require('../queue');
const JobRecord = require('../models/JobRecord');

const router = express.Router();
const redis = new Redis();

// Helper to get queue keys for a given queue name
function getKeys(queueName) {
    return {
        active: {
            high: `queue:${queueName}:active:high`,
            default: `queue:${queueName}:active:default`,
            low: `queue:${queueName}:active:low`,
        },
        delayed: `queue:${queueName}:delayed`,
        processing: `queue:${queueName}:processing`,
    };
}

// GET /api/stats?queue=myqueue
// Returns job counts across all states for a given queue
router.get('/stats', async (req, res) => {
    try {
        const queueName = req.query.queue || 'myqueue';
        const keys = getKeys(queueName);

        const pipeline = redis.pipeline();
        pipeline.llen(keys.active.high);
        pipeline.llen(keys.active.default);
        pipeline.llen(keys.active.low);
        pipeline.zcard(keys.delayed);
        pipeline.llen(keys.processing);

        const results = await pipeline.exec();

        const waiting =
            (results[0][1] || 0) +
            (results[1][1] || 0) +
            (results[2][1] || 0);

        // MongoDB counts
        const completed = await JobRecord.countDocuments({ queueName, status: 'completed' });
        const dlq = await JobRecord.countDocuments({ queueName, status: 'failed_dlq' });

        res.json({
            waiting,
            delayed: results[3][1] || 0,
            processing: results[4][1] || 0,
            completed,
            dlq,
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET /api/jobs?queue=myqueue&status=waiting
// Returns job list from Redis (waiting, delayed, processing)
router.get('/jobs', async (req, res) => {
    try {
        const queueName = req.query.queue || 'myqueue';
        const status = req.query.status || 'waiting';
        const keys = getKeys(queueName);

        let jobIds = [];

        if (status === 'waiting') {
            const [high, def, low] = await Promise.all([
                redis.lrange(keys.active.high, 0, 49),
                redis.lrange(keys.active.default, 0, 49),
                redis.lrange(keys.active.low, 0, 49),
            ]);
            jobIds = [...high, ...def, ...low];
        } else if (status === 'delayed') {
            jobIds = await redis.zrange(keys.delayed, 0, 49);
        } else if (status === 'processing') {
            jobIds = await redis.lrange(keys.processing, 0, 49);
        }

        // Fetch hash for each jobId
        const pipeline = redis.pipeline();
        for (const id of jobIds) {
            pipeline.hgetall(`job:${id}`);
        }

        const results = await pipeline.exec();
        const jobs = results
            .map(([, hash]) => hash)
            .filter(Boolean)
            .map(h => ({
                id: h.id,
                data: h.data ? JSON.parse(h.data) : null,
                priority: h.priority,
                status: h.status,
                attemptsMade: parseInt(h.attemptsMade) || 0,
                maxAttempts: parseInt(h.maxAttempts) || 1,
                createdAt: parseInt(h.createdAt),
            }));

        res.json(jobs);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET /api/dlq?queue=myqueue
// Returns all DLQ entries from MongoDB
router.get('/dlq', async (req, res) => {
    try {
        const queueName = req.query.queue || 'myqueue';
        const jobs = await JobRecord
            .find({ queueName, status: 'failed_dlq' })
            .sort({ finishedAt: -1 })
            .limit(100)
            .lean();
        res.json(jobs);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// POST /api/dlq/:jobId/retry
// Re-adds a DLQ job back into the active queue
router.post('/dlq/:jobId/retry', async (req, res) => {
    try {
        const { jobId } = req.params;
        const record = await JobRecord.findOne({ jobId, status: 'failed_dlq' });

        if (!record) {
            return res.status(404).json({ error: 'DLQ job not found' });
        }

        const queue = new Queue(record.queueName);
        const newJobId = await queue.add(record.data, {
            priority: record.priority,
            maxAttempts: record.maxAttempts,
        });
        await queue.close();

        // Remove from DLQ after retry
        await JobRecord.deleteOne({ jobId });

        res.json({ success: true, newJobId });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

module.exports = router;
