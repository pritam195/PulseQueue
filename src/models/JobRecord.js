const mongoose = require('mongoose');

const jobRecordSchema = new mongoose.Schema({
    jobId: {
        type: String,
        required: true,
        unique: true,
        index: true
    },
    queueName: {
        type: String,
        required: true,
        index: true
    },
    data: {
        type: mongoose.Schema.Types.Mixed,
        required: true
    },
    priority: {
        type: String,
        enum: ['low', 'default', 'high'],
        default: 'default'
    },
    status: {
        type: String,
        enum: ['completed', 'failed_dlq'],
        required: true,
        index: true
    },
    attemptsMade: {
        type: Number,
        default: 0
    },
    maxAttempts: {
        type: Number,
        default: 1
    },
    failedReason: {
        type: String
    },
    createdAt: {
        type: Date,
        default: Date.now
    },
    finishedAt: {
        type: Date,
        default: Date.now
    }
});

module.exports = mongoose.model('JobRecord', jobRecordSchema);
