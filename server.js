const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const apiRoutes = require('./src/api/routes');

const app = express();
const PORT = process.env.PORT || 3001;
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/pulsequeue';

app.use(cors());
app.use(express.json());

// Mount API routes
app.use('/api', apiRoutes);

// Health check
app.get('/health', (req, res) => res.json({ status: 'ok' }));

async function start() {
    try {
        await mongoose.connect(MONGO_URI);
        console.log('Connected to MongoDB');
    } catch (err) {
        console.warn('MongoDB not available — DLQ and history features will be limited:', err.message);
    }

    app.listen(PORT, () => {
        console.log(`PulseQueue API server running on http://localhost:${PORT}`);
    });
}

start();
