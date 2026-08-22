import { useState, useEffect, useCallback } from 'react';
import StatsBar from './components/StatsBar';
import JobTable from './components/JobTable';
import DLQView from './components/DLQView';
import { fetchStats, fetchJobs, fetchDLQ } from './api';
import './index.css';

const QUEUES = ['myqueue', 'relqueue', 'lifecycle'];
const REFRESH_INTERVAL = 3000;

export default function App() {
    const [queue, setQueue] = useState('myqueue');
    const [activeTab, setActiveTab] = useState('waiting');
    const [stats, setStats] = useState(null);
    const [jobs, setJobs] = useState([]);
    const [dlqJobs, setDlqJobs] = useState([]);
    const [loadingStats, setLoadingStats] = useState(true);
    const [loadingJobs, setLoadingJobs] = useState(true);
    const [error, setError] = useState(null);
    const [lastUpdated, setLastUpdated] = useState(null);

    const loadStats = useCallback(async () => {
        try {
            const data = await fetchStats(queue);
            setStats(data);
            setError(null);
        } catch {
            setError('Cannot reach API server. Is it running on port 3001?');
        } finally {
            setLoadingStats(false);
        }
    }, [queue]);

    const loadJobs = useCallback(async () => {
        setLoadingJobs(true);
        try {
            if (activeTab === 'dlq') {
                const data = await fetchDLQ(queue);
                setDlqJobs(data);
            } else {
                const data = await fetchJobs(queue, activeTab);
                setJobs(data);
            }
            setLastUpdated(new Date().toLocaleTimeString());
        } catch {
            // error already shown by stats
        } finally {
            setLoadingJobs(false);
        }
    }, [queue, activeTab]);

    // Initial load + polling
    useEffect(() => {
        loadStats();
        loadJobs();
        const interval = setInterval(() => {
            loadStats();
            loadJobs();
        }, REFRESH_INTERVAL);
        return () => clearInterval(interval);
    }, [loadStats, loadJobs]);

    const tabs = [
        { key: 'waiting',    label: '⏳ Waiting' },
        { key: 'delayed',    label: '🕐 Delayed' },
        { key: 'processing', label: '⚡ Processing' },
        { key: 'dlq',        label: '💀 Dead Letter' },
    ];

    return (
        <div className="app-shell">
            <header>
                <div className="logo">
                    <span className="logo-dot" />
                    PulseQueue
                </div>
                <div className="header-right">
                    <select
                        className="queue-selector"
                        value={queue}
                        onChange={e => setQueue(e.target.value)}
                    >
                        {QUEUES.map(q => (
                            <option key={q} value={q}>{q}</option>
                        ))}
                    </select>
                    {lastUpdated && (
                        <span className="refresh-badge">Updated {lastUpdated}</span>
                    )}
                </div>
            </header>

            <div className="main-content">
                {error && <div className="error-banner">⚠️ {error}</div>}

                <StatsBar stats={stats} loading={loadingStats} />

                <div className="tabs">
                    {tabs.map(t => (
                        <button
                            key={t.key}
                            className={`tab-btn ${activeTab === t.key ? 'active' : ''}`}
                            onClick={() => setActiveTab(t.key)}
                        >
                            {t.label}
                        </button>
                    ))}
                </div>

                <div className="section">
                    <div className="section-header">
                        <span className="section-title">
                            {tabs.find(t => t.key === activeTab)?.label} Jobs
                        </span>
                        <span className={`badge ${activeTab === 'dlq' ? 'red' : ''}`}>
                            {activeTab === 'dlq' ? dlqJobs.length : jobs.length} jobs
                        </span>
                    </div>

                    {activeTab === 'dlq' ? (
                        <DLQView
                            jobs={dlqJobs}
                            loading={loadingJobs}
                            onRetried={() => { loadStats(); loadJobs(); }}
                        />
                    ) : (
                        <JobTable
                            jobs={jobs}
                            status={activeTab}
                            loading={loadingJobs}
                        />
                    )}
                </div>
            </div>
        </div>
    );
}
