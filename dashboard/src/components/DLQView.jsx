import { useState } from 'react';
import { retryDLQJob } from '../api';

function formatDate(ts) {
    if (!ts) return '—';
    return new Date(ts).toLocaleString();
}

export default function DLQView({ jobs, loading, onRetried }) {
    const [retrying, setRetrying] = useState({});

    async function handleRetry(jobId) {
        setRetrying(prev => ({ ...prev, [jobId]: true }));
        try {
            await retryDLQJob(jobId);
            onRetried?.();
        } catch (err) {
            alert(`Retry failed: ${err.message}`);
        } finally {
            setRetrying(prev => ({ ...prev, [jobId]: false }));
        }
    }

    if (loading) {
        return (
            <div className="empty-state">
                <span className="empty-state-icon">⟳</span>
                <span>Loading DLQ...</span>
            </div>
        );
    }

    if (!jobs || jobs.length === 0) {
        return (
            <div className="empty-state">
                <span className="empty-state-icon">🎉</span>
                <span>Dead Letter Queue is empty</span>
            </div>
        );
    }

    return (
        <table>
            <thead>
                <tr>
                    <th>Job ID</th>
                    <th>Payload</th>
                    <th>Failure Reason</th>
                    <th>Attempts</th>
                    <th>Failed At</th>
                    <th>Action</th>
                </tr>
            </thead>
            <tbody>
                {jobs.map(job => (
                    <tr key={job.jobId}>
                        <td><span className="mono">{job.jobId?.slice(0, 14)}…</span></td>
                        <td>
                            <span className="mono" title={JSON.stringify(job.data)}>
                                {JSON.stringify(job.data)?.slice(0, 40)}
                            </span>
                        </td>
                        <td style={{ color: 'var(--red)', fontSize: '0.8rem' }}>
                            {job.failedReason || '—'}
                        </td>
                        <td style={{ color: 'var(--text-primary)' }}>
                            {job.attemptsMade}/{job.maxAttempts}
                        </td>
                        <td>{formatDate(job.finishedAt)}</td>
                        <td>
                            <button
                                className="retry-btn"
                                disabled={retrying[job.jobId]}
                                onClick={() => handleRetry(job.jobId)}
                            >
                                {retrying[job.jobId] ? 'Retrying…' : '↩ Retry'}
                            </button>
                        </td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
}
