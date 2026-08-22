function formatTime(ts) {
    if (!ts) return '—';
    return new Date(ts).toLocaleTimeString();
}

function StatusPill({ status }) {
    return <span className={`status-pill ${status}`}>{status}</span>;
}

function PriorityPill({ priority }) {
    return <span className={`priority-pill ${priority || 'default'}`}>{priority || 'default'}</span>;
}

export default function JobTable({ jobs, status, loading }) {
    if (loading) {
        return (
            <div className="empty-state">
                <span className="empty-state-icon">⟳</span>
                <span>Loading jobs...</span>
            </div>
        );
    }

    if (!jobs || jobs.length === 0) {
        return (
            <div className="empty-state">
                <span className="empty-state-icon">📭</span>
                <span>No {status} jobs right now</span>
            </div>
        );
    }

    return (
        <table>
            <thead>
                <tr>
                    <th>Job ID</th>
                    <th>Payload</th>
                    <th>Priority</th>
                    <th>Status</th>
                    <th>Attempts</th>
                    <th>Created</th>
                </tr>
            </thead>
            <tbody>
                {jobs.map(job => (
                    <tr key={job.id}>
                        <td><span className="mono">{job.id?.slice(0, 14)}…</span></td>
                        <td>
                            <span className="mono" title={JSON.stringify(job.data)}>
                                {JSON.stringify(job.data)?.slice(0, 40)}
                            </span>
                        </td>
                        <td><PriorityPill priority={job.priority} /></td>
                        <td><StatusPill status={job.status} /></td>
                        <td style={{ color: 'var(--text-primary)' }}>
                            {job.attemptsMade}/{job.maxAttempts}
                        </td>
                        <td>{formatTime(job.createdAt)}</td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
}
