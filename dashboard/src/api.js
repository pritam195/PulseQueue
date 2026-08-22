const API_BASE = 'http://localhost:3001/api';

export async function fetchStats(queue) {
    const res = await fetch(`${API_BASE}/stats?queue=${queue}`);
    if (!res.ok) throw new Error('Failed to fetch stats');
    return res.json();
}

export async function fetchJobs(queue, status) {
    const res = await fetch(`${API_BASE}/jobs?queue=${queue}&status=${status}`);
    if (!res.ok) throw new Error('Failed to fetch jobs');
    return res.json();
}

export async function fetchDLQ(queue) {
    const res = await fetch(`${API_BASE}/dlq?queue=${queue}`);
    if (!res.ok) throw new Error('Failed to fetch DLQ');
    return res.json();
}

export async function retryDLQJob(jobId) {
    const res = await fetch(`${API_BASE}/dlq/${jobId}/retry`, { method: 'POST' });
    if (!res.ok) throw new Error('Failed to retry job');
    return res.json();
}
