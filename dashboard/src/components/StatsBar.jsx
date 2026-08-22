export default function StatsBar({ stats, loading }) {
    const cards = [
        { key: 'waiting',    label: 'Waiting',    icon: '⏳' },
        { key: 'delayed',    label: 'Delayed',    icon: '🕐' },
        { key: 'processing', label: 'Processing', icon: '⚡' },
        { key: 'completed',  label: 'Completed',  icon: '✅' },
        { key: 'dlq',        label: 'Dead Letter', icon: '💀' },
    ];

    return (
        <div className="stats-bar">
            {cards.map(({ key, label, icon }) => (
                <div key={key} className={`stat-card ${key}`}>
                    <span className="stat-label">{icon} {label}</span>
                    <span className="stat-value">
                        {loading ? '—' : (stats?.[key] ?? 0)}
                    </span>
                </div>
            ))}
        </div>
    );
}
