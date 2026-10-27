export default function Loading({ label = 'Loading data…', compact = false }) {
  return (
    <div className={`loading-state${compact ? ' loading-compact' : ''}`} role="status">
      <span className="spinner" />
      <span>{label}</span>
    </div>
  )
}
