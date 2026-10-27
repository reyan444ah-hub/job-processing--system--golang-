import { ArrowUpRight, RotateCcw } from 'lucide-react'
import { Link } from 'react-router-dom'
import StatusBadge from './StatusBadge'

function shortId(id = '') {
  return `${String(id).slice(0, 8)}…`
}

function formatDate(value) {
  if (!value) return '—'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? '—' : date.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
}

export default function JobTable({ jobs = [], compact = false, readOnly = false }) {
  if (!jobs.length) return null
  return (
    <div className="table-scroll">
      <table className="job-table">
        <thead>
          <tr>
            <th>Job</th>
            <th>Status</th>
            <th>Attempts</th>
            <th>Created</th>
            {!compact && <th aria-label="Open details" />}
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => (
            <tr key={job.id}>
              <td>
                {readOnly ? <span className="job-name">{job.type || 'Untitled job'}</span> : <Link className="job-name" to={`/jobs/${job.id}`}>{job.type || 'Untitled job'}</Link>}
                <span className="job-id"><span className="mono">{shortId(job.id)}</span>{job.priority > 0 && <span className="priority-tag">P{job.priority}</span>}</span>
              </td>
              <td><StatusBadge status={job.status} /></td>
              <td><span className="attempts">{job.attempt_count || 0}<span> / {job.max_attempts || 3}</span></span></td>
              <td className="date-cell">{formatDate(job.created_at)}</td>
              {!compact && <td><Link className="table-action icon-button" to={`/jobs/${job.id}`} aria-label={`View ${job.type} details`}><ArrowUpRight size={17} /></Link></td>}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export { formatDate, shortId }
