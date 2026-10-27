import { useCallback, useEffect, useState } from 'react'
import { ArrowLeft, Ban, CalendarClock, Check, Clock3, Copy, RefreshCw, RotateCcw, UserRound, Zap } from 'lucide-react'
import { Link, useParams } from 'react-router-dom'
import EmptyState from '../components/EmptyState'
import Loading from '../components/Loading'
import StatusBadge from '../components/StatusBadge'
import { useToast } from '../context/ToastContext'
import { getApiError } from '../services/api'
import { cancelJob, getJob, getJobEvents, retryJob, formatDate } from '../services/jobs'

function JsonBlock({ value }) {
  if (value == null || value === '') return <span className="detail-empty">No data</span>
  let formatted = value
  if (typeof value === 'string') {
    try { formatted = JSON.parse(value) } catch { formatted = value }
  }
  return <pre className="json-block">{JSON.stringify(formatted, null, 2)}</pre>
}

export default function JobDetailsPage() {
  const { jobId } = useParams()
  const [job, setJob] = useState(null)
  const [events, setEvents] = useState([])
  const [loading, setLoading] = useState(true)
  const [actionLoading, setActionLoading] = useState(false)
  const [error, setError] = useState('')
  const { notify } = useToast()

  const load = useCallback(async () => {
    setError('')
    try {
      const [nextJob, nextEvents] = await Promise.all([getJob(jobId), getJobEvents(jobId)])
      setJob(nextJob)
      setEvents(nextEvents)
    } catch (loadError) {
      setError(getApiError(loadError))
    } finally {
      setLoading(false)
    }
  }, [jobId])

  useEffect(() => { load() }, [load])
  useEffect(() => {
    const onEvent = (event) => {
      if (event.detail?.aggregate_id === jobId) load()
    }
    window.addEventListener('workstream:job-event', onEvent)
    return () => window.removeEventListener('workstream:job-event', onEvent)
  }, [jobId, load])

  async function doAction(action) {
    setActionLoading(true)
    try {
      const updated = action === 'cancel' ? await cancelJob(jobId) : await retryJob(jobId)
      setJob(updated)
      notify(action === 'cancel' ? 'Job cancelled.' : 'Job re-enqueued.', 'success')
      await load()
    } catch (actionError) {
      notify(getApiError(actionError), 'error')
    } finally {
      setActionLoading(false)
    }
  }

  async function copyJobId() {
    try {
      await navigator.clipboard.writeText(jobId)
      notify('Job ID copied.', 'success')
    } catch {
      notify('Clipboard access was denied by the browser.', 'error')
    }
  }

  if (loading) return <Loading label="Loading job details…" />
  if (error || !job) return <div className="page-stack"><Link className="back-link" to="/jobs"><ArrowLeft size={15} />Back to jobs</Link><div className="inline-error" role="alert"><span>{error || 'Job not found.'}</span><button className="button button-quiet" onClick={load}><RefreshCw size={14} />Retry</button></div></div>

  const canCancel = ['queued', 'retrying'].includes(job.status)
  const canRetry = ['dead_letter', 'failed'].includes(job.status)

  return (
    <div className="page-stack">
      <Link className="back-link" to="/jobs"><ArrowLeft size={15} />Back to jobs</Link>
      <section className="detail-hero panel">
        <div className="detail-main"><span className="eyebrow">JOB RECORD</span><h2>{job.type}</h2><div className="detail-id"><code>{job.id}</code><button className="icon-button" onClick={copyJobId} aria-label="Copy job ID"><Copy size={14} /></button></div></div>
        <div className="detail-state"><StatusBadge status={job.status} />{canCancel && <button className="button button-secondary" disabled={actionLoading} onClick={() => doAction('cancel')}><Ban size={15} />Cancel</button>}{canRetry && <button className="button button-primary" disabled={actionLoading} onClick={() => doAction('retry')}><RotateCcw size={15} />Retry job</button>}</div>
      </section>

      <div className="detail-grid">
        <section className="panel detail-panel"><div className="panel-title"><Zap size={16} /><h3>Execution details</h3></div><div className="detail-stat-grid"><div><span>Attempt</span><strong>{job.attempt_count || 0} <small>/ {job.max_attempts || 3}</small></strong></div><div><span>Priority</span><strong>{job.priority ?? 0}</strong></div><div><span>Version</span><strong>{job.version ?? '—'}</strong></div><div><span>Worker</span><strong className="worker-name">{job.locked_by || 'Not assigned'}</strong></div></div><div className="detail-meta"><span><Clock3 size={14} />Created</span><strong>{formatDate(job.created_at)}</strong></div>{job.started_at && <div className="detail-meta"><span><ActivityIcon />Started</span><strong>{formatDate(job.started_at)}</strong></div>}{job.scheduled_at && <div className="detail-meta"><span><CalendarClock size={14} />Scheduled</span><strong>{formatDate(job.scheduled_at)}</strong></div>}{job.completed_at && <div className="detail-meta"><span><Check size={14} />Finished</span><strong>{formatDate(job.completed_at)}</strong></div>}</section>
        <section className="panel detail-panel"><div className="panel-title"><UserRound size={16} /><h3>Organization</h3></div><div className="org-detail"><span className="workspace-avatar"><UserRound size={16} /></span><span><strong>Organization scope</strong><small className="mono">{job.organization_id}</small></span></div><div className="privacy-note">This job and its history are returned only within the authenticated organization's scope.</div></section>
      </div>

      {job.last_error && <section className="panel detail-panel"><div className="panel-title panel-title-error"><Ban size={16} /><h3>Last error</h3></div><div className="error-detail">{job.last_error}</div></section>}
      <div className="detail-grid">
        <section className="panel detail-panel"><div className="panel-title"><Zap size={16} /><h3>Input payload</h3></div><JsonBlock value={job.payload} /></section>
        <section className="panel detail-panel"><div className="panel-title"><Check size={16} /><h3>Result</h3></div><JsonBlock value={job.result} /></section>
      </div>

      <section className="section-block"><div className="section-heading"><div><span className="eyebrow">AUDIT TRAIL</span><h2>Event history</h2></div></div><div className="panel timeline-panel">{events.length ? events.map((event) => <div className="timeline-item" key={event.id}><span className="timeline-mark" /><div><strong>{event.event_type?.replaceAll('.', ' ')}</strong><small>{formatDate(event.created_at)}</small><JsonBlock value={event.payload} /></div></div>) : <EmptyState title="No events recorded" description="Job lifecycle events will appear here when available." />}</div></section>
    </div>
  )
}

function ActivityIcon() {
  return <Clock3 size={14} />
}
