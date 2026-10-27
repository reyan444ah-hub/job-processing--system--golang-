import { useCallback, useEffect, useState } from 'react'
import { Activity, ArrowDownRight, ArrowUpRight, BriefcaseBusiness, CheckCircle2, Clock3, Layers3, Plus, RefreshCw, Server, Timer, Zap } from 'lucide-react'
import { Link } from 'react-router-dom'
import EmptyState from '../components/EmptyState'
import JobTable from '../components/JobTable'
import Loading from '../components/Loading'
import { useToast } from '../context/ToastContext'
import { getApiError } from '../services/api'
import { getMetrics, jobStatuses, listJobs } from '../services/jobs'

const statusCards = [
  { key: 'queued', label: 'Queued', icon: Clock3, tone: 'amber' },
  { key: 'processing', label: 'Processing', icon: Activity, tone: 'blue' },
  { key: 'completed', label: 'Completed', icon: CheckCircle2, tone: 'green' },
  { key: 'dead_letter', label: 'Dead letter', icon: ArrowDownRight, tone: 'red' },
]

function MetricCard({ title, value, icon: Icon, tone, note }) {
  return (
    <div className="metric-card">
      <div className="metric-top"><span>{title}</span><span className={`metric-icon metric-${tone}`}><Icon size={17} /></span></div>
      <strong className="metric-value">{value ?? '—'}</strong>
      <span className="metric-note">{note}</span>
    </div>
  )
}

export default function DashboardPage() {
  const [summary, setSummary] = useState({})
  const [jobs, setJobs] = useState([])
  const [metrics, setMetrics] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const { notify } = useToast()

  const load = useCallback(async (showLoading = true) => {
    if (showLoading) setLoading(true)
    setError('')
    try {
      const [recent, ...statusResults] = await Promise.all([
        listJobs({ limit: 6, offset: 0 }),
        ...jobStatuses.map((status) => listJobs({ status, limit: 1, offset: 0 })),
        getMetrics().catch(() => null),
      ])
      const counts = {}
      jobStatuses.forEach((status, index) => { counts[status] = statusResults[index]?.total ?? 0 })
      setSummary(counts)
      setJobs(recent.jobs || [])
      setMetrics(statusResults[jobStatuses.length] || null)
    } catch (loadError) {
      setError(getApiError(loadError))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { load() }, [load])
  useEffect(() => {
    const update = () => load(false)
    window.addEventListener('workstream:job-event', update)
    return () => window.removeEventListener('workstream:job-event', update)
  }, [load])

  const total = jobStatuses.reduce((sum, status) => sum + (summary[status] || 0), 0)
  const completed = summary.completed || 0
  const completionRate = total ? Math.round((completed / total) * 100) : 0

  return (
    <div className="page-stack">
      <section className="welcome-banner">
        <div className="welcome-copy"><span className="eyebrow">OPERATIONS OVERVIEW</span><h2>Your jobs, <span>in motion.</span></h2><p>Monitor your async workload and keep every workflow moving.</p></div>
        <div className="welcome-chart" aria-hidden="true"><div className="chart-label">COMPLETIONS</div><div className="chart-bars">{[35, 54, 42, 70, 48, 78, 61, 88, 64, 100, 75, 94].map((height, index) => <i style={{ height: `${height}%` }} key={index} />)}</div><div className="chart-axis"><span>Earlier</span><span>Latest</span></div></div>
        <div className="welcome-decoration" />
      </section>

      {error && <div className="inline-error" role="alert"><span>{error}</span><button className="button button-quiet" onClick={() => load()}><RefreshCw size={14} />Retry</button></div>}

      <section className="section-block">
        <div className="section-heading"><div><span className="eyebrow">LIVE SNAPSHOT</span><h2>Queue overview</h2></div><button className="icon-button refresh-button" onClick={() => load()} aria-label="Refresh dashboard"><RefreshCw size={16} /></button></div>
        {loading ? <div className="metric-grid">{statusCards.map((card) => <div className="metric-card metric-skeleton" key={card.key}><span className="skeleton skeleton-label" /><span className="skeleton skeleton-number" /></div>)}</div> : (
          <div className="metric-grid">
            {statusCards.map((card) => <MetricCard key={card.key} title={card.label} value={summary[card.key] ?? 0} icon={card.icon} tone={card.tone} note={card.key === 'completed' ? `${completionRate}% of all jobs` : `${total} total jobs in workspace`} />)}
          </div>
        )}
      </section>

      <section className="secondary-metrics">
        <div className="secondary-metric"><span className="secondary-icon"><Layers3 size={17} /></span><div><span>In stream</span><strong>{metrics?.stream_length ?? '—'}</strong></div><span className="secondary-caption">Redis queue</span></div>
        <div className="secondary-metric"><span className="secondary-icon icon-purple"><Timer size={17} /></span><div><span>Scheduled</span><strong>{metrics?.scheduled_jobs ?? '—'}</strong></div><span className="secondary-caption">Waiting to run</span></div>
        <div className="secondary-metric"><span className="secondary-icon icon-orange"><Server size={17} /></span><div><span>Active workers</span><strong>{metrics?.worker_count ?? '—'}</strong></div><span className="secondary-caption">Connected workers</span></div>
        <div className="secondary-metric"><span className="secondary-icon icon-green"><Zap size={17} /></span><div><span>Pending messages</span><strong>{metrics?.pending_messages ?? '—'}</strong></div><span className="secondary-caption">Awaiting acknowledgement</span></div>
      </section>

      <section className="section-block jobs-preview">
        <div className="section-heading"><div><span className="eyebrow">LATEST ACTIVITY</span><h2>Recent jobs</h2></div><Link to="/jobs" className="text-link">View all jobs <ArrowUpRight size={15} /></Link></div>
        {loading ? <Loading label="Fetching your latest jobs…" /> : jobs.length ? <div className="panel"><JobTable jobs={jobs} /></div> : !error ? <div className="panel"><EmptyState title="Your queue is clear" description="Create your first job to start tracking work in this workspace." /><div className="empty-action"><Link className="button button-primary" to="/jobs/new"><Plus size={16} />Create a job</Link></div></div> : null}
      </section>
      <div className="dashboard-footnote"><BriefcaseBusiness size={14} />Job counts are scoped to your authenticated organization. System queue metrics are shared platform counters.</div>
    </div>
  )
}
