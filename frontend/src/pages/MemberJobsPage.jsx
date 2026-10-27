import { useCallback, useEffect, useState } from 'react'
import { ChevronLeft, ChevronRight, Filter, RefreshCw } from 'lucide-react'
import EmptyState from '../components/EmptyState'
import JobTable from '../components/JobTable'
import Loading from '../components/Loading'
import { getApiError } from '../services/api'
import { jobStatuses, listMyJobs } from '../services/jobs'

const PAGE_SIZE = 10

export default function MemberJobsPage() {
  const [status, setStatus] = useState('')
  const [statusDraft, setStatusDraft] = useState('')
  const [page, setPage] = useState(0)
  const [data, setData] = useState({ jobs: [], total: 0 })
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      setData(await listMyJobs({ status, limit: PAGE_SIZE, offset: page * PAGE_SIZE }))
    } catch (loadError) {
      setError(getApiError(loadError))
    } finally {
      setLoading(false)
    }
  }, [status, page])

  useEffect(() => { load() }, [load])
  useEffect(() => {
    const onJobEvent = (event) => {
      if (event.detail?.event_type === 'job.completed') load()
    }
    window.addEventListener('workstream:job-event', onJobEvent)
    return () => window.removeEventListener('workstream:job-event', onJobEvent)
  }, [load])

  function applyStatusFilter() {
    setStatus(statusDraft)
    setPage(0)
  }

  const pageCount = Math.max(1, Math.ceil((data.total || 0) / PAGE_SIZE))

  return (
    <div className="page-stack">
      <section className="jobs-toolbar">
        <div className="jobs-intro"><span className="eyebrow">YOUR JOB HISTORY</span><h2>My jobs <span className="count-pill">{data.total}</span></h2><p>View every job submitted from your account and track its latest status.</p></div>
        <button className="button button-secondary" onClick={load} disabled={loading}><RefreshCw size={15} />Refresh</button>
      </section>
      <section className="filter-bar">
        <div className="jobs-status-group">
          <label className="filter-title" htmlFor="my-job-status-filter"><Filter size={15} />Filter by status</label>
          <div className="filter-control"><select id="my-job-status-filter" value={statusDraft} onChange={(event) => setStatusDraft(event.target.value)}><option value="">All statuses</option>{jobStatuses.map((item) => <option value={item} key={item}>{item.replace('_', ' ')}</option>)}</select></div>
          <button className="button button-secondary button-small" type="button" onClick={applyStatusFilter}>Filter</button>
        </div>
        <span className="filter-count">{data.total} {data.total === 1 ? 'job' : 'jobs'}</span>
      </section>
      {error && <div className="inline-error" role="alert"><span>{error}</span><button className="button button-quiet" onClick={load}><RefreshCw size={14} />Retry</button></div>}
      <section className="panel jobs-panel" aria-busy={loading}>
        {loading ? <Loading label="Loading your jobs…" /> : data.jobs?.length ? <JobTable jobs={data.jobs} compact readOnly /> : !error ? <EmptyState title={status ? 'No matching jobs' : 'No jobs submitted yet'} description={status ? 'Try another status filter.' : 'Jobs you submit will appear here so you can track their progress.'} /> : null}
      </section>
      <div className="pagination">
        <span>Showing <strong>{data.total ? page * PAGE_SIZE + 1 : 0}–{Math.min((page + 1) * PAGE_SIZE, data.total)}</strong> of <strong>{data.total}</strong></span>
        <div><button className="button button-secondary button-small" disabled={page === 0 || loading} onClick={() => setPage((value) => Math.max(0, value - 1))}><ChevronLeft size={15} />Previous</button><span className="page-number">Page {page + 1} of {pageCount}</span><button className="button button-secondary button-small" disabled={(page + 1) * PAGE_SIZE >= data.total || loading} onClick={() => setPage((value) => value + 1)}>Next<ChevronRight size={15} /></button></div>
      </div>
    </div>
  )
}
