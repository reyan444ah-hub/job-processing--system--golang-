import { useCallback, useEffect, useState } from 'react'
import { ChevronLeft, ChevronRight, Filter, RefreshCw, Search } from 'lucide-react'
import EmptyState from '../components/EmptyState'
import JobTable from '../components/JobTable'
import Loading from '../components/Loading'
import { useToast } from '../context/ToastContext'
import { getApiError } from '../services/api'
import { jobStatuses, listAllJobs, listJobs } from '../services/jobs'

const PAGE_SIZE = 10

export default function JobsPage() {
  const [status, setStatus] = useState('')
  const [statusDraft, setStatusDraft] = useState('')
  const [searchInput, setSearchInput] = useState('')
  const [searchQuery, setSearchQuery] = useState('')
  const [page, setPage] = useState(0)
  const [data, setData] = useState({ jobs: [], total: 0 })
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const { notify } = useToast()

  const load = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      if (searchQuery) {
        const jobs = await listAllJobs({ status })
        const matchingJobs = jobs.filter((job) => job.type?.toLocaleLowerCase().includes(searchQuery.toLocaleLowerCase()))
        setData({ jobs: matchingJobs.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE), total: matchingJobs.length })
        return
      }
      setData(await listJobs({ status, limit: PAGE_SIZE, offset: page * PAGE_SIZE }))
    } catch (loadError) {
      setError(getApiError(loadError))
    } finally {
      setLoading(false)
    }
  }, [status, searchQuery, page])

  useEffect(() => { load() }, [load])
  useEffect(() => {
    const onEvent = () => {
      load().catch(() => notify('Could not refresh jobs after a live event.', 'error'))
    }
    window.addEventListener('workstream:job-event', onEvent)
    return () => window.removeEventListener('workstream:job-event', onEvent)
  }, [load, notify])

  const pageCount = Math.max(1, Math.ceil((data.total || 0) / PAGE_SIZE))
  const applySearch = (event) => {
    event.preventDefault()
    setSearchQuery(searchInput.trim())
    setPage(0)
  }
  const applyStatusFilter = () => {
    setStatus(statusDraft)
    setPage(0)
  }

  return (
    <div className="page-stack">
      <section className="jobs-toolbar">
        <div className="jobs-intro"><span className="eyebrow">JOB QUEUE</span><h2>All jobs <span className="count-pill">{data.total}</span></h2><p>Search by job name or filter the queue by status.</p></div>
        <button className="button button-secondary" onClick={load}><RefreshCw size={15} />Refresh</button>
      </section>
      <section className="filter-bar">
        <form className="jobs-search-group" onSubmit={applySearch}>
          {/* <label className="filter-title" htmlFor="job-name-search"><Search size={15} /></label> */}
          <div className="search-control"><Search size={15} /><input id="job-name-search" type="search" value={searchInput} onChange={(event) => setSearchInput(event.target.value)} placeholder="Enter a job name" /></div>
          <button className="button button-secondary button-small" type="submit">Search</button>
        </form>
        <div className="jobs-status-group">
          <label className="filter-title" htmlFor="job-status-filter"><Filter size={15} />Filter by status</label>
          <div className="filter-control"><select id="job-status-filter" aria-label="Filter by status" value={statusDraft} onChange={(event) => setStatusDraft(event.target.value)}><option value="">All statuses</option>{jobStatuses.map((item) => <option value={item} key={item}>{item.replace('_', ' ')}</option>)}</select></div>
          <button className="button button-secondary button-small" type="button" onClick={applyStatusFilter}>Filter</button>
        </div>
        <span className="filter-count">{data.total} {data.total === 1 ? 'result' : 'results'}</span>
      </section>
      {error && <div className="inline-error" role="alert"><span>{error}</span><button className="button button-quiet" onClick={load}><RefreshCw size={14} />Retry</button></div>}
      <section className="panel jobs-panel" aria-busy={loading}>
        {loading ? <Loading label="Loading jobs…" /> : data.jobs?.length ? <JobTable jobs={data.jobs} /> : !error ? <EmptyState title={searchQuery ? 'No matching jobs' : status ? 'No matching jobs' : 'No jobs in this workspace'} description={searchQuery ? `No job names match "${searchQuery}". Try another name or status.` : status ? 'Choose another status filter to see more results.' : 'Jobs submitted by your organization will appear here.'} /> : null}
      </section>
      <div className="pagination">
        <span>Showing <strong>{data.total ? page * PAGE_SIZE + 1 : 0}–{Math.min((page + 1) * PAGE_SIZE, data.total)}</strong> of <strong>{data.total}</strong></span>
        <div><button className="button button-secondary button-small" disabled={page === 0 || loading} onClick={() => setPage((value) => Math.max(0, value - 1))}><ChevronLeft size={15} />Previous</button><span className="page-number">Page {page + 1} of {pageCount}</span><button className="button button-secondary button-small" disabled={(page + 1) * PAGE_SIZE >= data.total || loading} onClick={() => setPage((value) => value + 1)}>Next<ChevronRight size={15} /></button></div>
      </div>
    </div>
  )
}
