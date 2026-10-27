import { api } from './api'

export const jobStatuses = [
  'queued',
  'processing',
  'retrying',
  'completed',
  'failed',
  'dead_letter',
  'cancelled',
]

export function formatDate(value) {
  if (!value) return '—'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? '—' : date.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' })
}

export async function listJobs({ status = '', limit = 10, offset = 0 } = {}) {
  const { data } = await api.get('/jobs', {
    params: { ...(status ? { status } : {}), limit, offset },
  })
  return data
}

export async function listMyJobs({ status = '', limit = 10, offset = 0 } = {}) {
  const { data } = await api.get('/my/jobs', {
    params: { ...(status ? { status } : {}), limit, offset },
  })
  return data
}

export async function listAllJobs({ status = '' } = {}) {
  const firstPage = await listJobs({ status, limit: 100, offset: 0 })
  const jobs = [...(firstPage.jobs || [])]
  const total = Number(firstPage.total) || 0

  for (let offset = 100; offset < total; offset += 100) {
    const page = await listJobs({ status, limit: 100, offset })
    jobs.push(...(page.jobs || []))
  }

  return jobs
}

export async function getJob(jobId) {
  const { data } = await api.get(`/jobs/${encodeURIComponent(jobId)}`)
  return data.job
}

export async function getJobEvents(jobId) {
  const { data } = await api.get(`/jobs/${encodeURIComponent(jobId)}/events`)
  return data.events || []
}

export async function submitJob(payload, idempotencyKey) {
  const { data } = await api.post('/jobs', payload, {
    headers: { 'Idempotency-Key': idempotencyKey },
  })
  return data
}

export async function cancelJob(jobId) {
  const { data } = await api.post(`/jobs/${encodeURIComponent(jobId)}/cancel`)
  return data.job
}

export async function retryJob(jobId) {
  const { data } = await api.post(`/jobs/${encodeURIComponent(jobId)}/retry`)
  return data.job
}

export async function getMetrics() {
  const { data } = await api.get('/metrics')
  return data.metrics || {}
}
