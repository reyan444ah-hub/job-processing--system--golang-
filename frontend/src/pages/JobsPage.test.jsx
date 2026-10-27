import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import JobsPage from './JobsPage'

const { listAllJobsMock, listJobsMock } = vi.hoisted(() => ({
  listAllJobsMock: vi.fn(),
  listJobsMock: vi.fn(),
}))

vi.mock('../services/jobs', () => ({
  jobStatuses: ['queued', 'failed'],
  listAllJobs: listAllJobsMock,
  listJobs: listJobsMock,
}))
vi.mock('../context/ToastContext', () => ({ useToast: () => ({ notify: vi.fn() }) }))

const jobs = [
  { id: '1', type: 'invoice.send', status: 'queued', attempt_count: 0, max_attempts: 3, created_at: '2026-01-01T00:00:00Z' },
  { id: '2', type: 'email.send', status: 'failed', attempt_count: 1, max_attempts: 3, created_at: '2026-01-02T00:00:00Z' },
]

describe('JobsPage search and status filter', () => {
  beforeEach(() => {
    listAllJobsMock.mockReset()
    listJobsMock.mockReset().mockResolvedValue({ jobs, total: jobs.length })
  })

  it('searches job names separately from applying the status filter', async () => {
    listAllJobsMock.mockResolvedValue(jobs)
    render(<MemoryRouter><JobsPage /></MemoryRouter>)

    expect(await screen.findByText('invoice.send')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Search by job name'), { target: { value: 'INVOICE' } })
    fireEvent.click(screen.getByRole('button', { name: 'Search' }))

    expect(await screen.findByText('invoice.send')).toBeInTheDocument()
    expect(screen.queryByText('email.send')).not.toBeInTheDocument()
    expect(listAllJobsMock).toHaveBeenCalledWith({ status: '' })

    fireEvent.change(screen.getByLabelText('Filter by status'), { target: { value: 'failed' } })
    expect(listJobsMock).toHaveBeenCalledTimes(1)
    fireEvent.click(screen.getByRole('button', { name: 'Filter' }))

    await waitFor(() => expect(listAllJobsMock).toHaveBeenLastCalledWith({ status: 'failed' }))
  })
})
