import { render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import MemberJobsPage from './MemberJobsPage'

const { listMyJobsMock } = vi.hoisted(() => ({ listMyJobsMock: vi.fn() }))

vi.mock('../services/jobs', () => ({
  jobStatuses: ['queued', 'completed'],
  listMyJobs: listMyJobsMock,
}))

describe('MemberJobsPage', () => {
  beforeEach(() => {
    listMyJobsMock.mockReset()
    listMyJobsMock.mockResolvedValue({
      jobs: [{
        id: 'own-job-1',
        type: 'email.send',
        status: 'completed',
        attempt_count: 1,
        max_attempts: 3,
        created_at: '2026-01-01T00:00:00Z',
      }],
      total: 1,
    })
  })

  it('shows the authenticated user job results without admin detail links', async () => {
    render(<MemberJobsPage />)

    expect(await screen.findByText('email.send')).toBeInTheDocument()
    expect(screen.getByText('completed')).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: /my jobs/i })).toBeInTheDocument()
    expect(screen.queryByRole('link', { name: 'email.send' })).not.toBeInTheDocument()
  })
})
