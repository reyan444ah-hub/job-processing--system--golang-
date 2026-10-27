import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import NewJobPage from './NewJobPage'

const { submitJobMock, authState } = vi.hoisted(() => ({
  submitJobMock: vi.fn(),
  authState: { user: { role: 'admin' } },
}))

vi.mock('../services/jobs', () => ({ submitJob: submitJobMock }))
vi.mock('../context/ToastContext', () => ({ useToast: () => ({ notify: vi.fn() }) }))
vi.mock('../context/AuthContext', () => ({ useAuth: () => authState }))

describe('NewJobPage submission retries', () => {
  beforeEach(() => {
    submitJobMock.mockReset()
    authState.user = { role: 'admin' }
    vi.stubGlobal('crypto', { randomUUID: () => 'test-request-id' })
  })
  afterEach(() => vi.unstubAllGlobals())

  it('reuses the exact payload and idempotency key after a failed submission', async () => {
    submitJobMock
      .mockRejectedValueOnce({
        response: { status: 503, data: { error: 'temporarily unavailable' } },
      })
      .mockResolvedValueOnce({ job: { id: 'job-1' }, deduplicated: false })

    render(<MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}><NewJobPage /></MemoryRouter>)
    fireEvent.click(screen.getByRole('button', { name: /submit job/i }))
    await screen.findByRole('alert')

    fireEvent.click(screen.getByRole('button', { name: /retry same request/i }))
    await waitFor(() => expect(submitJobMock).toHaveBeenCalledTimes(2))

    expect(submitJobMock.mock.calls[1]).toEqual(submitJobMock.mock.calls[0])
    expect(submitJobMock.mock.calls[0][1]).toMatch(/^ws-test-request-id-/)
  })

  it('shows a success confirmation instead of opening admin job details for a member', async () => {
    authState.user = { role: 'member' }
    submitJobMock.mockResolvedValue({ job: { id: 'member-job-1', type: 'email.send' }, deduplicated: false })

    render(
      <MemoryRouter initialEntries={['/submit']}>
        <Routes>
          <Route path="/submit" element={<NewJobPage />} />
          <Route path="/jobs/:jobId" element={<p>Admin job details</p>} />
        </Routes>
      </MemoryRouter>,
    )
    fireEvent.click(screen.getByRole('button', { name: /submit job/i }))

    expect(await screen.findByRole('status')).toHaveTextContent('Job submitted successfully')
    expect(screen.queryByText('Admin job details')).not.toBeInTheDocument()
  })
})
