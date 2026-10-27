import { act, fireEvent, render, screen, within } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import MemberShell from './MemberShell'

const { eventHandler, notifyMock } = vi.hoisted(() => ({
  eventHandler: { current: null },
  notifyMock: vi.fn(),
}))

vi.mock('../context/AuthContext', () => ({
  useAuth: () => ({
    user: { id: 'member-1', email: 'member@example.com', role: 'member' },
    accessToken: 'member-token',
    signOut: vi.fn(),
  }),
}))
vi.mock('../context/ToastContext', () => ({
  useToast: () => ({ notify: notifyMock }),
}))
vi.mock('../hooks/useJobEvents', () => ({
  useJobEvents: (_token, onEvent) => {
    eventHandler.current = onEvent
    return 'connected'
  },
}))

describe('MemberShell completion notifications', () => {
  beforeEach(() => {
    eventHandler.current = null
    notifyMock.mockReset()
  })

  it('shows a completion toast and notification for the job owner only', () => {
    render(
      <MemoryRouter initialEntries={['/submit']}>
        <Routes>
          <Route element={<MemberShell />}>
            <Route path="/submit" element={<p>Submit job form</p>} />
          </Route>
        </Routes>
      </MemoryRouter>,
    )

    act(() => eventHandler.current({
      event_type: 'job.completed',
      recipient_user_id: 'member-1',
      aggregate_id: 'job-123',
      payload: { job_type: 'email.send' },
    }))

    expect(notifyMock).toHaveBeenCalledWith('Your job "email.send" has completed.', 'success')
    fireEvent.click(screen.getByRole('button', { name: 'Notifications' }))
    const panel = screen.getByRole('region', { name: 'Job completion notifications' })
    expect(within(panel).getByText('Your job completed')).toBeInTheDocument()
    expect(within(panel).getByText('email.send · job-123')).toBeInTheDocument()

    act(() => eventHandler.current({
      event_type: 'job.completed',
      recipient_user_id: 'someone-else',
      aggregate_id: 'other-job',
      payload: { job_type: 'other.job' },
    }))
    expect(notifyMock).toHaveBeenCalledTimes(1)
  })
})
