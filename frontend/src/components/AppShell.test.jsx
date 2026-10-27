import { act, fireEvent, render, screen, within } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import AppShell from './AppShell'

const { eventHandler } = vi.hoisted(() => ({ eventHandler: { current: null } }))

vi.mock('../context/AuthContext', () => ({
  useAuth: () => ({ user: { email: 'member@example.com', role: 'Member' }, accessToken: 'token', signOut: vi.fn() }),
}))
vi.mock('../context/ToastContext', () => ({ useToast: () => ({ notify: vi.fn() }) }))
vi.mock('../hooks/useJobEvents', () => ({
  useJobEvents: (_token, onEvent) => {
    eventHandler.current = onEvent
    return 'connected'
  },
}))

describe('AppShell notifications', () => {
  beforeEach(() => { eventHandler.current = null })

  it('removes the mobile menu control and shows recent job events from the bell', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route element={<AppShell />}>
            <Route path="/" element={<p>Dashboard</p>} />
          </Route>
        </Routes>
      </MemoryRouter>,
    )

    expect(screen.queryByRole('button', { name: 'Open navigation' })).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Notifications' }))
    expect(screen.getByText('No notifications yet. Job updates will appear here.')).toBeInTheDocument()

    act(() => eventHandler.current({ event_type: 'job.completed', aggregate_id: 'job-123' }))
    expect(screen.getByLabelText('1 unread notifications')).toBeInTheDocument()
    const panel = screen.getByRole('region', { name: 'Recent notifications' })
    expect(within(panel).getByText('job completed')).toBeInTheDocument()
    expect(within(panel).getByRole('button', { name: 'Open job job-123' })).toBeInTheDocument()
  })

  it('labels the admin user page Users and hides the New job shortcut there', () => {
    render(
      <MemoryRouter initialEntries={['/users']}>
        <Routes>
          <Route element={<AppShell />}>
            <Route path="/users" element={<p>User accounts</p>} />
          </Route>
        </Routes>
      </MemoryRouter>,
    )

    expect(screen.getByRole('link', { name: 'Users' })).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Users' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /new job/i })).not.toBeInTheDocument()
  })
})
