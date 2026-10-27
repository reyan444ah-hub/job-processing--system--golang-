import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import ProtectedRoute from './ProtectedRoute'

const { authState } = vi.hoisted(() => ({ authState: { isAuthenticated: true, user: { role: 'member' } } }))

vi.mock('../context/AuthContext', () => ({
  useAuth: () => authState,
}))

function renderRoleRoutes(initialEntry) {
  return render(
    <MemoryRouter initialEntries={[initialEntry]}>
      <Routes>
        <Route element={<ProtectedRoute allowedRoles={['admin']} />}>
          <Route index element={<p>Admin home</p>} />
          <Route path="/jobs" element={<p>Admin jobs</p>} />
        </Route>
        <Route element={<ProtectedRoute allowedRoles={['member']} />}>
          <Route path="/submit" element={<p>Submit a job</p>} />
        </Route>
      </Routes>
    </MemoryRouter>,
  )
}

describe('ProtectedRoute role access', () => {
  it('redirects a member away from admin pages to the submit-only portal', () => {
    authState.isAuthenticated = true
    authState.user = { role: 'member' }
    renderRoleRoutes('/jobs')
    expect(screen.getByText('Submit a job')).toBeInTheDocument()
    expect(screen.queryByText('Admin jobs')).not.toBeInTheDocument()
  })

  it('redirects an admin away from the member portal', () => {
    authState.user = { role: 'admin' }
    renderRoleRoutes('/submit')
    expect(screen.getByText('Admin home')).toBeInTheDocument()
    expect(screen.queryByText('Submit a job')).not.toBeInTheDocument()
  })
})
