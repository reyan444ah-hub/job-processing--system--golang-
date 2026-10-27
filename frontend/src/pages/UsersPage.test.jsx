import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import UsersPage from './UsersPage'

const { createUserMock, listUsersMock, notifyMock } = vi.hoisted(() => ({
  createUserMock: vi.fn(),
  listUsersMock: vi.fn(),
  notifyMock: vi.fn(),
}))

vi.mock('../services/users', () => ({
  createUser: createUserMock,
  listUsers: listUsersMock,
}))
vi.mock('../context/ToastContext', () => ({
  useToast: () => ({ notify: notifyMock }),
}))

describe('UsersPage', () => {
  beforeEach(() => {
    createUserMock.mockReset()
    listUsersMock.mockReset()
    notifyMock.mockReset()
    listUsersMock.mockResolvedValue([{
      id: 'admin-1',
      email: 'admin@example.com',
      role: 'admin',
      created_at: '2026-01-01T00:00:00Z',
    }])
  })

  it('lists organization accounts and creates a member account', async () => {
    createUserMock.mockResolvedValue({
      id: 'member-1',
      email: 'member@example.com',
      role: 'member',
      created_at: '2026-02-01T00:00:00Z',
    })

    render(<UsersPage />)
    expect(await screen.findByText('admin@example.com')).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Email address'), { target: { value: 'member@example.com' } })
    fireEvent.change(screen.getByLabelText(/^Temporary password/), { target: { value: 'secret-pass' } })
    fireEvent.click(screen.getByRole('button', { name: 'Create user' }))

    await waitFor(() => expect(createUserMock).toHaveBeenCalledWith({
      email: 'member@example.com',
      password: 'secret-pass',
    }))
    expect(await screen.findByText('member@example.com')).toBeInTheDocument()
    expect(notifyMock).toHaveBeenCalledWith('User account created. Share the login details securely.', 'success')
  })
})
