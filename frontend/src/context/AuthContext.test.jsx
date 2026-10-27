import { describe, expect, it } from 'vitest'
import { act, render, screen } from '@testing-library/react'
import { AuthProvider, useAuth } from './AuthContext'
import { saveSession, updateTokens } from '../services/tokenStore'

function SessionToken() {
  const { accessToken } = useAuth()
  return <span>{accessToken}</span>
}

describe('AuthContext token rotation', () => {
  it('updates the live auth state when the API client rotates tokens', () => {
    saveSession({
      accessToken: 'old-access',
      refreshToken: 'old-refresh',
      user: { email: 'user@example.com' },
    })
    render(<AuthProvider><SessionToken /></AuthProvider>)
    expect(screen.getByText('old-access')).toBeInTheDocument()

    act(() => updateTokens({ accessToken: 'new-access', refreshToken: 'new-refresh' }))

    expect(screen.getByText('new-access')).toBeInTheDocument()
  })
})
