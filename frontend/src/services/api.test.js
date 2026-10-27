import { beforeEach, describe, expect, it, vi } from 'vitest'
import { getApiError } from './api'
import { clearSession, getSession, saveSession } from './tokenStore'

describe('session token storage', () => {
  beforeEach(() => clearSession())

  it('keeps the access/refresh pair and user only in session storage', () => {
    const session = {
      accessToken: 'access',
      refreshToken: 'refresh',
      user: { email: 'user@example.com', organization_id: 'org-1' },
    }
    saveSession(session)
    expect(getSession()).toEqual(session)
    expect(localStorage.length).toBe(0)
    clearSession()
    expect(getSession()).toEqual({ accessToken: null, refreshToken: null, user: null })
  })
})

describe('API error mapping', () => {
  it('uses the message returned by the actual Go handler', () => {
    expect(getApiError({ response: { data: { error: 'invalid or expired token' } } }))
      .toBe('invalid or expired token')
  })

  it('explains network errors without inventing a backend response', () => {
    expect(getApiError({ code: 'ERR_NETWORK' })).toContain('backend is running')
  })
})
