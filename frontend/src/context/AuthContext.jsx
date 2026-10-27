import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'
import { clearSession, getSession, saveSession } from '../services/tokenStore'
import * as authApi from '../services/auth'

const AuthContext = createContext(null)

export function AuthProvider({ children }) {
  const [session, setSession] = useState(getSession)
  const [authLoading, setAuthLoading] = useState(false)

  const acceptSession = useCallback((response) => {
    const next = {
      accessToken: response.access_token,
      refreshToken: response.refresh_token,
      user: response.user || session.user,
    }
    saveSession(next)
    setSession(next)
    return next
  }, [session.user])

  const signIn = useCallback(async (credentials) => {
    setAuthLoading(true)
    try {
      return acceptSession(await authApi.login(credentials))
    } finally {
      setAuthLoading(false)
    }
  }, [acceptSession])

  const signUp = useCallback(async (details) => {
    setAuthLoading(true)
    try {
      const response = await authApi.register(details)
      return acceptSession(response)
    } finally {
      setAuthLoading(false)
    }
  }, [acceptSession])

  const signOut = useCallback(async () => {
    try {
      if (session.accessToken) await authApi.logout()
    } finally {
      clearSession()
      setSession({ accessToken: null, refreshToken: null, user: null })
    }
  }, [session.accessToken])

  useEffect(() => {
    const expire = () => {
      clearSession()
      setSession({ accessToken: null, refreshToken: null, user: null })
    }
    const syncTokens = () => setSession(getSession())
    window.addEventListener('workstream:session-expired', expire)
    window.addEventListener('workstream:tokens-refreshed', syncTokens)
    return () => {
      window.removeEventListener('workstream:session-expired', expire)
      window.removeEventListener('workstream:tokens-refreshed', syncTokens)
    }
  }, [])

  const value = useMemo(() => ({
    ...session,
    isAuthenticated: Boolean(session.accessToken && session.user),
    authLoading,
    signIn,
    signUp,
    signOut,
  }), [session, authLoading, signIn, signUp, signOut])

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) throw new Error('useAuth must be used within AuthProvider')
  return context
}
