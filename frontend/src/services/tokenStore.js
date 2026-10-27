const ACCESS_KEY = 'workstream.accessToken'
const REFRESH_KEY = 'workstream.refreshToken'
const USER_KEY = 'workstream.user'

export function getSession() {
  try {
    const user = sessionStorage.getItem(USER_KEY)
    return {
      accessToken: sessionStorage.getItem(ACCESS_KEY),
      refreshToken: sessionStorage.getItem(REFRESH_KEY),
      user: user ? JSON.parse(user) : null,
    }
  } catch {
    return { accessToken: null, refreshToken: null, user: null }
  }
}

export function saveSession({ accessToken, refreshToken, user }) {
  sessionStorage.setItem(ACCESS_KEY, accessToken)
  sessionStorage.setItem(REFRESH_KEY, refreshToken)
  sessionStorage.setItem(USER_KEY, JSON.stringify(user))
}

export function updateTokens({ accessToken, refreshToken }) {
  sessionStorage.setItem(ACCESS_KEY, accessToken)
  sessionStorage.setItem(REFRESH_KEY, refreshToken)
  window.dispatchEvent(new Event('workstream:tokens-refreshed'))
}

export function clearSession() {
  sessionStorage.removeItem(ACCESS_KEY)
  sessionStorage.removeItem(REFRESH_KEY)
  sessionStorage.removeItem(USER_KEY)
}

export function getAccessToken() {
  return sessionStorage.getItem(ACCESS_KEY)
}

export function getRefreshToken() {
  return sessionStorage.getItem(REFRESH_KEY)
}
