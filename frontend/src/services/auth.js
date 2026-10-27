import { api } from './api'

export async function login(credentials) {
  const { data } = await api.post('/auth/login', credentials)
  return data
}

export async function register(details) {
  const { data } = await api.post('/auth/register', details)
  return data
}

export async function logout() {
  const { data } = await api.post('/auth/logout')
  return data
}

export async function refreshTokens(refreshToken) {
  const { data } = await api.post('/auth/refresh', { refresh_token: refreshToken })
  return data
}
