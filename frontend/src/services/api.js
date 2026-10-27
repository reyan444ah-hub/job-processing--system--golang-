import axios from 'axios'
import {
  clearSession,
  getAccessToken,
  getRefreshToken,
  updateTokens,
} from './tokenStore'

const configuredBase = import.meta.env.VITE_API_BASE_URL || '/api/v1'
const normalizedBase = configuredBase.replace(/\/+$/, '')
export const apiBaseURL = normalizedBase.endsWith('/api/v1')
  ? normalizedBase
  : `${normalizedBase}/api/v1`

export const api = axios.create({
  baseURL: apiBaseURL,
  timeout: 15000,
  headers: { 'Content-Type': 'application/json' },
})

let refreshPromise

api.interceptors.request.use((config) => {
  const accessToken = getAccessToken()
  if (accessToken) config.headers.Authorization = `Bearer ${accessToken}`
  return config
})

api.interceptors.response.use(
  (response) => response,
  async (error) => {
    const original = error.config
    const url = original?.url || ''
    const isAuthRequest = /\/auth\/(login|register|refresh|logout)$/.test(url)
    if (error.response?.status !== 401 || !original || original._retried || isAuthRequest) {
      return Promise.reject(error)
    }

    const refreshToken = getRefreshToken()
    if (!refreshToken) {
      clearSession()
      window.dispatchEvent(new Event('workstream:session-expired'))
      return Promise.reject(error)
    }

    original._retried = true
    try {
      refreshPromise ||= axios
        .post(`${apiBaseURL}/auth/refresh`, { refresh_token: refreshToken }, { timeout: 15000 })
        .then(({ data }) => {
          if (!data.access_token || !data.refresh_token) {
            throw new Error('The refresh response did not include both tokens.')
          }
          updateTokens({ accessToken: data.access_token, refreshToken: data.refresh_token })
          return data.access_token
        })
        .finally(() => {
          refreshPromise = null
        })

      const accessToken = await refreshPromise
      original.headers = original.headers || {}
      original.headers.Authorization = `Bearer ${accessToken}`
      return api(original)
    } catch (refreshError) {
      clearSession()
      window.dispatchEvent(new Event('workstream:session-expired'))
      return Promise.reject(refreshError)
    }
  },
)

export function getApiError(error) {
  const data = error?.response?.data
  if (typeof data?.error === 'string') return data.error
  if (typeof data?.message === 'string') return data.message
  if (error?.code === 'ECONNABORTED') return 'The request timed out. Please try again.'
  if (!error?.response) return 'Could not connect to the API. Check that the backend is running.'
  return `Request failed (${error.response.status}). Please try again.`
}
