import { apiBaseURL } from './api'

export function createJobSocket(accessToken) {
  const base = new URL(apiBaseURL, window.location.origin)
  const protocol = base.protocol === 'https:' ? 'wss:' : 'ws:'
  const socketURL = new URL('/api/v1/ws/jobs', `${protocol}//${base.host}`)
  socketURL.searchParams.set('token', accessToken)
  return new WebSocket(socketURL)
}

export function parseSocketMessage(raw) {
  return raw
    .split('\n')
    .map((part) => part.trim())
    .filter(Boolean)
    .flatMap((part) => {
      try {
        return [JSON.parse(part)]
      } catch {
        return []
      }
    })
}
