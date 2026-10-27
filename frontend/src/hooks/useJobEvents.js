import { useEffect, useRef, useState } from 'react'
import { createJobSocket, parseSocketMessage } from '../services/realtime'

const MAX_RECONNECT_DELAY = 30000

export function useJobEvents(accessToken, onEvent) {
  const [connection, setConnection] = useState('disconnected')
  const callbackRef = useRef(onEvent)
  callbackRef.current = onEvent

  useEffect(() => {
    if (!accessToken) {
      setConnection('disconnected')
      return undefined
    }

    let socket
    let retryTimer
    let stopped = false
    let attempt = 0

    const connect = () => {
      if (stopped) return
      setConnection(attempt > 0 ? 'reconnecting' : 'connecting')
      socket = createJobSocket(accessToken)
      socket.onopen = () => {
        attempt = 0
        setConnection('connected')
      }
      socket.onmessage = (message) => {
        parseSocketMessage(message.data).forEach((event) => callbackRef.current?.(event))
      }
      socket.onerror = () => socket.close()
      socket.onclose = () => {
        if (stopped) return
        setConnection('reconnecting')
        const delay = Math.min(1000 * 2 ** attempt, MAX_RECONNECT_DELAY)
        attempt += 1
        retryTimer = window.setTimeout(connect, delay)
      }
    }

    connect()
    return () => {
      stopped = true
      window.clearTimeout(retryTimer)
      if (socket && socket.readyState < WebSocket.CLOSING) socket.close()
    }
  }, [accessToken])

  return connection
}
