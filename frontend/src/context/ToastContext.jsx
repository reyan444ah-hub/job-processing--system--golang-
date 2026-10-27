import { createContext, useCallback, useContext, useMemo, useState } from 'react'
import { CircleCheck, CircleX, Info, X } from 'lucide-react'

const ToastContext = createContext(null)

const icons = {
  success: CircleCheck,
  error: CircleX,
  info: Info,
}

export function ToastProvider({ children }) {
  const [toasts, setToasts] = useState([])
  const dismiss = useCallback((id) => setToasts((items) => items.filter((item) => item.id !== id)), [])
  const notify = useCallback((message, type = 'info') => {
    const id = `${Date.now()}-${Math.random()}`
    setToasts((items) => [...items, { id, message, type }])
    window.setTimeout(() => dismiss(id), 5000)
  }, [dismiss])
  const value = useMemo(() => ({ notify }), [notify])

  return (
    <ToastContext.Provider value={value}>
      {children}
      <div className="toast-stack" aria-live="polite" aria-atomic="false">
        {toasts.map(({ id, message, type }) => {
          const Icon = icons[type] || Info
          return (
            <div className={`toast toast-${type}`} key={id} role="status">
              <Icon size={17} />
              <span>{message}</span>
              <button className="icon-button toast-close" onClick={() => dismiss(id)} aria-label="Dismiss notification">
                <X size={15} />
              </button>
            </div>
          )
        })}
      </div>
    </ToastContext.Provider>
  )
}

export function useToast() {
  const context = useContext(ToastContext)
  if (!context) throw new Error('useToast must be used within ToastProvider')
  return context
}
