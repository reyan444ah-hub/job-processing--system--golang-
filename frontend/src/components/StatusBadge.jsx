import { Check, CircleAlert, Clock3, LoaderCircle, RotateCcw, X } from 'lucide-react'

const statusMeta = {
  queued: { label: 'Queued', icon: Clock3 },
  processing: { label: 'Processing', icon: LoaderCircle },
  retrying: { label: 'Retrying', icon: RotateCcw },
  completed: { label: 'Completed', icon: Check },
  failed: { label: 'Failed', icon: CircleAlert },
  dead_letter: { label: 'Dead letter', icon: CircleAlert },
  cancelled: { label: 'Cancelled', icon: X },
}

export default function StatusBadge({ status = 'queued' }) {
  const meta = statusMeta[status] || { label: status, icon: CircleAlert }
  const Icon = meta.icon
  return (
    <span className={`status-badge status-${status}`}>
      <Icon size={13} className={status === 'processing' ? 'spin' : ''} />
      {meta.label}
    </span>
  )
}
