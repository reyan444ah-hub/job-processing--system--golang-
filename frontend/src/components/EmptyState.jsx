import { BriefcaseBusiness } from 'lucide-react'

export default function EmptyState({ title = 'Nothing to show yet', description = 'Jobs submitted to your workspace will appear here.' }) {
  return (
    <div className="empty-state">
      <span className="empty-icon"><BriefcaseBusiness size={22} /></span>
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  )
}
