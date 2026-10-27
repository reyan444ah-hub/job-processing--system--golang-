import { Bell, BriefcaseBusiness, Command, LogOut, Send } from 'lucide-react'
import { useState } from 'react'
import { NavLink, Outlet, useLocation, useNavigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext'
import { useToast } from '../context/ToastContext'
import { useJobEvents } from '../hooks/useJobEvents'

export default function MemberShell() {
  const { user, accessToken, signOut } = useAuth()
  const { notify } = useToast()
  const navigate = useNavigate()
  const location = useLocation()
  const [notifications, setNotifications] = useState([])
  const [notificationsOpen, setNotificationsOpen] = useState(false)
  const viewingJobs = location.pathname.startsWith('/submit/jobs')
  useJobEvents(accessToken, (event) => {
    if (event.event === 'connected') return
    if (event.event_type !== 'job.completed' || event.recipient_user_id !== user?.id) return
    setNotifications((current) => [event, ...current].slice(0, 10))
    notify(`Your job "${event.payload?.job_type || 'job'}" has completed.`, 'success')
    window.dispatchEvent(new CustomEvent('workstream:job-event', { detail: event }))
  })

  async function handleSignOut() {
    try {
      await signOut()
    } catch (error) {
      notify(error?.response?.data?.error || 'Could not complete server logout. Your local session was cleared.', 'error')
    } finally {
      navigate('/login', { replace: true })
    }
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand"><span className="brand-mark"><Command size={19} strokeWidth={2.5} /></span><span>workstream<span className="brand-dot">.</span></span></div>
        <div className="workspace-switcher">
          <span className="workspace-avatar">{(user?.email || 'U').slice(0, 1).toUpperCase()}</span>
          <span className="workspace-copy"><strong>{user?.email || 'User account'}</strong><small>Job submitter</small></span>
        </div>
        <div className="nav-label">USER PORTAL</div>
        <nav className="side-nav">
          <NavLink to="/submit" end><Send size={17} />Submit a job</NavLink>
          <NavLink to="/submit/jobs"><BriefcaseBusiness size={17} />My jobs</NavLink>
        </nav>
        <div className="sidebar-bottom">
          <button className="profile-row" onClick={handleSignOut}>
            <span className="profile-avatar">{(user?.email || 'U').slice(0, 1).toUpperCase()}</span>
            <span className="profile-copy"><strong>{user?.email || 'Signed-in user'}</strong><small>Member</small></span>
            <LogOut size={16} />
          </button>
        </div>
      </aside>
      <main className="main-area">
        <header className="topbar">
          <div className="topbar-heading"><h1>{viewingJobs ? 'My jobs' : 'Submit a job'}</h1><span>{viewingJobs ? 'Only jobs submitted from your account are shown' : 'Send work to your organization&apos;s background queue'}</span></div>
          <div className="topbar-actions">
            <div className="notification-wrap">
              <button
                className="icon-button notification-button"
                aria-label="Notifications"
                aria-expanded={notificationsOpen}
                aria-controls="member-notification-panel"
                onClick={() => setNotificationsOpen((open) => !open)}
              >
                <Bell size={17} />
                {notifications.length > 0 && !notificationsOpen && <i aria-label={`${notifications.length} job completion notifications`} />}
              </button>
              {notificationsOpen && (
                <section className="notification-panel" id="member-notification-panel" aria-label="Job completion notifications">
                  <div className="notification-panel-heading">
                    <strong>Job notifications</strong>
                    <span>{notifications.length} recent</span>
                  </div>
                  {notifications.length === 0 ? (
                    <p className="notification-empty">Completed jobs submitted by you will appear here.</p>
                  ) : (
                    <ul className="notification-list">
                      {notifications.map((event, index) => (
                        <li key={event.event_id || `${event.aggregate_id}-${index}`}>
                          <span className="notification-dot" />
                          <span className="notification-copy">
                            <strong>Your job completed</strong>
                            <small>{event.payload?.job_type || 'Job'} · {event.aggregate_id}</small>
                          </span>
                          <button className="notification-open" onClick={() => {
                            setNotificationsOpen(false)
                            navigate('/submit/jobs')
                          }}>My jobs</button>
                        </li>
                      ))}
                    </ul>
                  )}
                </section>
              )}
            </div>
          </div>
        </header>
        <div className="page-content"><Outlet /></div>
        <footer className="footer"><span><Send size={13} /> Workstream User Portal</span><span>Job submission access</span></footer>
      </main>
    </div>
  )
}
