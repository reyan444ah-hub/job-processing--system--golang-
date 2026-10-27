import { useState } from 'react'
import { Activity, Bell, BriefcaseBusiness, Command, LayoutDashboard, LogOut, Plus, Radio, Users } from 'lucide-react'
import { NavLink, Outlet, useLocation, useNavigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext'
import { useToast } from '../context/ToastContext'
import { useJobEvents } from '../hooks/useJobEvents'

const titles = {
  '/': ['Overview', 'Your queue at a glance'],
  '/jobs': ['Job explorer', 'Track and manage every job'],
  '/jobs/new': ['Create a job', 'Submit work to your queue'],
  '/users': ['Users', 'Manage user access to this organization'],
}

export default function AppShell() {
  const { user, accessToken, signOut } = useAuth()
  const { notify } = useToast()
  const location = useLocation()
  const navigate = useNavigate()
  const [activity, setActivity] = useState([])
  const [notificationsOpen, setNotificationsOpen] = useState(false)
  const [unreadCount, setUnreadCount] = useState(0)
  const connection = useJobEvents(accessToken, (event) => {
    if (event.event === 'connected') return
    setActivity((items) => [event, ...items].slice(0, 5))
    setUnreadCount((count) => count + 1)
    if (event.event_type) notify(event.event_type.replaceAll('.', ' '), 'info')
    window.dispatchEvent(new CustomEvent('workstream:job-event', { detail: event }))
  })
  const title = titles[location.pathname] || (location.pathname.startsWith('/jobs/') ? ['Job details', 'Review the job lifecycle and event history'] : ['Workspace', ''])

  function toggleNotifications() {
    setNotificationsOpen((open) => !open)
    setUnreadCount(0)
  }

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
        <div className="brand">
          <span className="brand-mark"><Command size={19} strokeWidth={2.5} /></span>
          <span>workstream<span className="brand-dot">.</span></span>
        </div>
        <div className="workspace-switcher">
          <span className="workspace-avatar">{(user?.email || 'U').slice(0, 1).toUpperCase()}</span>
           <span className="workspace-copy"><strong>{user?.email?.split('@')[1] || 'My workspace'}</strong><small>{user?.role || 'Member'} workspace</small></span>
           {/* <ChevronDown size={15} className="muted" />  */}
        </div>
        <div className="nav-label">WORKSPACE</div>
        <nav className="side-nav">
          <NavLink to="/" end><LayoutDashboard size={17} />Overview</NavLink>
          <NavLink to="/jobs"><BriefcaseBusiness size={17} />Jobs</NavLink>
          <NavLink to="/users"><Users size={17} />Users</NavLink>
        </nav>
        <div className="sidebar-bottom">
          <div className="connection-card">
            <span className={`connection-dot connection-${connection}`} />
            <span><strong>{connection === 'connected' ? 'Live updates' : connection === 'reconnecting' ? 'Reconnecting' : 'Connecting'}</strong><small>Event stream</small></span>
            <Radio size={15} className="muted" />
          </div>
          {activity.length > 0 && (
            <div className="activity-mini">
              <span className="nav-label">LATEST ACTIVITY</span>
              {activity.slice(0, 2).map((event, index) => <span className="activity-line" key={`${event.event_id || index}`}>{event.event_type?.replaceAll('.', ' ') || 'Job event'}</span>)}
            </div>
          )}
          <button className="profile-row" onClick={handleSignOut}>
            <span className="profile-avatar">{(user?.email || 'U').slice(0, 1).toUpperCase()}</span>
            <span className="profile-copy"><strong>{user?.email || 'Signed-in user'}</strong><small>{user?.role || 'Member'}</small></span>
            <LogOut size={16} />
          </button>
        </div>
      </aside>

      <main className="main-area">
        <header className="topbar">
          <div className="topbar-heading"><h1>{title[0]}</h1><span>{title[1]}</span></div>
          <div className="topbar-actions">
            <span className="live-pill"><span className={`connection-dot connection-${connection}`} />{connection === 'connected' ? 'Live' : 'Connecting'}</span>
            <div className="notification-wrap">
              <button
                className="icon-button notification-button"
                aria-label="Notifications"
                aria-expanded={notificationsOpen}
                aria-controls="notification-panel"
                onClick={toggleNotifications}
              >
                <Bell size={17} />
                {unreadCount > 0 && <i aria-label={`${unreadCount} unread notifications`} />}
              </button>
              {notificationsOpen && (
                <section className="notification-panel" id="notification-panel" aria-label="Recent notifications">
                  <div className="notification-panel-heading">
                    <strong>Notifications</strong>
                    <span>{activity.length} recent</span>
                  </div>
                  {activity.length === 0 ? (
                    <p className="notification-empty">No notifications yet. Job updates will appear here.</p>
                  ) : (
                    <ul className="notification-list">
                      {activity.map((event, index) => (
                        <li key={event.event_id || `${event.event_type}-${index}`}>
                          <span className="notification-dot" />
                          <span className="notification-copy">
                            <strong>{event.event_type?.replaceAll('.', ' ') || 'Job event'}</strong>
                            {event.aggregate_id && <small>Job {event.aggregate_id}</small>}
                          </span>
                          {event.aggregate_id && (
                            <button
                              className="notification-open"
                              aria-label={`Open job ${event.aggregate_id}`}
                              onClick={() => {
                                setNotificationsOpen(false)
                                navigate(`/jobs/${event.aggregate_id}`)
                              }}
                            >
                              View
                            </button>
                          )}
                        </li>
                      ))}
                    </ul>
                  )}
                </section>
              )}
            </div>
            {location.pathname !== '/users' && <button className="button button-primary button-small" onClick={() => navigate('/jobs/new')}><Plus size={16} />New job</button>}
          </div>
        </header>
        <div className="page-content"><Outlet /></div>
        <footer className="footer"><span><Activity size={13} /> Workstream Operations</span><span>Distributed job platform</span></footer>
      </main>
    </div>
  )
}
