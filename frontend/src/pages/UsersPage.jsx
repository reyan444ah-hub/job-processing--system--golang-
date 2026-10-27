import { useCallback, useEffect, useState } from 'react'
import { RefreshCw, UserPlus, Users } from 'lucide-react'
import Loading from '../components/Loading'
import { useToast } from '../context/ToastContext'
import { getApiError } from '../services/api'
import { createUser, listUsers } from '../services/users'

export default function UsersPage() {
  const [users, setUsers] = useState([])
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState('')
  const [form, setForm] = useState({ email: '', password: '' })
  const { notify } = useToast()

  const loadUsers = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      setUsers(await listUsers())
    } catch (loadError) {
      setError(getApiError(loadError))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { loadUsers() }, [loadUsers])

  async function handleSubmit(event) {
    event.preventDefault()
    setError('')
    setSaving(true)
    try {
      const user = await createUser(form)
      setUsers((current) => [...current, user])
      setForm({ email: '', password: '' })
      notify('User account created. Share the login details securely.', 'success')
    } catch (submitError) {
      setError(getApiError(submitError))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="page-stack">
      <section className="jobs-toolbar">
        <div className="jobs-intro"><span className="eyebrow">ACCESS CONTROL</span><h2>Users</h2><p>Create user accounts that can submit jobs to this organization.</p></div>
        <button className="button button-secondary" onClick={loadUsers} disabled={loading}><RefreshCw size={15} />Refresh</button>
      </section>

      {error && <div className="inline-error" role="alert">{error}</div>}

      <section className="panel detail-panel">
        <div className="panel-title"><UserPlus size={16} /><h3>Create a user account</h3></div>
        <form className="user-create-form" onSubmit={handleSubmit}>
          <label>Email address<input type="email" name="email" value={form.email} onChange={(event) => setForm((current) => ({ ...current, email: event.target.value }))} autoComplete="email" required /></label>
          <label>Temporary password<input type="password" name="password" value={form.password} onChange={(event) => setForm((current) => ({ ...current, password: event.target.value }))} minLength={6} autoComplete="new-password" required /><small>At least 6 characters. Share it with the user using a secure channel.</small></label>
          <button className="button button-primary" type="submit" disabled={saving}>{saving ? 'Creating…' : <><UserPlus size={15} />Create user</>}</button>
        </form>
      </section>

      <section className="section-block">
        <div className="section-heading"><div><span className="eyebrow">ORGANIZATION ACCESS</span><h2>Accounts</h2></div><span className="count-pill">{users.length}</span></div>
        <div className="panel team-list" aria-busy={loading}>
          {loading ? <Loading label="Loading accounts…" /> : users.map((user) => (
            <div className="team-row" key={user.id}>
              <span className="workspace-avatar">{user.email.slice(0, 1).toUpperCase()}</span>
              <span className="team-user"><strong>{user.email}</strong><small>Added {new Date(user.created_at).toLocaleDateString()}</small></span>
              <span className={`role-badge role-${user.role}`}>{user.role}</span>
            </div>
          ))}
          {!loading && users.length === 0 && <div className="team-empty"><Users size={18} />No accounts found.</div>}
        </div>
      </section>
    </div>
  )
}
