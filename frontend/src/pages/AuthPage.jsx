import { useState } from 'react'
import { Activity, ArrowRight, Command, Eye, EyeOff, ShieldCheck, Workflow } from 'lucide-react'
import { Link, Navigate, useLocation, useNavigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext'
import { useToast } from '../context/ToastContext'
import { getApiError } from '../services/api'

export default function AuthPage({ mode }) {
  const registering = mode === 'register'
  const { isAuthenticated, user, signIn, signUp, authLoading } = useAuth()
  const { notify } = useToast()
  const navigate = useNavigate()
  const location = useLocation()
  const [showPassword, setShowPassword] = useState(false)
  const [form, setForm] = useState({ organization_name: '', email: '', password: '' })
  const [error, setError] = useState('')

  if (isAuthenticated) return <Navigate to={user?.role === 'admin' ? '/' : '/submit'} replace />

  function update(event) {
    setError('')
    setForm((previous) => ({ ...previous, [event.target.name]: event.target.value }))
  }

  async function handleSubmit(event) {
    event.preventDefault()
    setError('')
    try {
      let session
      if (registering) {
        session = await signUp(form)
      } else {
        const { organization_name: _organizationName, ...credentials } = form
        session = await signIn(credentials)
      }
      notify(registering ? 'Workspace created. Welcome to Workstream.' : 'Welcome back.', 'success')
      const requestedPath = location.state?.from?.pathname
      const roleHome = session.user?.role === 'admin' ? '/' : '/submit'
      navigate(requestedPath && (session.user?.role === 'admin' ? requestedPath !== '/submit' : requestedPath === '/submit') ? requestedPath : roleHome, { replace: true })
    } catch (submitError) {
      const message = getApiError(submitError)
      setError(message)
    }
  }

  return (
    <div className="auth-page">
      <section className="auth-visual">
        <div className="auth-brand"><span className="brand-mark"><Command size={19} /></span>workstream<span className="brand-dot">.</span></div>
        <div className="visual-content">
          <span className="eyebrow"><span className="eyebrow-pulse" /> DISTRIBUTED WORK, IN SYNC</span>
          <h1>Reliable work.<br /><span>Real-time clarity.</span></h1>
          <p>One command center for every background job, worker, and event flowing through your platform.</p>
          <div className="visual-stats">
            <div><Workflow size={18} /><span><strong>Built to scale</strong><small>Concurrent worker pools</small></span></div>
            <div><Activity size={18} /><span><strong>Always in the loop</strong><small>Live job event stream</small></span></div>
            <div><ShieldCheck size={18} /><span><strong>Tenant isolated</strong><small>Your workspace, your data</small></span></div>
          </div>
        </div>
        <div className="visual-foot"><span className="tiny-pulse" /> SYSTEMS OPERATIONAL <span>·</span> DISTRIBUTED JOB PLATFORM</div>
      </section>
      <section className="auth-form-side">
        <div className="auth-mobile-brand"><span className="brand-mark"><Command size={18} /></span>workstream<span className="brand-dot">.</span></div>
        <div className="auth-form-wrap">
          <span className="form-overline">{registering ? 'GET STARTED' : 'WELCOME BACK'}</span>
          <h2>{registering ? 'Create your workspace' : 'Sign in to Workstream'}</h2>
          <p className="auth-subtitle">{registering ? 'Your command center for reliable background work.' : 'Your jobs are right where you left them.'}</p>

          {error && <div className="form-error" role="alert">{error}</div>}

          <form className="auth-form" onSubmit={handleSubmit}>
            {registering && <label>Organization name<input name="organization_name" value={form.organization_name} onChange={update} placeholder="Acme Inc." required autoComplete="organization" /></label>}
            <label>Work email<input type="email" name="email" value={form.email} onChange={update} placeholder="you@company.com" required autoComplete="email" /></label>
            <label>Password<div className="password-wrap"><input type={showPassword ? 'text' : 'password'} name="password" value={form.password} onChange={update} placeholder={registering ? 'At least 6 characters' : 'Enter your password'} minLength={registering ? 6 : undefined} required autoComplete={registering ? 'new-password' : 'current-password'} /><button type="button" className="password-toggle" onClick={() => setShowPassword((shown) => !shown)} aria-label={showPassword ? 'Hide password' : 'Show password'}>{showPassword ? <EyeOff size={17} /> : <Eye size={17} />}</button></div></label>
            <button className="button button-primary auth-submit" type="submit" disabled={authLoading}>{authLoading ? 'Please wait…' : registering ? 'Create workspace' : 'Sign in'}<ArrowRight size={17} /></button>
          </form>

          <div className="auth-switch">{registering ? 'Already have an account?' : 'New to Workstream?'} <Link to={registering ? '/login' : '/register'}>{registering ? 'Sign in' : 'Create a workspace'}</Link></div>
          <div className="auth-secure"><ShieldCheck size={14} /> Secured with organization-scoped access</div>
        </div>
        <div className="auth-legal">By continuing, you agree to use Workstream in accordance with your organization's policies.</div>
      </section>
    </div>
  )
}
