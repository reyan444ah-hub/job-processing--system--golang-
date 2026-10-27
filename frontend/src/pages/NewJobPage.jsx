import { useRef, useState } from 'react'
import { ArrowLeft, Check, Copy, FileJson, Info, RotateCcw, Send, ShieldCheck } from 'lucide-react'
import { Link, useNavigate } from 'react-router-dom'
import { useAuth } from '../context/AuthContext'
import { useToast } from '../context/ToastContext'
import { getApiError } from '../services/api'
import { submitJob } from '../services/jobs'

const starterPayload = '{\n  "to": "client@example.com",\n  "subject": "Welcome!"\n}'

function makeIdempotencyKey(request) {
  const encoded = JSON.stringify(request)
  return `ws-${crypto.randomUUID()}-${encoded.length}`
}

export default function NewJobPage() {
  const navigate = useNavigate()
  const { user } = useAuth()
  const memberMode = user?.role === 'member'
  const { notify } = useToast()
  const keyByRequest = useRef(new Map())
  const [form, setForm] = useState({ type: 'email.send', payload: starterPayload, priority: '0', max_attempts: '3', scheduled_at: '' })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [keyUsed, setKeyUsed] = useState('')
  const [submittedJob, setSubmittedJob] = useState(null)
  const request = useRef(null)

  function update(event) {
    setError('')
    setSubmittedJob(null)
    setForm((current) => ({ ...current, [event.target.name]: event.target.value }))
  }

  async function sendSubmission(submission, idempotencyKey) {
    request.current = { submission, idempotencyKey }
    setKeyUsed(idempotencyKey)
    setError('')
    setLoading(true)
    try {
      const response = await submitJob(submission, idempotencyKey)
      notify(response.deduplicated ? 'This request matched an existing job; no duplicate was created.' : 'Job accepted and added to the queue.', 'success')
      if (memberMode) setSubmittedJob(response.job)
      else navigate(`/jobs/${response.job.id}`)
    } catch (submitError) {
      const message = getApiError(submitError)
      setError(message)
      if (submitError.response?.status === 409) {
        notify('This idempotency key conflicts with a prior request. Edit the request to generate a new key.', 'error')
      }
    } finally {
      setLoading(false)
    }
  }

  async function handleSubmit(event) {
    event.preventDefault()
    setError('')
    let parsedPayload
    try {
      parsedPayload = JSON.parse(form.payload)
    } catch {
      setError('Payload must be valid JSON.')
      return
    }
    const submission = {
      type: form.type.trim(),
      payload: parsedPayload,
      priority: Number(form.priority) || 0,
      max_attempts: Number(form.max_attempts) || 3,
      ...(form.scheduled_at ? { scheduled_at: new Date(form.scheduled_at).toISOString() } : {}),
    }
    if (!submission.type) {
      setError('Job type is required.')
      return
    }
    const fingerprint = JSON.stringify(submission)
    let idempotencyKey = keyByRequest.current.get(fingerprint)
    if (!idempotencyKey) {
      idempotencyKey = makeIdempotencyKey(submission)
      keyByRequest.current.set(fingerprint, idempotencyKey)
    }
    await sendSubmission(submission, idempotencyKey)
  }

  function retrySameRequest() {
    if (request.current && !loading) {
      sendSubmission(request.current.submission, request.current.idempotencyKey)
    }
  }

  async function copyKey() {
    if (!keyUsed) return
    try {
      await navigator.clipboard.writeText(keyUsed)
      notify('Idempotency key copied.', 'success')
    } catch {
      notify('Clipboard access was denied by the browser.', 'error')
    }
  }

  return (
    <div className="page-stack narrow-page">
      {!memberMode && <Link className="back-link" to="/jobs"><ArrowLeft size={15} />Back to jobs</Link>}
      <section className="jobs-intro new-job-intro"><span className="eyebrow">ASYNC SUBMISSION</span><h2>Define the work.</h2><p>Submit JSON payloads to your organization's background queue.</p></section>
      {submittedJob && <div className="submission-success" role="status"><strong>Job submitted successfully</strong><span>{submittedJob.type} · ID {submittedJob.id}</span></div>}
      {error && <div className="inline-error" role="alert"><span>{error}</span>{keyUsed && error !== 'Payload must be valid JSON.' && <button className="button button-quiet" onClick={retrySameRequest}><RotateCcw size={14} />Retry same request</button>}</div>}
      <form id="job-submit-form" className="panel job-form" onSubmit={handleSubmit}>
        <div className="form-section-head"><span className="form-section-icon"><FileJson size={17} /></span><div><strong>Job definition</strong><small>Specify a handler and its JSON input.</small></div></div>
        <label>Job type <span className="required-mark">*</span><input name="type" value={form.type} onChange={update} placeholder="email.send" required maxLength={100} /><small>Use a job handler registered by your backend workers.</small></label>
        <label>Payload <span className="required-mark">*</span><textarea className="code-input" name="payload" value={form.payload} onChange={update} spellCheck="false" rows={8} required /><small>Must be valid JSON. The payload shape depends on your worker handler.</small></label>
        <div className="form-two-col">
          <label>Priority<input name="priority" type="number" value={form.priority} onChange={update} min="0" max="100" /><small>Higher priority values run first when supported.</small></label>
          <label>Maximum attempts<input name="max_attempts" type="number" value={form.max_attempts} onChange={update} min="1" max="100" /><small>Defaults to 3 when left at 0.</small></label>
        </div>
        <label>Schedule for later <span className="optional-mark">OPTIONAL</span><input name="scheduled_at" type="datetime-local" value={form.scheduled_at} onChange={update} /><small>Leave empty to enqueue the job immediately.</small></label>
        <div className="idempotency-info"><span><ShieldCheck size={17} /></span><p><strong>Duplicate-safe submission</strong><br />A stable key is reused when you retry the exact same request.</p><Info size={15} className="idempotency-help" /></div>
        {keyUsed && <div className="key-row"><span><small>KEY FOR THIS REQUEST</small><code>{keyUsed}</code></span><button type="button" className="icon-button" onClick={copyKey} aria-label="Copy idempotency key"><Copy size={15} /></button></div>}
        <div className="form-actions">{!memberMode && <Link className="button button-quiet" to="/jobs">Cancel</Link>}<button className="button button-primary" type="submit" disabled={loading}>{loading ? <><span className="spinner spinner-light" />Submitting…</> : <><Send size={15} />Submit job</>}</button></div>
      </form>
    </div>
  )
}
