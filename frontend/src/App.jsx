import { Navigate, Route, Routes } from 'react-router-dom'
import AppShell from './components/AppShell'
import MemberShell from './components/MemberShell'
import ProtectedRoute from './components/ProtectedRoute'
import AuthPage from './pages/AuthPage'
import DashboardPage from './pages/DashboardPage'
import JobDetailsPage from './pages/JobDetailsPage'
import JobsPage from './pages/JobsPage'
import MemberJobsPage from './pages/MemberJobsPage'
import NewJobPage from './pages/NewJobPage'
import UsersPage from './pages/UsersPage'

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<AuthPage mode="login" />} />
      <Route path="/register" element={<AuthPage mode="register" />} />
      <Route element={<ProtectedRoute />}>
        <Route element={<ProtectedRoute allowedRoles={['admin']} />}>
          <Route element={<AppShell />}>
            <Route index element={<DashboardPage />} />
            <Route path="jobs" element={<JobsPage />} />
            <Route path="jobs/new" element={<NewJobPage />} />
            <Route path="jobs/:jobId" element={<JobDetailsPage />} />
            <Route path="users" element={<UsersPage />} />
          </Route>
        </Route>
        <Route element={<ProtectedRoute allowedRoles={['member']} />}>
          <Route path="/submit" element={<MemberShell />}>
            <Route index element={<NewJobPage />} />
            <Route path="jobs" element={<MemberJobsPage />} />
          </Route>
        </Route>
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}
