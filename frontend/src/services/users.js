import { api } from './api'

export async function listUsers() {
  const { data } = await api.get('/admin/users')
  return data.users || []
}

export async function createUser(credentials) {
  const { data } = await api.post('/admin/users', credentials)
  return data.user
}
