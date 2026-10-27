import { beforeEach, describe, expect, it, vi } from 'vitest'

const { apiGet } = vi.hoisted(() => ({ apiGet: vi.fn() }))
vi.mock('./api', () => ({ api: { get: apiGet } }))

import { listAllJobs, listMyJobs } from './jobs'

describe('listAllJobs', () => {
  beforeEach(() => apiGet.mockReset())

  it('fetches all pages using the actual status filter and pagination parameters', async () => {
    apiGet
      .mockResolvedValueOnce({ data: { jobs: Array.from({ length: 100 }, (_, index) => ({ id: index })), total: 101 } })
      .mockResolvedValueOnce({ data: { jobs: [{ id: 100 }], total: 101 } })

    const result = await listAllJobs({ status: 'failed' })

    expect(result).toHaveLength(101)
    expect(apiGet).toHaveBeenNthCalledWith(1, '/jobs', { params: { status: 'failed', limit: 100, offset: 0 } })
    expect(apiGet).toHaveBeenNthCalledWith(2, '/jobs', { params: { status: 'failed', limit: 100, offset: 100 } })
  })

  describe('listMyJobs', () => {
    beforeEach(() => apiGet.mockReset())

    it('uses the member-scoped endpoint and forwards pagination and status', async () => {
      apiGet.mockResolvedValue({ data: { jobs: [], total: 0 } })

      await listMyJobs({ status: 'completed', limit: 10, offset: 20 })

      expect(apiGet).toHaveBeenCalledWith('/my/jobs', {
        params: { status: 'completed', limit: 10, offset: 20 },
      })
    })
  })
})
