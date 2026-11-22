import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function listRecoveryCandidates(
  key: string,
  options: {
    failedOnly?: boolean
    keyword?: string
    cursor?: string | null
    page?: number
    size?: number
  } = {},
): Promise<PageResult> {
  const query = new URLSearchParams()
  if (options.failedOnly) query.set('failed_only', 'true')
  if (options.keyword) query.set('keyword', options.keyword)
  if (options.cursor) query.set('cursor', options.cursor)
  query.set('page', String(options.page ?? 1))
  query.set('size', String(options.size ?? 20))
  const response = await request(`/api/${key}/recovery-candidates?${query.toString()}`)
  if (!response.ok) throw new Error('恢复候选读取失败')
  return await response.json()
}

export async function recoverFromCandidate(
  key: string,
  candidate: Record<string, unknown>,
  action = '确认恢复',
): Promise<ActionResult> {
  const response = await request(`/api/${key}/recover`, {
    method: 'POST',
    body: JSON.stringify({
      candidate,
      entry_id: candidate.entry_id ?? null,
      record_code: candidate['记录编号'] ?? null,
      action,
    }),
  })
  return await response.json()
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}
