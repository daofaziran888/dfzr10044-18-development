import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  EntryRow,
  MigrationManifestView,
  MigrationReconcile,
  OverviewResult,
  PageResult,
} from '@/data/types'

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

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 冷层迁移交接 ----

async function readError(response: Response, fallback: string): Promise<string> {
  try {
    const payload = (await response.json()) as { detail?: string }
    return payload.detail || fallback
  } catch {
    return fallback
  }
}

export async function exportMigrationManifest(payload: {
  lake_table: string
  snapshot_id: string
  compression?: string
  manifest_id?: string
}): Promise<MigrationManifestView> {
  const response = await request('/api/storage_tier/manifests/export', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  if (!response.ok) throw new Error(await readError(response, '导出迁移清单失败'))
  return await response.json()
}

export function downloadMigrationManifest(manifestId: string): void {
  const anchor = document.createElement('a')
  anchor.href = `/api/storage_tier/manifests/${encodeURIComponent(manifestId)}/download`
  anchor.download = `${manifestId}.handoff.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
}

export async function reconcileMigrationManifest(manifestId: string): Promise<MigrationReconcile> {
  const response = await request(
    `/api/storage_tier/manifests/${encodeURIComponent(manifestId)}/reconcile`,
    { method: 'POST' },
  )
  if (!response.ok) throw new Error(await readError(response, '分层对账失败'))
  return await response.json()
}
