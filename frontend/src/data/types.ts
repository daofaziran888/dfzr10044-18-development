/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

export type MigrationManifestEntry = {
  file_id: string
  lake_table: string
  partition_key: string
  file_rows: number
  file_bytes: number
  file_manifest: string
  tier: string
  read_location: string
}

export type MigrationManifestView = {
  manifest_id: string
  lake_table: string
  snapshot_id: string
  compression: string
  entry_count: number
  entries: MigrationManifestEntry[]
  created_at: number
}

export type MigrationReconcile = {
  ok: boolean
  manifest_id: string
  entry_count: number
  migrated_bytes: number
  reconciled_now: number
  already_reconciled: boolean
  total_reconciled: number
}
