<template>
  <section class="page" :data-module="meta.key">
    <header class="page-head">
      <div>
        <h2>{{ meta.name }}</h2>
        <p class="page-desc">{{ meta.desc }}</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">
          登记{{ meta.entity }}
        </button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">
          导出{{ meta.name }}清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <p class="status-legend">
      <span v-for="item in statusSummary" :key="item.status" class="legend-item">
        {{ item.status }}：{{ item.count }}
      </span>
    </p>

    <section v-if="isStorageTier" class="handoff-panel" data-testid="handoff-panel">
      <header class="handoff-head">
        <h3>冷文件交接包</h3>
        <p class="page-desc">
          按快照导出迁移清单只用冷层文件作为迁移对象；回执确认后读取位置才切到校验过的冷副本。
        </p>
      </header>
      <div class="handoff-actions">
        <input v-model="handoff.lakeTable" data-testid="handoff-lake-table" placeholder="湖表名" />
        <input v-model="handoff.snapshotId" data-testid="handoff-snapshot" placeholder="快照标识" />
        <button class="btn" data-testid="handoff-export" type="button" @click="exportHandoffManifest">
          导出迁移清单
        </button>
      </div>
      <p v-if="handoff.message" class="error-text" data-testid="handoff-message">{{ handoff.message }}</p>
      <article v-if="handoff.manifest" class="handoff-card" data-testid="handoff-manifest">
        <strong>{{ handoff.manifest.manifest_id }}</strong>
        <span>快照 {{ handoff.manifest.snapshot_id }}</span>
        <span>迁移对象 {{ handoff.manifest.entry_count }} 个冷层文件</span>
        <ul>
          <li v-for="entry in handoff.manifest.entries" :key="entry.file_id">
            {{ entry.file_id }} · 分区 {{ entry.partition_key }} · {{ entry.file_rows }} 行 ·
            导出时读取位置 {{ entry.read_location }}
          </li>
        </ul>
        <div class="page-actions">
          <button class="btn" data-testid="handoff-download" type="button" @click="downloadHandoffManifest">
            下载交接包
          </button>
          <button class="btn" data-testid="handoff-reconcile" type="button" @click="reconcileHandoffManifest">
            分层对账
          </button>
        </div>
        <p v-if="handoff.reconcileMessage" class="error-text" data-testid="handoff-reconcile-message">
          {{ handoff.reconcileMessage }}
        </p>
      </article>
    </section>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in meta.fields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in meta.fields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-for="action in meta.actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="meta.fields.length + 2" class="empty-state">
            暂无{{ meta.name }}数据，可先登记{{ meta.entity }}
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记{{ meta.entity }}</h3>
        <label v-for="field in meta.fields" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  downloadMigrationManifest,
  exportMigrationManifest,
  listEntries,
  moduleMeta,
  reconcileMigrationManifest,
  runAction as applyAction,
} from '@/api/backend-service'
import type { EntryRow, MigrationManifestView } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
const isStorageTier = props.moduleKey === 'storage_tier'
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const handoff = ref<{
  lakeTable: string
  snapshotId: string
  manifest: MigrationManifestView | null
  message: string
  reconcileMessage: string
}>({ lakeTable: '', snapshotId: '', manifest: null, message: '', reconcileMessage: '' })
const stats = computed(() => [
  { label: meta.metrics[0], value: total.value },
  { label: meta.metrics[1], value: rows.value.filter((row) => row.status === meta.statuses[1]).length },
  {
    label: meta.metrics[2],
    value: rows.value.filter((row) => row.status === meta.statuses[meta.statuses.length - 1]).length,
  },
])
const statusSummary = computed(() =>
  meta.statuses.map((status) => ({
    status,
    count: rows.value.filter((row) => String(row.status) === status).length,
  })),
)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  try {
    await downloadEntries(meta.key, filters.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(meta.fields.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function exportHandoffManifest() {
  handoff.value.message = ''
  handoff.value.reconcileMessage = ''
  try {
    handoff.value.manifest = await exportMigrationManifest({
      lake_table: handoff.value.lakeTable.trim(),
      snapshot_id: handoff.value.snapshotId.trim(),
    })
  } catch (error) {
    handoff.value.manifest = null
    handoff.value.message = error instanceof Error ? error.message : '导出迁移清单失败'
  }
}

function downloadHandoffManifest() {
  if (!handoff.value.manifest) return
  downloadMigrationManifest(handoff.value.manifest.manifest_id)
}

async function reconcileHandoffManifest() {
  if (!handoff.value.manifest) return
  handoff.value.reconcileMessage = ''
  try {
    const result = await reconcileMigrationManifest(handoff.value.manifest.manifest_id)
    handoff.value.reconcileMessage = result.already_reconciled
      ? `已在 ${result.total_reconciled} 字节处核销过，本次不重复核销`
      : `本次核销 ${result.reconciled_now} 字节`
  } catch (error) {
    handoff.value.reconcileMessage = error instanceof Error ? error.message : '分层对账失败'
  }
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const result = await createEntry(meta.key, createValues.value)
    if (!result.ok) throw new Error(result.message)
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: EntryRow) {
  errorMessage.value = ''
  try {
    const result = await applyAction(meta.key, Number(row.id), action)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>
