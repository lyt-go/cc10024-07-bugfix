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

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <label v-if="isRecovery" class="filter-item">
        <span>只看失败</span>
        <input
          v-model="failedOnly"
          data-testid="filter-failed-only"
          type="checkbox"
          @change="resetCursorAndReload"
        />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in displayFields" :key="column">{{ column }}</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in displayFields" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions">
            <button
              v-if="isRecovery"
              class="link"
              type="button"
              :data-testid="`recover-${row.id}`"
              @click="recover(row)"
            >
              按此运行重采
            </button>
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
          <td :colspan="displayFields.length + 2" class="empty-state">
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

    <div v-if="isRecovery" class="page-actions cursor-bar">
      <button class="btn ghost" data-testid="cursor-prev" type="button" :disabled="!cursorStack.length" @click="previousPage">
        上一页
      </button>
      <button class="btn" data-testid="cursor-next" type="button" :disabled="!nextCursor" @click="nextPage">
        下一页
      </button>
      <span class="cursor-note">按运行时间稳定翻页，同一秒落盘也能逐页取全</span>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条{{ meta.name }}记录</span>
      <span v-if="recoveryMessage" class="ok-text">{{ recoveryMessage }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  createEntry,
  downloadEntries,
  listEntries,
  listRecoveryCandidates,
  moduleMeta,
  recoverFromCandidate,
  runAction as applyAction,
} from '@/api/backend-service'
import type { EntryRow, RecoveryCandidate } from '@/data/types'

const props = defineProps<{ moduleKey: string }>()
const meta = moduleMeta(props.moduleKey)
// 运行历史与错误恢复共用合并后的恢复候选：谁选中的运行就由谁发起重采。
const isRecovery = computed(() => meta.key === 'run_history' || meta.key === 'error_recover')
const rows = ref<EntryRow[]>([])
const total = ref(0)
const errorMessage = ref('')
const recoveryMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = meta.fields.slice(0, 3)
// 恢复视图展示的是合并后的运行候选，列要能看出实际采集来源与失败范围。
const displayFields = computed(() =>
  isRecovery.value
    ? ['记录编号', '关联计划', '开始时间', '采集来源', '失败范围', '执行日志']
    : meta.fields,
)
const failedOnly = ref(false)
const nextCursor = ref<string | null>(null)
const cursorStack = ref<(string | null)[]>([])
const currentCursor = ref<string | null>(null)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
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
  failedOnly.value = false
  cursorStack.value = []
  currentCursor.value = null
  void reload()
}

function resetCursorAndReload() {
  cursorStack.value = []
  currentCursor.value = null
  void reload()
}

async function nextPage() {
  if (!nextCursor.value) return
  cursorStack.value = [...cursorStack.value, currentCursor.value]
  currentCursor.value = nextCursor.value
  await reload()
}

async function previousPage() {
  if (!cursorStack.value.length) return
  const stack = [...cursorStack.value]
  currentCursor.value = stack.pop() ?? null
  cursorStack.value = stack
  await reload()
}

async function recover(row: EntryRow) {
  errorMessage.value = ''
  recoveryMessage.value = ''
  try {
    // 恢复必须带上选中的那次运行本身，后端会核对运行ID/记录编号。
    const candidate = row as RecoveryCandidate
    const result = await recoverFromCandidate(meta.key, {
      ...candidate,
      entry_id: candidate.entry_id ?? Number(row.id),
    })
    if (!result.ok) throw new Error(result.message)
    recoveryMessage.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '发起重采失败'
  }
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
    if (isRecovery.value) {
      const keyword = filters.value[meta.fields[0]] || undefined
      const payload = await listRecoveryCandidates(meta.key, {
        failedOnly: failedOnly.value,
        keyword,
        cursor: currentCursor.value,
        size: 20,
      })
      rows.value = payload.items
      total.value = payload.total
      nextCursor.value = payload.next_cursor ?? null
      return
    }
    const payload = await listEntries(meta.key, filters.value)
    rows.value = payload.items
    total.value = payload.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '业务列表读取失败'
  }
}

onMounted(reload)
</script>
