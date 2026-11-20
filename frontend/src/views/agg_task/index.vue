<template>
  <section class="page" data-module="agg_task">
    <header class="page-head">
      <div><h2>聚合任务</h2><p class="page-desc">维护定时聚合计算任务的配置、依赖关系和执行调度，驱动指标批量和增量产出。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记聚合计算</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>
    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" @click="reload">查询</button>
    </div>
    <div class="timeline-wrap">
      <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
        <span class="timeline-label">{{ item.name || item.label }}</span>
        <div class="timeline-track">
          <div class="timeline-bar" :class="`status-${item.status}`" :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }" :data-testid="`bar-${item.id}`" :title="`${item.start} ~ ${item.end}`" @click="selectItem(item)">&nbsp;</div>
        </div>
        <span style="font-size:12px;color:var(--muted);margin-left:8px;">{{ item.start }} ~ {{ item.end }}</span>
      </div>
      <p v-if="!timelineItems.length" style="text-align:center;color:var(--muted)">暂无作业记录</p>
    </div>
    <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
      <strong>{{ selectedItem.name || selectedItem.label }}</strong> — 状态：{{ selectedItem.status }}
      <div style="margin-top:4px;">{{ selectedItem.logs || '暂无日志' }}</div>
    </div>

    <section class="recovery-panel" data-testid="recovery-panel">
      <div class="page-head">
        <div><h3>失败任务恢复批次</h3><p class="page-desc">一次勾选多条仍处于「计算失败」的任务提交，系统真实触发调度复算；找不到指标定义的任务会留在批次里标记未开跑，已成功任务不会被重新执行。</p></div>
        <div class="page-actions">
          <button class="btn" type="button" data-testid="refresh-recovery" @click="loadFailedTasks">刷新失败任务</button>
          <button class="btn primary" type="button" data-testid="submit-recovery" :disabled="submitting || !selectedTaskIds.length" @click="submitRecovery">
            {{ submitting ? '提交中…' : `提交恢复批次（${selectedTaskIds.length}）` }}
          </button>
        </div>
      </div>
      <p v-if="errorMessage" class="error-text" data-testid="recovery-error">{{ errorMessage }}</p>
      <p v-if="successMessage" style="color:#027a48;font-size:13px" data-testid="recovery-success">{{ successMessage }}</p>
      <table class="data-table">
        <thead>
          <tr><th style="width:40px">选择</th><th>任务ID</th><th>任务编号</th><th>目标指标</th><th>当前状态</th><th>异常标记</th></tr>
        </thead>
        <tbody>
          <tr v-for="task in failedTasks" :key="String(task.id)" :data-testid="`failed-row-${task.id}`">
            <td><input type="checkbox" :value="task.id" v-model="checkedTaskIds" :data-testid="`pick-${task.id}`" /></td>
            <td>{{ task.id }}</td>
            <td>{{ task['任务编号'] }}</td>
            <td>{{ task['目标指标'] }}</td>
            <td>{{ task.status }}</td>
            <td>{{ task.abnormal ? '是' : '否' }}</td>
          </tr>
          <tr v-if="!failedTasks.length">
            <td colspan="6" class="empty-state">当前没有处于「计算失败」的聚合任务</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="recovery-panel" data-testid="batch-history">
      <div class="page-head">
        <div><h3>恢复批次监控</h3><p class="page-desc">按批次核对每轮真正取得的指标数据与仍停留在上次失败的任务，未开跑项不会用旧状态冒充本轮结果。</p></div>
      </div>
      <table class="data-table">
        <thead>
          <tr><th>批次编号</th><th>批次状态</th><th>本轮取得新指标数据</th><th>仍停留失败</th><th>跳过未执行</th><th>提交时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="batch in batches" :key="batch.batch_id" :data-testid="`batch-row-${batch.batch_id}`">
            <td>{{ batch.batch_id }}</td>
            <td>{{ batch.status }}</td>
            <td>{{ formatTaskList(batch.items.filter((i: any) => i.metric_data)) }}</td>
            <td>{{ formatTaskList(batch.items.filter((i: any) => !i.metric_data)) }}</td>
            <td>{{ batch.skipped.length ? batch.skipped.map((s: any) => `#${s.task_id}`).join('、') : '—' }}</td>
            <td>{{ batch.created_at }}</td>
          </tr>
          <tr v-if="!batches.length">
            <td colspan="6" class="empty-state">暂无恢复批次</td>
          </tr>
        </tbody>
      </table>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记聚合计算</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/agg_task'
const columns = ["任务编号", "目标指标", "聚合维度", "计算周期", "依赖上游", "并行度", "超时分钟", "重试上限"]
const timelineItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const successMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])
const failedTasks = ref<Record<string, any>[]>([])
const checkedTaskIds = ref<number[]>([])
const batches = ref<any[]>([])
const submitting = ref(false)
const selectedTaskIds = computed(() => checkedTaskIds.value)
function barLeft(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
function selectItem(item: any) { selectedItem.value = selectedItem.value?.id === item.id ? null : item }
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); allItems.value = data.items || []; applyFilter() } catch(e) { errorMessage.value = '加载失败' }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => i.start >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => i.end <= dateTo.value)
  timelineItems.value = items; selectedItem.value = null
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
function formatTaskList(items: any[]) {
  if (!items.length) return '—'
  return items.map((i) => `#${i.task_id}${i.metric_data ? `(${i.metric_data.metric_code})` : ''}`).join('、')
}
async function loadFailedTasks() {
  errorMessage.value = ''
  try {
    const res = await request(ENDPOINT)
    const data = await res.json()
    const ids = (data.items || []).map((i: any) => i.id)
    const details = await Promise.all(ids.map((id: number) => request(`${ENDPOINT}/${id}`).then((r) => r.json())))
    failedTasks.value = details.filter((d: any) => d.status === '计算失败')
    checkedTaskIds.value = checkedTaskIds.value.filter((id) => failedTasks.value.some((t) => t.id === id))
  } catch (e: any) {
    errorMessage.value = e.message || '失败任务加载失败'
  }
}
async function loadBatches() {
  try {
    const res = await request(`${ENDPOINT}/recovery-batches`)
    const data = await res.json()
    batches.value = data.items || []
  } catch (e: any) {
    errorMessage.value = e.message || '恢复批次加载失败'
  }
}
async function submitRecovery() {
  submitting.value = true
  errorMessage.value = ''
  successMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/recovery-batches`, {
      method: 'POST',
      body: JSON.stringify({ task_ids: [...checkedTaskIds.value] }),
    })
    const payload = await res.json()
    if (!res.ok || !payload.ok) throw new Error(payload.message || '恢复批次提交失败')
    successMessage.value = payload.message
    checkedTaskIds.value = []
    await Promise.all([reload(), loadFailedTasks(), loadBatches()])
  } catch (e: any) {
    errorMessage.value = e.message
  } finally {
    submitting.value = false
  }
}
onMounted(() => { void reload(); void loadFailedTasks(); void loadBatches() })
</script>
