// src/apiClient.js
const API_BASE_URL = import.meta.env.VITE_API_URL || '/api'

// 通用錯誤處理
async function fetchJSON(path, options = {}) {
    const resp = await fetch(`${API_BASE_URL}${path}`, {
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        ...options,
    })
    if (!resp.ok) {
        let detail = ''
        try {
            const err = await resp.json()
            detail = err?.detail ? (typeof err.detail === 'string' ? err.detail : JSON.stringify(err.detail)) : ''
        } catch {
            try { detail = await resp.text() } catch { }
        }
        const msg = `${options.method || 'GET'} ${path} 失敗 (${resp.status}) ${resp.statusText}${detail ? `\n${detail}` : ''}`
        const e = new Error(msg)
        e.status = resp.status
        throw e
    }
    return resp.json()
}

// /health 的欄位正規化（修正 mysql 拼字判斷）
function normalizeHealth(data) {
    const hasES = typeof data?.elasticsearch === 'boolean'
    const hasSQL = typeof data?.mysql === 'boolean'
    const hasOpenAI = typeof data?.openai === 'boolean'
    const status = data?.status || 'ok'
    return {
        elasticsearch: hasES ? data.elasticsearch : status === 'healthy',
        mysql: hasSQL ? data.mysql : status === 'healthy',
        openai: hasOpenAI ? data.openai : status === 'healthy',
        status: status === 'healthy' ? 'ok' : (status === 'degraded' ? 'warning' : 'error'),
    }
}

// /query 的回傳欄位映射：對齊前端現有 UI
function mapQueryResponse(data) {
    return {
        ...data,
        search_mode: data.mode,
        total_hits: data.total,
        sources: data.documents,
        processing_time_ms: data.search_time_ms,
        processed_query: data.processed_query ?? data.query ?? '',
        answer: data.gpt_response,
    }
}

// ==== 導出給前端用的 API ====

// 根路徑（可做 smoke test）
export async function pingRoot() {
    return fetchJSON('/', { method: 'GET' })
}

// 系統健康檢查
export async function getHealth() {
    const raw = await fetchJSON('/health', { method: 'GET' })
    return normalizeHealth(raw)
}

// 系統統計
export async function getStats() {
    return fetchJSON('/stats', { method: 'GET' })
}

// 搜尋
export async function postQuery(payload) {
    // 建議保守只送後端有定義的欄位；其他進階參數可待 UI 成熟再開
    const body = JSON.stringify({
        query: String(payload.query || '').trim(),
        mode: payload.mode || 'hybrid',
        top_k: Number(payload.top_k ?? 5),
        use_gpt: Boolean(payload.use_gpt ?? true),
        // doc_type_filter, date_from, date_to, department 可視需求補上
    })
    const raw = await fetchJSON('/query', { method: 'POST', body })
    return mapQueryResponse(raw)
}

// 取得文件（若後端支援 /docs?id= 或 /docs/<id>，依你的後端實際路由調整）
export async function getDoc(params = {}) {
    const { id } = params
    if (!id) throw new Error('getDoc 需要提供 id')
    return fetchJSON(`/docs?id=${encodeURIComponent(id)}`, { method: 'GET' })
}
