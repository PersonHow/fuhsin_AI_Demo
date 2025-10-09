// src/api/apiClient.js
const API_BASE_URL = import.meta.env.VITE_API_URL || '/api'

// 通用錯誤處理
async function fetchJSON(path, options = {}) {
    const resp = await fetch(`${API_BASE_URL}${path}`, {
        headers: { 
            'Content-Type': 'application/json', 
            Accept: 'application/json' 
        },
        ...options,
    })
    
    if (!resp.ok) {
        let detail = ''
        try {
            const err = await resp.json()
            detail = err?.detail ? 
                (typeof err.detail === 'string' ? err.detail : JSON.stringify(err.detail)) 
                : ''
        } catch {
            try { 
                detail = await resp.text() 
            } catch { }
        }
        
        const msg = `${options.method || 'GET'} ${path} 失敗 (${resp.status}) ${resp.statusText}${detail ? `\n${detail}` : ''}`
        const e = new Error(msg)
        e.status = resp.status
        throw e
    }
    
    return resp.json()
}

// /health 的欄位正規化
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
        indices: data?.indices || [],
        timestamp: data?.timestamp
    }
}

// /query 的回傳欄位映射：對齊後端格式
function mapQueryResponse(data) {
    // 處理文檔資料，直接使用後端的 DocumentInfo 結構
    const sources = (data.documents || []).map(doc => {
        // 提取高亮內容
        const highlight = doc.highlight || {}
        
        // 構建搜索內容（用於顯示）
        let searchable_content = doc.summary || ''
        
        // 如果有高亮，優先使用高亮內容
        if (Object.keys(highlight).length > 0) {
            const firstHighlight = Object.values(highlight)[0]
            if (Array.isArray(firstHighlight) && firstHighlight.length > 0) {
                searchable_content = firstHighlight.join(' ... ')
            }
        }
        
        return {
            // 基本資訊 - 直接對應後端 DocumentInfo
            doc_id: doc.doc_id,
            doc_number: doc.doc_number || '',
            doc_type: doc.doc_type || '未分類',
            
            // 檔案資訊
            file_name: doc.file_name || doc.title || '未命名文件',
            file_url: doc.file_url || '',
            
            // 內容資訊
            title: doc.title || doc.file_name || '',
            summary: doc.summary || '',
            searchable_content: searchable_content,
            
            // 日期與人員
            doc_date: doc.issue_date || '',  // 後端用 issue_date
            department: doc.department || '',
            applicant: doc.applicant || '',
            
            // 產品與關鍵字
            product_codes: doc.product_codes || [],
            keywords: doc.keywords || [],
            
            // 搜尋相關
            score: doc.score || 0,
            highlight: highlight,  // 後端用 highlight (單數)
            index: doc.index_name || 'unknown',  // 後端用 index_name
            
            // 內容欄位（用於展開顯示）
            content: doc.summary || ''
        }
    })
    
    return {
        // 搜尋基本資訊 - 直接對應後端 SearchResponse
        query: data.query,
        processed_query: data.query,
        search_mode: data.mode,  // 後端用 mode
        
        // 結果資訊
        success: data.success,
        total_hits: data.total,  // 後端用 total
        sources: sources,
        
        // AI 回應
        answer: data.gpt_response || null,  // 後端用 gpt_response
        
        // 效能資訊
        processing_time_ms: data.search_time_ms,  // 後端用 search_time_ms
        
        // 元資料 - 直接對應後端 metadata
        metadata: {
            mysql_hits: data.metadata?.mysql_hits || 0,
            product_ids_found: data.metadata?.product_ids_found || [],
            keywords_used: data.metadata?.keywords_used || [],
            indices_searched: data.metadata?.indices_searched || '',
            index_distribution: data.metadata?.index_distribution || {}
        }
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
    const raw = await fetchJSON('/stats', { method: 'GET' })
    
    // 格式化統計資料 - 對應後端 /stats 回傳格式
    if (raw.success && raw.stats) {
        return {
            success: true,
            stats: {
                // 索引統計
                total_documents: raw.stats.total_documents || 0,
                index_counts: raw.stats.index_counts || {},
            },
            timestamp: raw.timestamp
        }
    }
    
    return raw
}

// 搜尋
export async function postQuery(payload) {
    // 構建請求 payload - 對應後端 SearchRequest
    const body = JSON.stringify({
        query: String(payload.query || '').trim(),
        mode: payload.mode || 'hybrid',
        top_k: Number(payload.top_k ?? 10),
        use_gpt: Boolean(payload.use_gpt ?? true),
        doc_type_filter: payload.doc_type_filter || null,
        date_from: payload.date_from || null,
        date_to: payload.date_to || null,
        department: payload.department || null
    })
    
    console.log('發送搜尋請求:', body)
    
    const raw = await fetchJSON('/query', { method: 'POST', body })
    const mapped = mapQueryResponse(raw)
    
    console.log('搜尋回應:', mapped)
    
    return mapped
}

// 取得單一文件詳情
export async function getDoc(params = {}) {
    const { id } = params
    if (!id) throw new Error('getDoc 需要提供 id')
    
    const raw = await fetchJSON(`/document/${encodeURIComponent(id)}`, { method: 'GET' })
    
    if (raw.success && raw.document) {
        return {
            success: true,
            document: raw.document,
            related_documents: raw.related_documents || []
        }
    }
    
    return raw
}

// 批量取得文件
export async function getDocuments(docIds = []) {
    if (!Array.isArray(docIds) || docIds.length === 0) {
        throw new Error('getDocuments 需要提供文件 ID 陣列')
    }
    
    // 由於後端沒有批量查詢端點，這裡使用 Promise.all 並行查詢
    const promises = docIds.map(id => getDoc({ id }))
    const results = await Promise.allSettled(promises)
    
    return results
        .filter(r => r.status === 'fulfilled')
        .map(r => r.value.document)
}

// 導出 API 基礎 URL（用於除錯）
export { API_BASE_URL }
