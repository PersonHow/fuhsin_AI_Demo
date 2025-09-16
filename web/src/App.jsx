import { useState, useEffect, useCallback } from 'react'
import './App.scss'

// 元件
import Header from './components/Header'
import SearchBar from './components/SearchBar'
import SearchResults from './components/SearchResults'
import StatusIndicator from './components/StatusIndicator'
import SearchModeSelector from './components/SearchModeSelector'
import LoadingSpinner from './components/LoadingSpinner'

/**
 * API 基底網址
 * - 生產環境 (Docker) ：VITE_API_URL 設為 "/api" （由後端代理）
 * - 開發環境 (Vite) ：可在 .env 設定 VITE_API_URL，或透過 dev server proxy 代理
 */
const API_BASE_URL = import.meta.env.VITE_API_URL || '/api'

export default function App() {
  // ───────────────────────── 狀態管理 ─────────────────────────
  const [searchQuery, setSearchQuery] = useState('')               // 使用者輸入的查詢字串
  const [searchMode, setSearchMode] = useState('hybrid')           // 搜尋模式 (keyword | vector | hybrid)
  const [searchResults, setSearchResults] = useState(null)         // 搜尋結果
  const [isLoading, setIsLoading] = useState(false)                // 是否載入中
  const [error, setError] = useState(null)                         // 錯誤訊息
  const [systemStatus, setSystemStatus] = useState({               // 系統健康狀態
    elasticsearch: false,
    openai: false,
    status: 'checking'
  })
  const [searchHistory, setSearchHistory] = useState([])           // 搜尋歷史紀錄
  const [useGPT, setUseGPT] = useState(true)                       // 是否使用 GPT 生成答案
  const [topK, setTopK] = useState(5)                              // 回傳結果數量

  // ─────────────────────── 系統健康檢查 ───────────────────────
  /**
   * 將 /health API 回傳結果正規化，確保舊版或新版後端格式都能處理。
   * - 舊版後端：{ elasticsearch: boolean, openai: boolean, status: 'ok'|'error' }
   * - 新版後端：{ status: 'ok', time: <number> }
   */
  const normalizeHealth = (data) => {
    const hasES = typeof data?.elasticsearch === 'boolean'
    const hasOpenAI = typeof data?.openai === 'boolean'
    const status = data?.status || 'ok'
    return {
      elasticsearch: hasES ? data.elasticsearch : status === 'ok',
      openai: hasOpenAI ? data.openai : true, // 如果沒有明確提供，預設為 true
      status
    }
  }

  // 呼叫後端 /health 取得系統狀態
  const checkSystemHealth = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/health`)
      if (response.ok) {
        const data = await response.json()
        setSystemStatus(normalizeHealth(data))
      } else {
        setSystemStatus({ elasticsearch: false, openai: false, status: 'error' })
      }
    } catch (err) {
      console.error('Health check failed:', err)
      setSystemStatus({ elasticsearch: false, openai: false, status: 'error' })
    }
  }, [])

  // 初始化時執行一次健康檢查，之後每 30 秒重複檢查一次
  useEffect(() => {
    checkSystemHealth()
    const interval = setInterval(checkSystemHealth, 30000)
    return () => clearInterval(interval)
  }, [checkSystemHealth])

  // ──────────────────────── 搜尋處理 ─────────────────────────
  /**
   * 執行搜尋請求
   * - 使用新版後端 API：POST /query
   * - Body 格式：{ query, mode, top_k, use_gpt, index_pattern }
   * - mode 參數修正為後端期待的名稱
   */
  const handleSearch = async (query = searchQuery) => {
    if (!query.trim()) {
      setError('請輸入搜尋內容')
      return
    }
    
    console.log('搜尋模式:', searchMode);
    console.log('搜尋數量:', topK);

    setIsLoading(true)
    setError(null)

    // 更新搜尋歷史（最多保留 10 筆，並避免重複）
    setSearchHistory(prev => {
      const newHistory = [query, ...prev.filter(h => h !== query)].slice(0, 10)
      localStorage.setItem('searchHistory', JSON.stringify(newHistory))
      return newHistory
    })

    try {
      const payload = {
        query: query,
        mode: searchMode,  // 修正：使用 'mode' 而不是 'search_mode'
        top_k: Number(topK),
        use_gpt: Boolean(useGPT),
        index_pattern: 'erp-*',
        temperature: 0.7,
        convert_to_traditional: true
      }

      console.log('發送請求 payload:', payload);

      const response = await fetch(`${API_BASE_URL}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      })

      if (!response.ok) {
        // 把錯誤細節印出來
        let msg = `搜尋失敗: ${response.status}`
        try {
          const err = await response.json()
          if (err?.detail) msg += `\n${JSON.stringify(err.detail, null, 2)}`
        } catch (_) { }
        throw new Error(msg)
      }

      const data = await response.json()
      console.log('搜尋結果:', data);
      console.log('結果數量:', data.sources?.length);
      console.log('總命中數:', data.total_hits);
      setSearchResults(data)

    } catch (err) {
      console.error('Search error:', err)
      setError(err?.message || '搜尋時發生錯誤')
      setSearchResults(null)
    } finally {
      setIsLoading(false)
    }
  }

  // 初始化時載入搜尋歷史紀錄
  useEffect(() => {
    const saved = localStorage.getItem('searchHistory')
    if (saved) {
      try {
        setSearchHistory(JSON.parse(saved))
      } catch (e) {
        console.error('Failed to load search history:', e)
      }
    }
  }, [])

  // 清除目前的搜尋字串與結果
  const handleClear = () => {
    setSearchQuery('')
    setSearchResults(null)
    setError(null)
  }

  // ────────────────────────── Render ──────────────────────────
  return (
    <div className="app-container">
      {/* 頁面標題 */}
      <Header title="Fushin AI 智能檢索系統" />

      {/* 系統健康狀態指示器 */}
      <StatusIndicator status={systemStatus} />

      <main className="main-content">
        <div className="search-section">
          {/* 搜尋欄位 */}
          <SearchBar
            value={searchQuery}
            onChange={setSearchQuery}
            onSearch={handleSearch}
            onClear={handleClear}
            isLoading={isLoading}
            placeholder="輸入關鍵字進行智能搜尋..."
            searchHistory={searchHistory}
            onHistorySelect={(q) => {
              setSearchQuery(q)
              handleSearch(q)
            }}
          />

          {/* 搜尋選項 */}
          <div className="search-options">
            <SearchModeSelector
              mode={searchMode}
              onChange={setSearchMode}
              disabled={isLoading}
            />

            <div className="option-group">
              {/* GPT 功能選項 */}
              <label className="checkbox-label">
                <input
                  type="checkbox"
                  checked={useGPT}
                  onChange={(e) => setUseGPT(e.target.checked)}
                  disabled={isLoading}
                />
                <span>使用 AI 生成答案</span>
              </label>

              {/* 控制回傳結果數量 */}
              <div className="top-k-selector">
                <label>結果數量：</label>
                <select
                  value={topK}
                  onChange={(e) => setTopK(Number(e.target.value))}
                  disabled={isLoading}
                >
                  <option value={3}>3</option>
                  <option value={5}>5</option>
                  <option value={10}>10</option>
                  <option value={20}>20</option>
                </select>
              </div>
            </div>
          </div>
        </div>

        {/* 錯誤訊息 */}
        {error && (
          <div className="error-message">
            <span>⚠️ {error}</span>
          </div>
        )}

        {/* 載入中提示 */}
        {isLoading && <LoadingSpinner message="搜尋中..." />}

        {/* 搜尋結果列表 */}
        {searchResults && !isLoading && (
          <SearchResults
            results={searchResults}
            searchMode={searchResults?.search_mode || searchMode}
            useGPT={useGPT}
          />
        )}
        
        {/* 結果統計資訊 */}
        {searchResults && !isLoading && (
          <div className="search-stats" style={{ marginTop: '1rem', padding: '0.5rem', backgroundColor: '#f0f0f0', borderRadius: '4px' }}>
            <small>
              搜尋模式: {searchResults.search_mode} | 
              返回結果: {searchResults.sources?.length || 0} 筆 | 
              總命中數: {searchResults.total_hits || 0} 筆 | 
              處理時間: {searchResults.processing_time_ms || 0} ms
            </small>
          </div>
        )}
      </main>
    </div>
  )
}
