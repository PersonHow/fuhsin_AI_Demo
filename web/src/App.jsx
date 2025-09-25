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
  const [searchStats, setSearchStats] = useState(null)             // 搜尋統計資訊
  const [lastSearchTime, setLastSearchTime] = useState(null)       // 最後搜尋時間

  // ─────────────────────── 系統健康檢查 ───────────────────────
  /**
   * 將 /health API 回傳結果正規化，確保舊版或新版後端格式都能處理。
   */
  const normalizeHealth = (data) => {
    const hasES = typeof data?.elasticsearch === 'boolean'
    const hasOpenAI = typeof data?.openai === 'boolean'
    const status = data?.status || 'ok'
    
    return {
      elasticsearch: hasES ? data.elasticsearch : status === 'healthy',
      openai: hasOpenAI ? data.openai : status === 'healthy',
      status: status === 'healthy' ? 'ok' : (status === 'degraded' ? 'warning' : 'error')
    }
  }

  // 呼叫後端 /health 取得系統狀態
  const checkSystemHealth = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/health`, {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' }
      })
      
      if (response.ok) {
        const data = await response.json()
        setSystemStatus(normalizeHealth(data))
        console.log('系統健康狀態:', data)
      } else {
        console.warn('健康檢查失敗:', response.status, response.statusText)
        setSystemStatus({ elasticsearch: false, openai: false, status: 'error' })
      }
    } catch (err) {
      console.error('健康檢查錯誤:', err)
      setSystemStatus({ elasticsearch: false, openai: false, status: 'error' })
    }
  }, [])

  // 取得系統統計資訊
  const getSystemStats = useCallback(async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/stats`)
      if (response.ok) {
        const stats = await response.json()
        setSearchStats(stats)
        console.log('系統統計資訊:', stats)
      }
    } catch (err) {
      console.error('獲取統計資訊失敗:', err)
    }
  }, [])

  // 初始化時執行一次健康檢查，之後每 30 秒重複檢查一次
  useEffect(() => {
    checkSystemHealth()
    getSystemStats()
    
    const healthInterval = setInterval(checkSystemHealth, 30000)
    const statsInterval = setInterval(getSystemStats, 60000)
    
    return () => {
      clearInterval(healthInterval)
      clearInterval(statsInterval)
    }
  }, [checkSystemHealth, getSystemStats])

  // ──────────────────────── 搜尋處理 ─────────────────────────
  /**
   * 執行搜尋請求 - 改進版本，包含更詳細的錯誤處理和日誌
   */
  const handleSearch = async (query = searchQuery) => {
    if (!query.trim()) {
      setError('請輸入搜尋內容')
      return
    }
    
    console.log('=== 開始搜尋 ===')
    console.log('搜尋查詢:', query)
    console.log('搜尋模式:', searchMode)
    console.log('結果數量:', topK)
    console.log('使用 GPT:', useGPT)

    setIsLoading(true)
    setError(null)
    setSearchResults(null)

    // 更新搜尋歷史（最多保留 10 筆，並避免重複）
    setSearchHistory(prev => {
      const newHistory = [query, ...prev.filter(h => h !== query)].slice(0, 10)
      localStorage.setItem('searchHistory', JSON.stringify(newHistory))
      return newHistory
    })

    // 記錄搜尋開始時間
    const searchStartTime = Date.now()
    setLastSearchTime(new Date().toLocaleString())

    try {
      // 構建請求 payload - 確保欄位名稱與後端 API 一致
      const payload = {
        query: query.trim(),
        mode: searchMode,  // 使用 'mode' 參數
        top_k: Number(topK),
        use_gpt: Boolean(useGPT),
        index_pattern: 'erp-*',
        temperature: 0.7,
        convert_to_traditional: true
      }

      console.log('發送請求 payload:', JSON.stringify(payload, null, 2))

      const response = await fetch(`${API_BASE_URL}/query`, {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify(payload)
      })

      // 詳細的響應處理
      console.log('響應狀態:', response.status, response.statusText)
      console.log('響應頭:', Object.fromEntries(response.headers.entries()))

      if (!response.ok) {
        let errorMsg = `搜尋失敗 (${response.status}): ${response.statusText}`
        
        try {
          const errorData = await response.json()
          console.error('錯誤詳情:', errorData)
          
          if (errorData?.detail) {
            if (typeof errorData.detail === 'string') {
              errorMsg = errorData.detail
            } else {
              errorMsg += `\n${JSON.stringify(errorData.detail, null, 2)}`
            }
          }
        } catch (parseError) {
          console.error('解析錯誤響應失敗:', parseError)
          // 嘗試獲取原始文本
          try {
            const errorText = await response.text()
            console.error('錯誤響應原始內容:', errorText)
            if (errorText) {
              errorMsg += `\n${errorText}`
            }
          } catch (textError) {
            console.error('獲取錯誤文本失敗:', textError)
          }
        }
        
        throw new Error(errorMsg)
      }

      const data = await response.json()
      
      // 詳細的響應日誌
      console.log('=== 搜尋成功 ===')
      console.log('完整響應數據:', JSON.stringify(data, null, 2))
      console.log('處理後的查詢:', data.processed_query)
      console.log('搜尋模式:', data.search_mode)
      console.log('結果數量:', data.sources?.length)
      console.log('總命中數:', data.total_hits)
      console.log('處理時間:', data.processing_time_ms, 'ms')
      
      if (data.answer) {
        console.log('GPT 答案長度:', data.answer.length, '字符')
      }
      
      if (data.sources) {
        data.sources.forEach((source, index) => {
          console.log(`結果 ${index + 1}:`, {
            score: source.score,
            index: source.index,
            type: source.metadata?.type,
            content_length: source.content?.length
          })
        })
      }

      // 計算客戶端總時間
      const totalTime = Date.now() - searchStartTime
      console.log('客戶端總時間:', totalTime, 'ms')

      setSearchResults(data)

    } catch (err) {
      console.error('=== 搜尋錯誤 ===')
      console.error('錯誤類型:', err.constructor.name)
      console.error('錯誤訊息:', err.message)
      console.error('錯誤堆疊:', err.stack)
      
      // 更友善的錯誤訊息
      let userFriendlyError = '搜尋時發生錯誤'
      
      if (err.message.includes('Failed to fetch') || err.message.includes('NetworkError')) {
        userFriendlyError = '無法連接到搜尋服務，請檢查網路連接'
      } else if (err.message.includes('timeout')) {
        userFriendlyError = '搜尋請求超時，請稍後再試'
      } else if (err.message.includes('503')) {
        userFriendlyError = '搜尋服務暫時不可用，請稍後再試'
      } else if (err.message.includes('500')) {
        userFriendlyError = '服務器內部錯誤，請稍後再試'
      } else if (err.message) {
        userFriendlyError = err.message
      }
      
      setError(userFriendlyError)
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
        console.error('載入搜尋歷史失敗:', e)
      }
    }
  }, [])

  // 清除目前的搜尋字串與結果
  const handleClear = () => {
    setSearchQuery('')
    setSearchResults(null)
    setError(null)
  }

  // 重置錯誤狀態
  const handleErrorReset = () => {
    setError(null)
  }

  // 快速搜尋建議
  const quickSearchSuggestions = [
    'P026', 'P001', 'P002',  // 產品代碼範例
    '交流伺服馬達', '動力設備', '安川電機',  // 產品相關
    '客訴', '退貨', '品質問題',  // 客訴相關
    '技術文件', '申請', '核准'   // 文件相關
  ]

  // ────────────────────────── Render ──────────────────────────
  return (
    <div className="app-container">
      {/* 頁面標題 */}
      <Header title="Fushin AI 智能檢索系統" />

      {/* 系統健康狀態指示器 */}
      <StatusIndicator 
        status={systemStatus} 
        stats={searchStats}
        lastUpdate={lastSearchTime}
      />

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

          {/* 快速搜尋建議 */}
          {!searchQuery && !searchResults && (
            <div className="quick-search-suggestions">
              <label>快速搜尋建議：</label>
              <div className="suggestion-buttons">
                {quickSearchSuggestions.map(suggestion => (
                  <button
                    key={suggestion}
                    onClick={() => {
                      setSearchQuery(suggestion)
                      handleSearch(suggestion)
                    }}
                    className="suggestion-button"
                    disabled={isLoading}
                  >
                    {suggestion}
                  </button>
                ))}
              </div>
            </div>
          )}

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
                  <option value={15}>15</option>
                  <option value={20}>20</option>
                </select>
              </div>
            </div>
          </div>
        </div>

        {/* 錯誤訊息 */}
        {error && (
          <div className="error-message">
            <div className="error-content">
              <div className="error-main">
                <span className="error-icon">⚠️</span>
                <span className="error-text">{error}</span>
              </div>
              <button 
                className="error-close-btn"
                onClick={handleErrorReset}
                title="關閉錯誤訊息"
              >
                ✕
              </button>
            </div>
            {/* 錯誤時的調試資訊 */}
            {process.env.NODE_ENV === 'development' && (
              <div className="debug-info">
                <details>
                  <summary>調試資訊</summary>
                  <pre>
                    <code>
                      {JSON.stringify({
                        timestamp: new Date().toISOString(),
                        searchQuery,
                        searchMode,
                        topK,
                        useGPT,
                        systemStatus,
                        apiUrl: API_BASE_URL
                      }, null, 2)}
                    </code>
                  </pre>
                </details>
              </div>
            )}
          </div>
        )}

        {/* 載入中提示 */}
        {isLoading && (
          <LoadingSpinner 
            message={`正在執行${searchMode === 'hybrid' ? '混合' : searchMode === 'vector' ? '語義' : '關鍵字'}搜尋...`} 
          />
        )}

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
          <div className="search-stats">
            <div className="stats-row">
              <div className="stats-item">
                <strong>搜尋模式:</strong> {searchResults.search_mode}
              </div>
              <div className="stats-item">
                <strong>返回結果:</strong> {searchResults.sources?.length || 0} 筆
              </div>
              <div className="stats-item">
                <strong>總命中數:</strong> {searchResults.total_hits || 0} 筆
              </div>
              <div className="stats-item">
                <strong>處理時間:</strong> {searchResults.processing_time_ms || 0} ms
              </div>
            </div>
            
            {/* 查詢處理資訊 */}
            {searchResults.processed_query !== searchResults.query && (
              <div className="query-processing-info">
                <small>
                  <strong>原始查詢:</strong> {searchResults.query} → 
                  <strong>處理後:</strong> {searchResults.processed_query}
                </small>
              </div>
            )}
          </div>
        )}

        {/* 無結果提示 */}
        {searchResults && !isLoading && searchResults.sources?.length === 0 && (
          <div className="no-results">
            <div className="no-results-content">
              <h3>🔍 未找到相關結果</h3>
              <p>嘗試以下建議：</p>
              <ul>
                <li>檢查搜尋關鍵字是否正確</li>
                <li>嘗試更簡短或更具體的關鍵字</li>
                <li>使用不同的搜尋模式（關鍵字/語義/混合）</li>
                <li>檢查是否有相關的產品代碼或文件編號</li>
                <li>嘗試使用上方的快速搜尋建議</li>
              </ul>
            </div>
          </div>
        )}

        {/* 系統狀態面板（開發模式） */}
        {process.env.NODE_ENV === 'development' && (
          <div className="debug-panel">
            <details>
              <summary>系統調試資訊</summary>
              <div className="debug-content">
                <h4>系統狀態</h4>
                <pre>{JSON.stringify(systemStatus, null, 2)}</pre>
                
                <h4>統計資訊</h4>
                <pre>{JSON.stringify(searchStats, null, 2)}</pre>
                
                <h4>搜尋配置</h4>
                <pre>{JSON.stringify({
                  searchMode,
                  topK,
                  useGPT,
                  apiUrl: API_BASE_URL
                }, null, 2)}</pre>
              </div>
            </details>
          </div>
        )}
      </main>
    </div>
  )
}
