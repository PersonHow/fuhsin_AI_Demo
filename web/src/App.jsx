import { useState, useEffect, useCallback } from 'react'
import './App.scss'
import { getHealth, getStats, postQuery, getDoc } from './api/apiClient'

// 元件
import Header from './components/Header'
import SearchBar from './components/SearchBar'
import SearchResults from './components/SearchResults'
import StatusIndicator from './components/StatusIndicator'
import SearchModeSelector from './components/SearchModeSelector'
import LoadingSpinner from './components/LoadingSpinner'

/**
 * 主應用程式組件 - 多索引 RAG 系統
 */
export default function App() {
  // ───────────────────────── 狀態管理 ─────────────────────────
  const [searchQuery, setSearchQuery] = useState('')               // 使用者輸入的查詢字串
  const [searchMode, setSearchMode] = useState('hybrid')           // 搜尋模式 (keyword | vector | hybrid)
  const [searchResults, setSearchResults] = useState(null)         // 搜尋結果
  const [isLoading, setIsLoading] = useState(false)                // 是否載入中
  const [error, setError] = useState(null)                         // 錯誤訊息
  const [systemStatus, setSystemStatus] = useState({               // 系統健康狀態
    elasticsearch: false,
    mysql: false,
    openai: false,
    status: 'checking',
    indices: []
  })
  const [searchHistory, setSearchHistory] = useState([])           // 搜尋歷史紀錄
  const [useGPT, setUseGPT] = useState(true)                       // 是否使用 GPT 生成答案
  const [topK, setTopK] = useState(10)                             // 回傳結果數量
  const [searchStats, setSearchStats] = useState(null)             // 搜尋統計資訊
  const [lastSearchTime, setLastSearchTime] = useState(null)       // 最後搜尋時間
  const [docTypeFilter, setDocTypeFilter] = useState([])          // 文件類型過濾

  // ─────────────────────── 系統健康檢查 ───────────────────────

  // 呼叫後端 /health 取得系統狀態
  const checkSystemHealth = useCallback(async () => {
    try {
      const status = await getHealth()
      setSystemStatus(status)
      console.log('系統健康檢查：', status)
      
      // 如果系統不健康，顯示警告
      if (status.status === 'error' || status.status === 'degraded') {
        console.warn('系統狀態異常：', status)
      }
    } catch (err) {
      console.error('健康檢查錯誤:', err)
      setSystemStatus({ 
        elasticsearch: false, 
        mysql: false, 
        openai: false, 
        status: 'error',
        indices: []
      })
    }
  }, [])

  // 取得系統統計資訊
  const getSystemStats = useCallback(async () => {
    try {
      const response = await getStats()
      setSearchStats(response)
      console.log('系統統計資訊：', response)
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
   * 執行搜尋請求 - 改進版本，支援多索引
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
    console.log('文件類型過濾:', docTypeFilter)

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
    setLastSearchTime(new Date().toLocaleString('zh-TW', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    }))

    try {
      // 構建請求 payload - 對應後端 SearchRequest
      const payload = {
        query: query.trim(),
        mode: searchMode,
        top_k: Number(topK),
        use_gpt: Boolean(useGPT),
        doc_type_filter: docTypeFilter.length > 0 ? docTypeFilter : null
      }

      console.log('發送請求 payload:', JSON.stringify(payload, null, 2))

      const result = await postQuery(payload)
      
      console.log("=== 搜尋成功 ===")
      console.log("回傳結果：", result)
      
      // 驗證結果
      if (!result.success) {
        throw new Error('搜尋請求未成功')
      }
      
      setSearchResults(result)
      
      // 顯示搜尋結果摘要
      const resultSummary = {
        總結果數: result.total_hits,
        返回數量: result.sources?.length || 0,
        處理時間: `${result.processing_time_ms}ms`,
        搜尋模式: result.search_mode,
        MySQL輔助: result.metadata?.mysql_hits || 0,
        識別產品: result.metadata?.product_ids_found || [],
        使用關鍵字: result.metadata?.keywords_used || [],
        搜尋索引: result.metadata?.indices_searched || '',
        索引分布: result.metadata?.index_distribution || {}
      }
      console.log('搜尋結果摘要:', resultSummary)

    } catch (err) {
      console.error('=== 搜尋錯誤 ===')
      console.error('錯誤類型:', err.constructor.name)
      console.error('錯誤訊息:', err.message)
      console.error('錯誤堆疊:', err.stack)
      
      // 更友善的錯誤訊息
      let userFriendlyError = '搜尋時發生錯誤'
      
      if (err.message.includes('Failed to fetch') || err.message.includes('NetworkError')) {
        userFriendlyError = '無法連接到搜尋服務，請檢查網路連接或確認服務是否運行'
      } else if (err.message.includes('timeout')) {
        userFriendlyError = '搜尋請求超時，請稍後再試或減少結果數量'
      } else if (err.message.includes('503')) {
        userFriendlyError = '搜尋服務暫時不可用，請稍後再試'
      } else if (err.message.includes('500')) {
        userFriendlyError = '服務器內部錯誤，請檢查後端日誌'
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

  // 處理文件類型過濾變更
  const handleDocTypeFilterChange = (types) => {
    setDocTypeFilter(types)
    console.log('文件類型過濾已更新:', types)
  }

  // ────────────────────────── Render ──────────────────────────
  return (
    <div className="app-container">
      {/* 頁面標題 */}
      <Header title="Fushin AI 智能檢索系統" subtitle="多索引技術文件搜尋" />

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
            placeholder="輸入關鍵字、產品編號或問題描述進行智能搜尋..."
            searchHistory={searchHistory}
            onHistorySelect={(q) => {
              setSearchQuery(q)
              handleSearch(q)
            }}
          />

          {/* 搜尋選項 */}
          <div className="search-options">
            {/* 搜尋模式選擇 */}
            <SearchModeSelector
              mode={searchMode}
              onChange={setSearchMode}
              disabled={isLoading}
            />

            <div className="option-group">

              {/* 控制回傳結果數量 */}
              <div className="top-k-selector">
                <label>結果數量：</label>
                <select
                  value={topK}
                  onChange={(e) => setTopK(Number(e.target.value))}
                  disabled={isLoading}
                >
                  <option value={5}>5</option>
                  <option value={10}>10</option>
                  <option value={15}>15</option>
                  <option value={20}>20</option>
                  <option value={30}>30</option>
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
          </div>
        )}

        {/* 載入中提示 */}
        {isLoading && (
          <LoadingSpinner 
            message={`正在執行${
              searchMode === 'hybrid' ? '混合' : 
              searchMode === 'vector' ? '語義' : 
              '關鍵字'
            }搜尋...`} 
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
        
        {/* 結果統計資訊 - 使用後端的格式 */}
        {searchResults && !isLoading && (
          <div className="search-stats">
            {/* <div className="stats-row">
              <div className="stats-item">
                <strong>搜尋模式:</strong> 
                <span className="stats-value">{searchResults.search_mode}</span>
              </div>
              <div className="stats-item">
                <strong>返回結果:</strong> 
                <span className="stats-value">{searchResults.sources?.length || 0} 筆</span>
              </div>
              <div className="stats-item">
                <strong>總命中數:</strong> 
                <span className="stats-value">{searchResults.total_hits || 0} 筆</span>
              </div>
              <div className="stats-item">
                <strong>處理時間:</strong> 
                <span className="stats-value">{searchResults.processing_time_ms || 0} ms</span>
              </div>
            </div> */}
            
            {/* MySQL 輔助查詢資訊 */}
            {/* {searchResults.metadata?.mysql_hits > 0 && (
              <div className="stats-row">
                <div className="stats-item">
                  <strong>MySQL 輔助:</strong> 
                  <span className="stats-value">{searchResults.metadata.mysql_hits} 筆</span>
                </div>
                {searchResults.metadata.product_ids_found?.length > 0 && (
                  <div className="stats-item">
                    <strong>識別產品:</strong> 
                    <span className="stats-value">
                      {searchResults.metadata.product_ids_found.join(', ')}
                    </span>
                  </div>
                )}
                {searchResults.metadata.keywords_used?.length > 0 && (
                  <div className="stats-item">
                    <strong>使用關鍵字:</strong> 
                    <span className="stats-value">
                      {searchResults.metadata.keywords_used.join(', ')}
                    </span>
                  </div>
                )}
              </div>
            )} */}
            
            {/* 索引分布資訊 */}
            {/* {searchResults.metadata?.index_distribution && 
             Object.keys(searchResults.metadata.index_distribution).length > 0 && (
              <div className="stats-row">
                <div className="stats-item">
                  <strong>索引分布:</strong>
                  <div className="index-distribution">
                    {Object.entries(searchResults.metadata.index_distribution).map(([index, count]) => (
                      <span key={index} className="distribution-tag">
                        {index.replace('erp-', '')}: {count}
                      </span>
                    ))}
                  </div>
                </div>
              </div>
            )} */}

            {/* 搜尋索引資訊 */}
            {/* {searchResults.metadata?.indices_searched && (
              <div className="stats-row">
                <div className="stats-item">
                  <strong>搜尋範圍:</strong>
                  <span className="stats-value">
                    {searchResults.metadata.indices_searched}
                  </span>
                </div>
              </div>
            )} */}
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
                <li>嘗試移除文件類型過濾</li>
              </ul>
              
              {searchHistory.length > 0 && (
                <div className="search-suggestions">
                  <p><strong>或試試最近的搜尋：</strong></p>
                  <div className="suggestion-list">
                    {searchHistory.slice(0, 5).map((item, idx) => (
                      <button
                        key={idx}
                        className="suggestion-item"
                        onClick={() => {
                          setSearchQuery(item)
                          handleSearch(item)
                        }}
                      >
                        {item}
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        )}
      </main>
    </div>
  )
}
