import { useState } from 'react'
import { Converter } from 'opencc-js'
import './SearchResults.scss'
import { RiColorFilterAiFill } from "react-icons/ri"
import { 
    FiFileText, 
    FiAlertCircle, 
    FiTool, 
    FiFilePlus,
    FiDownload
} from "react-icons/fi"

export default function SearchResults({ results, searchMode, useGPT }) {
    const [expandedItems, setExpandedItems] = useState(new Set())
    const cn2tw = Converter({ from: 'cn', to: 'tw' })
    
    // 清理文本中的頁碼標記和其他無用標記
    const cleanText = (text) => {
        if (!text || typeof text !== 'string') return text
        
        return text
            // 移除 [第 X 頁] 標記
            .replace(/\[第\s*\d+\s*頁\]/g, '')
            .replace(/【第\s*\d+\s*頁】/g, '')
            // 移除開頭的空白
            .trim()
    }
    
    const toTW = (text) => {
        if (typeof text !== 'string') return text
        // 先轉換繁體，再清理標記
        return cleanText(cn2tw(text))
    }

    // 索引類型對應的圖標和顏色
    const indexConfig = {
        'ECN_NOTICE': { label: 'ECN 通知', icon: FiFileText, color: '#3b82f6' },
        'ECN_APPLICATION': { label: 'ECN 申請', icon: FiFilePlus, color: '#8b5cf6' },
        'COMPLAINT': { label: '客訴記錄', icon: FiAlertCircle, color: '#ef4444' },
        'FMEA': { label: 'FMEA 分析', icon: FiTool, color: '#f59e0b' },
    }

    const toggleExpand = (index) => {
        const newExpanded = new Set(expandedItems)
        if (newExpanded.has(index)) {
            newExpanded.delete(index)
        } else {
            newExpanded.add(index)
        }
        setExpandedItems(newExpanded)
    }

    // 獲取索引配置
    const getIndexConfig = (source) => {
        const indexSource = source.doc_type|| 'unknown'
        return indexConfig[indexSource] || indexConfig['unknown']
    }

    // 生成文件顯示名稱
    const getDocumentDisplayName = (source) => {
        if (source.file_name) {
            return source.file_name.replace('.pdf', '')
        }
        if (source.title) {
            return source.title
        }
        if (source.doc_number && source.doc_type) {
            return `${source.doc_type} - ${source.doc_number}`
        }
        if (source.doc_number) {
            return `文件編號: ${source.doc_number}`
        }
        const config = getIndexConfig(source)
        
        return `${config.label} - ${source.doc_id?.substring(0, 8) || '未命名'}`
    }

    // 建立標題列資訊
    const buildHeaderInfo = (source) => {
        const config = getIndexConfig(source)
        const headerParts = []
        const displayName = getDocumentDisplayName(source)
        headerParts.push(displayName)

        if (source.doc_number && !displayName.includes(source.doc_number)) {
            headerParts.push(`編號: ${source.doc_number}`)
        }
        if (source.department) {
            headerParts.push(`部門: ${source.department}`)
        }
        if (source.doc_date) {
            headerParts.push(`日期: ${source.doc_date}`)
        }

        return { parts: headerParts, config }
    }

    // 取得關鍵描述（收起時顯示）
    const getKeyDescription = (source) => {
        // 優先使用後端提供的 searchable preview
        if (source.highlight?._searchable_preview?.[0]) {
            return source.highlight._searchable_preview[0]
        }
        
        // 其次使用其他 highlight 片段
        if (source.highlight) {
            for (const [key, values] of Object.entries(source.highlight)) {
                if (key !== '_searchable_preview' && 
                    key !== 'content_snippets' && 
                    Array.isArray(values) && 
                    values.length > 0) {
                    return values[0]
                }
            }
        }
        
        // 根據索引類型選擇合適的欄位
        if (source.index?.includes('complaint')) {
            return source.complaint_description || source.summary || ''
        } else if (source.index?.includes('ecn')) {
            return source.change_description || source.summary || ''
        } else if (source.index?.includes('fmea')) {
            return source.failure_mode || source.summary || ''
        }

        return source.summary || ''
    }

    // 取得詳細資訊
    const getDetailedInfo = (source) => {
        const details = {}

        details['基本資訊'] = []
        if (source.doc_type) {
            details['基本資訊'].push({ label: '文件類型', value: source.doc_type })
        }
        if (source.doc_date) {
            details['基本資訊'].push({ label: '文件日期', value: source.doc_date })
        }
        if (source.department) {
            details['基本資訊'].push({ label: '部門', value: source.department })
        }
        if (source.applicant) {
            details['基本資訊'].push({ label: '申請人', value: source.applicant })
        }

        if (source.product_codes && source.product_codes.length > 0) {
            details['產品資訊'] = [
                { label: '產品編號', value: source.product_codes.join(', ') }
            ]
        }

        if (source.keywords && source.keywords.length > 0) {
            details['關鍵字'] = [
                { label: '', value: source.keywords.join(', ') }
            ]
        }

        return details
    }

    // 截斷文字顯示
    const truncateText = (text, maxLength = 150) => {
        if (!text) return ''
        if (Array.isArray(text)) {
            text = text.join(', ')
        }
        const cleanText = text.replace(/<(?!em|\/em)[^>]*>/g, '')
        if (cleanText.length <= maxLength) return cleanText
        return cleanText.substring(0, maxLength) + '...'
    }

    // 渲染高亮文字
    const renderHighlightedText = (text) => {
        if (!text) return null
        return text.split(/(<em>.*?<\/em>)/g).map((part, index) => {
            if (part.startsWith('<em>') && part.endsWith('</em>')) {
                const content = part.slice(4, -5)
                return <mark key={index}>{content}</mark>
            }
            return part
        })
    }

    // 處理文件下載
    const handleDownload = (source) => {
        if (!source.file_url) {
            alert('檔案連結不可用')
            return
        }
        console.log('下載文件:', source.file_url)
        window.open(source.file_url, '_blank')
    }

    return (
        <div className="search-results">
            {/* 結果標題 */}
            <div className="results-header">
                <div className="results-info">
                    <h2>搜尋結果</h2>
                    <div className="results-meta">
                        <span className="result-count">找到 {results.total_hits} 筆資料</span>
                        <span className="search-mode">模式: {searchMode}</span>
                        <span className="process-time">耗時: {results.processing_time_ms}ms</span>
                    </div>
                </div>

                {/* 索引分布統計 */}
                {/* {results.metadata?.index_distribution && 
                 Object.keys(results.metadata.index_distribution).length > 0 && (
                    <div className="index-distribution">
                        <span className="distribution-label">來源分布:</span>
                        {Object.entries(results.metadata.index_distribution).map(([index, count]) => {
                            const config = indexConfig[index] || indexConfig['unknown']
                            const Icon = config.icon
                            return (
                                <span 
                                    key={index} 
                                    className="distribution-item"
                                    style={{ color: config.color }}
                                >
                                    <Icon size={14} />
                                    {config.label}: {count}
                                </span>
                            )
                        })}
                    </div>
                )} */}
            </div>

            {/* AI 答案區塊 */}
            {useGPT && results.answer && (
                <div className="ai-answer">
                    <div className="ai-answer-header">
                        <RiColorFilterAiFill size="30" />
                        <h3>AI 智能回答</h3>
                    </div>
                    <div className="ai-answer-content">
                        {toTW(results.answer).split('\n').map((line, index) => {
                            if (line.match(/^【.+】$/)) {
                                return <h4 key={index} className="answer-section-title">{line}</h4>
                            }
                            if (line.match(/^[-\d.]\s/)) {
                                return <li key={index} className="answer-list-item">{line}</li>
                            }
                            return line.trim() ? <p key={index}>{line}</p> : null
                        })}
                    </div>
                </div>
            )}

            {/* 相關文檔列表 */}
            <div className="results-list">
                <h3>相關文檔</h3>
                {results.sources.map((source, index) => {
                    const { parts: headerInfo, config } = buildHeaderInfo(source)
                    const keyDescription = getKeyDescription(source)
                    const detailedInfo = getDetailedInfo(source)
                    const isExpanded = expandedItems.has(index)
                    const Icon = config.icon

                    return (
                        <div 
                            key={index} 
                            className="result-item"
                            style={{ borderLeftColor: config.color }}
                        >
                            {/* 標題列 */}
                            <div className="result-header">
                                <div className="result-title-bar">
                                    <span className="result-number">#{index + 1}</span>
                                    
                                    <span 
                                        className="index-badge"
                                        style={{ 
                                            backgroundColor: config.color + '20',
                                            color: config.color 
                                        }}
                                    >
                                        <Icon size={14} />
                                        {config.label}
                                    </span>
                                    
                                    <span className="header-info">
                                        {headerInfo.map((info, idx) => (
                                            <span key={idx} className="header-item">
                                                {idx > 0 && <span className="separator"> | </span>}
                                                {toTW(info)}
                                            </span>
                                        ))}
                                    </span>
                                </div>
                                <div className="result-actions">
                                    <span className="score">相關度: {source.score.toFixed(3)}</span>
                                    
                                    {source.file_url && (
                                        <button
                                            className="download-button"
                                            onClick={() => handleDownload(source)}
                                            title="下載文件"
                                        >
                                            <FiDownload size={16} />
                                            下載
                                        </button>
                                    )}
                                    
                                    <button
                                        className="expand-button"
                                        onClick={() => toggleExpand(index)}
                                    >
                                        {isExpanded ? '收起' : '展開'}
                                    </button>
                                </div>
                            </div>

                            {/* 簡短預覽 - 關鍵描述 */}
                            {!isExpanded && keyDescription && (
                                <div className="result-preview">
                                    <span className="preview-label">關鍵資訊：</span>
                                    <span className="preview-text">
                                        {renderHighlightedText(toTW(truncateText(keyDescription, 200)))}
                                    </span>
                                </div>
                            )}

                            {/* 詳細內容 */}
                            {isExpanded && (
                                <div className="result-details-expanded">
                                    {/* 完整摘要 */}
                                    {source.summary && source.summary !== keyDescription && (
                                        <div className="detail-group">
                                            <h4 className="group-title">📄 摘要說明</h4>
                                            <div className="group-content">
                                                <div className="detail-item">
                                                    <span className="detail-value">
                                                        {renderHighlightedText(toTW(source.summary))}
                                                    </span>
                                                </div>
                                            </div>
                                        </div>
                                    )}

                                    {/* 基本資訊、產品資訊等 */}
                                    {Object.entries(detailedInfo).map(([groupName, items]) => (
                                        items.length > 0 && (
                                            <div key={groupName} className="detail-group">
                                                <h4 className="group-title">{groupName}</h4>
                                                <div className="group-content">
                                                    {items.map((item, idx) => (
                                                        <div key={idx} className="detail-item">
                                                            {item.label && (
                                                                <span className="detail-label">{item.label}：</span>
                                                            )}
                                                            <span className="detail-value">
                                                                {toTW(item.value)}
                                                            </span>
                                                        </div>
                                                    ))}
                                                </div>
                                            </div>
                                        )
                                    ))}

                                    {/* 相關內容片段 */}
                                    {source.highlight?.content_snippets && 
                                     source.highlight.content_snippets.length > 0 && (
                                        <div className="highlight-section">
                                            <h4 className="group-title">🔍 相關內容片段</h4>
                                            <div className="highlight-values">
                                                {source.highlight.content_snippets.map((snippet, idx) => (
                                                    <div key={idx} className="highlight-fragment">
                                                        <span className="fragment-label">片段 {idx + 1}:</span>
                                                        <p className="fragment-content">
                                                            {renderHighlightedText(toTW(snippet))}
                                                        </p>
                                                    </div>
                                                ))}
                                            </div>
                                        </div>
                                    )}

                                    {/* 其他高亮內容 */}
                                    {/* {source.highlight && Object.entries(source.highlight)
                                        .filter(([field]) => 
                                            field !== 'content_snippets' && 
                                            field !== '_searchable_preview'
                                        )
                                        .length > 0 && (
                                        <div className="highlight-section">
                                            <h4 className="group-title">📌 其他匹配內容</h4>
                                            {Object.entries(source.highlight)
                                                .filter(([field]) => 
                                                    field !== 'content_snippets' && 
                                                    field !== '_searchable_preview'
                                                )
                                                .map(([field, values]) => (
                                                    <div key={field} className="highlight-item">
                                                        <span className="highlight-field">{field}:</span>
                                                        <div className="highlight-values">
                                                            {values.map((value, vIdx) => (
                                                                <p key={vIdx} className="highlight-fragment">
                                                                    {renderHighlightedText(toTW(value))}
                                                                </p>
                                                            ))}
                                                        </div>
                                                    </div>
                                                ))}
                                        </div>
                                    )} */}
                                </div>
                            )}
                        </div>
                    )
                })}
            </div>

            {/* 搜尋元資料 */}
            {/* {results.metadata && (
                <div className="search-metadata">
                    {results.metadata.mysql_hits > 0 && (
                        <div className="metadata-item">
                            <span className="metadata-label">MySQL 輔助查詢:</span>
                            <span className="metadata-value">{results.metadata.mysql_hits} 筆</span>
                        </div>
                    )}
                    {results.metadata.product_ids_found?.length > 0 && (
                        <div className="metadata-item">
                            <span className="metadata-label">識別產品編號:</span>
                            <span className="metadata-value">
                                {results.metadata.product_ids_found.join(', ')}
                            </span>
                        </div>
                    )}
                    {results.metadata.keywords_used?.length > 0 && (
                        <div className="metadata-item">
                            <span className="metadata-label">使用關鍵字:</span>
                            <span className="metadata-value">
                                {results.metadata.keywords_used.join(', ')}
                            </span>
                        </div>
                    )}
                </div>
            )} */}
        </div>
    )
}
