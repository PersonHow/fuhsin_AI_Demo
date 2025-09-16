import { useState } from 'react'
import { Converter } from 'opencc-js'
import './SearchResults.scss'
import { RiColorFilterAiFill } from "react-icons/ri";

export default function SearchResults({ results, searchMode, useGPT }) {
    const [expandedItems, setExpandedItems] = useState(new Set())
    const cn2tw = Converter({ from: 'cn', to: 'tw' });
    const toTW = (text) => (typeof text === 'string' ? cn2tw(text) : text)
    
    // 欄位名稱對應表
    const fieldNameMapping = {
        // 產品主檔欄位
        'product_id': '產品編號',
        'product_name': '產品名稱',
        'product_model': '產品型號',
        'category': '類別',
        'price': '價格',
        'stock_qty': '庫存數量',
        'manufacture_date': '製造日期',
        'supplier': '供應商',
        'status': '狀態',

        // 倉庫欄位
        'warehouse_location': '倉庫位置',
        'quantity': '數量',
        'last_inventory_date': '最後盤點日期',
        'manager': '管理人',
        'special_notes': '特別備註',
        'min_stock_level': '最低庫存水平',

        // 客訴欄位
        'complaint_id': '客訴編號',
        'complaint_date': '客訴日期',
        'customer_name': '客戶姓名',
        'customer_company': '客戶公司',
        'complaint_type': '客訴類型',
        'severity': '嚴重程度',
        'description': '問題描述',
        'handler': '處理人員',
        'resolution_date': '解決日期',
        'resolution_status': '處理狀態',
        'follow_up_person': '跟進人員',
        'follow_up_date': '跟進日期',

        // 通用欄位
        'searchable_content': '內容',
        'all_content': '全文內容'
    }

    // 資料表名稱對應
    const tableNameMapping = {
        'erp-products': '產品主檔',
        'erp-warehouse': '倉庫資料',
        'erp-complaints': '客訴記錄'
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

    // 取得資料表顯示名稱
    const getTableDisplayName = (index) => {
        return tableNameMapping[index] || index
    }

    // 根據不同表格類型建立標題列資訊
    const buildHeaderInfo = (source, index) => {
        const data = source.metadata || {}
        const tableName = getTableDisplayName(source.index)
        const headerParts = [`來源：${tableName}`]
        
        if (source.index === 'erp-complaints') {
            // 客訴記錄
            if (data.complaint_id) headerParts.push(`編號：${data.complaint_id}`)
            if (data.complaint_date) headerParts.push(`發生日期：${data.complaint_date}`)
            if (data.severity) headerParts.push(`緊急性：${data.severity}`)
            if (data.complaint_type) headerParts.push(`類型：${data.complaint_type}`)
            if (data.status) headerParts.push(`狀態：${data.status}`)
            if (data.handler) headerParts.push(`接洽人：${data.handler}`)
            
            
        } else if (source.index === 'erp-warehouse') {
            // 倉庫資料
            if (data.product_id) headerParts.push(`產品編號：${data.product_id}`)
            if (data.product_name) headerParts.push(`產品：${data.product_name}`)
            if (data.warehouse_location) headerParts.push(`位置：${data.warehouse_location}`)
            if (data.quantity) headerParts.push(`數量：${data.quantity}`)
            if (data.manager) headerParts.push(`管理人：${data.manager}`)
            if (data.last_inventory_date) headerParts.push(`盤點日期：${data.last_inventory_date}`)
            
        } else if (source.index === 'erp-products') {
            // 產品主檔
            if (data.product_id) headerParts.push(`編號：${data.product_id}`)
            if (data.product_name) headerParts.push(`名稱：${data.product_name}`)
            if (data.product_model) headerParts.push(`型號：${data.product_model}`)
            if (data.category) headerParts.push(`類別：${data.category}`)
            if (data.status) headerParts.push(`狀態：${data.status}`)
            if (data.supplier) headerParts.push(`供應商：${data.supplier}`)
        }
        
        return headerParts
    }

    // 取得主要描述內容
    const getMainDescription = (source) => {
        const data = source._source || {}
        
        if (source.index === 'erp-complaints') {
            return data.description || data.all_content || ''
        } else if (source.index === 'erp-warehouse') {
            return data.special_notes || data.all_content || ''
        } else if (source.index === 'erp-products') {
            return data.all_content || ''
        }
        
        return data.all_content || ''
    }

    // 取得詳細資訊（展開時顯示）
    const getDetailedInfo = (source) => {
        const data = source.metadata || {}
        const details = {}
        
        if (source.index === 'erp-complaints') {
            // 客訴詳細資訊
            if (data.customer_name || data.customer_company) {
                details['客戶資訊'] = []
                if (data.customer_name) details['客戶資訊'].push({ label: '客戶姓名', value: data.customer_name })
                if (data.customer_company) details['客戶資訊'].push({ label: '客戶公司', value: data.customer_company })
            }
            
            if (data.description) {
                details['問題描述'] = [{ label: '', value: data.description }]
            }
            
            if (data.resolution_date || data.resolution_status) {
                details['處理進度'] = []
                if (data.resolution_status) details['處理進度'].push({ label: '處理狀態', value: data.resolution_status })
                if (data.resolution_date) details['處理進度'].push({ label: '解決日期', value: data.resolution_date })
            }
            
        } else if (source.index === 'erp-warehouse') {
            // 倉庫詳細資訊
            details['庫存資訊'] = []
            if (data.quantity) details['庫存資訊'].push({ label: '當前數量', value: data.quantity })
            if (data.min_stock_level) details['庫存資訊'].push({ label: '最低庫存', value: data.min_stock_level })
            if (data.special_notes) details['庫存資訊'].push({ label: '特別備註', value: data.special_notes })
            
        } else if (source.index === 'erp-products') {
            // 產品詳細資訊
            details['產品規格'] = []
            if (data.product_model) details['產品規格'].push({ label: '型號', value: data.product_model })
            if (data.price) details['產品規格'].push({ label: '價格', value: `$${data.price}` })
            if (data.stock_qty) details['產品規格'].push({ label: '庫存', value: data.stock_qty })
            if (data.manufacture_date) details['產品規格'].push({ label: '製造日期', value: data.manufacture_date })
        }
        
        return details
    }

    // 截斷文字顯示
    const truncateText = (text, maxLength = 10) => {
        if (!text) return ''
        const cleanText = text.replace(/<[^>]*>/g, '') // 移除 HTML 標籤
        if (cleanText.length <= maxLength) return cleanText
        return cleanText.substring(0, maxLength) + '...'
    }

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
                    const headerInfo = buildHeaderInfo(source, index)
                    const mainDescription = getMainDescription(source)
                    const detailedInfo = getDetailedInfo(source)
                    const isExpanded = expandedItems.has(index)
                    
                    return (
                        <div key={index} className="result-item">
                            {/* 標題列 - 永遠顯示 */}
                            <div className="result-header">
                                <div className="result-title-bar">
                                    <span className="result-number">#{index + 1}</span>
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
                                    <button
                                        className="expand-button"
                                        onClick={() => toggleExpand(index)}
                                    >
                                        {isExpanded ? '收起' : '展開'}
                                    </button>
                                </div>
                            </div>

                            {/* 簡短預覽 - 收起時顯示 */}
                            {!isExpanded && mainDescription && (
                                <div className="result-preview">
                                    <span className="preview-label">描述：</span>
                                    <span className="preview-text">
                                        {toTW(truncateText(mainDescription, 30))}
                                    </span>
                                </div>
                            )}

                            {/* 詳細內容 - 展開時顯示 */}
                            {isExpanded && (
                                <div className="result-details-expanded">
                                    {/* 分組顯示詳細資訊 */}
                                    {Object.entries(detailedInfo).map(([groupName, items]) => (
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
                                    ))}

                                    {/* 高亮內容（如果有） */}
                                    {source.highlights && Object.keys(source.highlights).length > 0 && (
                                        <div className="highlight-section">
                                            <h4>相關片段</h4>
                                            {Object.entries(source.highlights).map(([field, values]) => (
                                                <div key={field} className="highlight-item">
                                                    <strong>{fieldNameMapping[field] || field}:</strong>
                                                    {values.map((value, vIdx) => (
                                                        <p key={vIdx}>{toTW(renderHighlightedText(value))}</p>
                                                    ))}
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            )}
                        </div>
                    )
                })}
            </div>
        </div>
    )
}
