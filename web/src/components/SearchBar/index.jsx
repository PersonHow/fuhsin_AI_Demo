import { useState, useRef, useEffect } from 'react'
import './SearchBar.scss'

export default function SearchBar({
    value,
    onChange,
    onSearch,
    onClear,
    isLoading,
    placeholder,
    searchHistory = [],
    onHistorySelect
}) {
    const [showHistory, setShowHistory] = useState(false)
    const [isFocused, setIsFocused] = useState(false)
    const inputRef = useRef(null)

    // 處理 Enter 鍵搜尋
    const handleKeyPress = (e) => {
        if (e.key === 'Enter' && !isLoading) {
            onSearch()
            setShowHistory(false)
        }
    }

    // 處理歷史記錄選擇
    const handleHistoryClick = (query) => {
        onHistorySelect(query)
        setShowHistory(false)
    }

    // 點擊外部關閉歷史記錄
    useEffect(() => {
        const handleClickOutside = (e) => {
            if (!e.target.closest('.search-bar-container')) {
                setShowHistory(false)
            }
        }
        document.addEventListener('click', handleClickOutside)
        return () => document.removeEventListener('click', handleClickOutside)
    }, [])

    return (
        <div className="search-bar-container">
            <div className={`search-bar ${isFocused ? 'focused' : ''} ${isLoading ? 'loading' : ''}`}>
                <div className="search-icon">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none">
                        <path d="M21 21L15 15M17 10C17 13.866 13.866 17 10 17C6.13401 17 3 13.866 3 10C3 6.13401 6.13401 3 10 3C13.866 3 17 6.13401 17 10Z"
                            stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                    </svg>
                </div>

                <input
                    ref={inputRef}
                    type="text"
                    value={value}
                    onChange={(e) => onChange(e.target.value)}
                    onKeyPress={handleKeyPress}
                    onFocus={() => {
                        setIsFocused(true)
                        setShowHistory(true)
                    }}
                    onBlur={() => setIsFocused(false)}
                    placeholder={placeholder}
                    disabled={isLoading}
                    className="search-input"
                />

                {value && (
                    <button
                        className="clear-button"
                        onClick={onClear}
                        disabled={isLoading}
                        aria-label="清除"
                    >
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none">
                            <path d="M18 6L6 18M6 6L18 18" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                        </svg>
                    </button>
                )}

                <button
                    className="search-button"
                    onClick={() => onSearch()}
                    disabled={isLoading || !value}
                    aria-label="搜尋"
                >
                    {isLoading ? '搜尋中...' : '搜尋'}
                </button>
            </div>

            {showHistory && searchHistory.length > 0 && !value && (
                <div className="search-history">
                    <div className="history-header">最近搜尋</div>
                    {searchHistory.map((item, index) => (
                        <div
                            key={index}
                            className="history-item"
                            onClick={() => handleHistoryClick(item)}
                        >
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none">
                                <path d="M12 6V12L16 14M21 12C21 16.9706 16.9706 21 12 21C7.02944 21 3 16.9706 3 12C3 7.02944 7.02944 3 12 3C16.9706 3 21 7.02944 21 12Z"
                                    stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                            </svg>
                            <span>{item}</span>
                        </div>
                    ))}
                </div>
            )}
        </div>
    )
}


