#!/bin/bash
# opencc-converter/process_ik_dicts.sh
# IK 詞典批次轉換腳本

set -e

# 顏色定義
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 日誌函數
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 顯示標題
echo -e "${BLUE}=====================================${NC}"
echo -e "${BLUE}    IK 詞典 OpenCC 繁體轉換工具    ${NC}"
echo -e "${BLUE}=====================================${NC}"

# 檢查命令參數
COMMAND=${1:-convert}

case $COMMAND in
    convert)
        log_info "開始執行詞典轉換..."
        
        # 檢查原始詞典是否存在
        if [ ! -d "/dictionaries/original" ]; then
            log_error "原始詞典目錄不存在！"
            exit 1
        fi
        
        # 列出原始詞典文件
        log_info "檢查原始詞典文件："
        ls -la /dictionaries/original/*.dic 2>/dev/null || {
            log_warn "沒有找到詞典文件！"
        }
        
        # 執行轉換
        python3 /app/convert_dict.py
        
        # 複製自定義詞典
        log_info "複製自定義詞典..."
        if [ -d "/dictionaries/custom" ]; then
            cp -f /dictionaries/custom/*.dic /dictionaries/converted/ 2>/dev/null || true
        fi
        
        # 創建初始化標記
        touch /dictionaries/converted/.initialized
        
        log_info "詞典轉換完成！"
        ;;
        
    check)
        log_info "檢查轉換結果..."
        
        if [ -d "/dictionaries/converted" ]; then
            echo -e "\n轉換後的詞典文件："
            ls -la /dictionaries/converted/*.dic
            
            echo -e "\n詞典統計："
            for file in /dictionaries/converted/*.dic; do
                if [ -f "$file" ]; then
                    count=$(wc -l < "$file")
                    printf "%-30s: %6d 個詞彙\n" "$(basename $file)" "$count"
                fi
            done
        else
            log_error "轉換後的詞典目錄不存在！"
        fi
        ;;
        
    test)
        log_info "測試詞典轉換效果..."
        
        # 測試幾個常見詞彙的轉換
        test_words=(
            "软件"
            "硬件"
            "网络"
            "程序"
            "服务器"
            "数据库"
            "内存"
            "缓存"
        )
        
        echo -e "\n測試詞彙轉換："
        for word in "${test_words[@]}"; do
            # 使用 Python 進行轉換測試
            result=$(python3 -c "
from opencc import OpenCC
cc = OpenCC('s2twp')
print(cc.convert('$word'))
")
            printf "%-10s => %s\n" "$word" "$result"
        done
        ;;
        
    clean)
        log_info "清理轉換結果..."
        rm -rf /dictionaries/converted/*
        log_info "清理完成！"
        ;;
        
    *)
        log_error "未知命令: $COMMAND"
        echo "可用命令："
        echo "  convert - 執行詞典轉換（預設）"
        echo "  check   - 檢查轉換結果"
        echo "  test    - 測試轉換效果"
        echo "  clean   - 清理轉換結果"
        exit 1
        ;;
esac