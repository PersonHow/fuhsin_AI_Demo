#!/bin/bash
# 優化版系統管理腳本 - 支援大量資料處理

set -e

# 顏色輸出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 檢查 .env 檔案
check_env() {
    if [ ! -f .env ]; then
        echo -e "${YELLOW}⚠️  .env 檔案不存在，建立預設檔案...${NC}"
        cat > .env << 'EOF'
# Elasticsearch 設定
ES_USER=elastic
ES_PASS=admin@12345

# OpenAI 設定 (請填入您的 API Key)
OPENAI_API_KEY=your-openai-api-key-here
OPENAI_BASE_URL=https://api.openai.com/v1
EMBEDDING_MODEL=text-embedding-3-small
GPT_MODEL=gpt-4o-mini

# 資料庫同步設定（大量資料優化）
DB_BATCH_SIZE=2000
DB_PAGE_SIZE=5000
PARALLEL_THREADS=4
DB_SYNC_INTERVAL=30

# SQL 導入設定
SQL_SCAN_INTERVAL=10
SQL_BATCH_SIZE=1000

# 向量生成設定
VECTOR_BATCH_SIZE=50
VECTOR_SLEEP=10
EOF
        echo -e "${YELLOW}請編輯 .env 檔案，填入您的 OpenAI API Key${NC}"
        exit 1
    fi
}

# 建立必要目錄
setup_dirs() {
    echo -e "${GREEN}📁 建立必要目錄...${NC}"
    mkdir -p data/{elasticsearch,kibana}
    mkdir -p logs/{elasticsearch,db-sync,importer,vector,api}
    mkdir -p scripts
    mkdir -p sql/incoming/{.done,.error,.progress}
    mkdir -p sql/init
    mkdir -p web
    chmod -R 777 data logs  # 確保 Docker 容器可以寫入
}

# 初始化資料庫
init_db() {
    echo -e "${GREEN}🗄️  初始化資料庫...${NC}"
    
    # 啟動 MySQL
    docker-compose up -d mysql
    
    # 等待 MySQL 就緒
    echo "等待 MySQL 啟動..."
    sleep 10
    
    # 檢查資料庫健康狀態
    until docker-compose exec mysql mysqladmin ping -uroot -proot &>/dev/null; do
        echo -n "."
        sleep 2
    done
    echo ""
    
    # 檢查是否已初始化
    if docker-compose exec mysql mysql -uroot -proot -e "USE fuhsin_erp_demo; SHOW TABLES;" 2>/dev/null | grep -q "product_master_a"; then
        echo -e "${YELLOW}資料庫已存在，跳過初始化${NC}"
        return
    fi
    
    # 匯入 SQL 檔案
    echo "匯入資料表結構..."
    if [ -f sql/00_init.sql ]; then
        docker-compose exec -T mysql mysql -uroot -proot < sql/00_init.sql
    fi
    
    echo "匯入初始資料..."
    for sql_file in sql/product_master_a.sql sql/product_warehouse_b.sql sql/customer_complaint_c.sql; do
        if [ -f "$sql_file" ]; then
            echo "  匯入 $(basename $sql_file)..."
            docker-compose exec -T mysql mysql -uroot -proot fuhsin_erp_demo < "$sql_file"
        fi
    done
    
    echo -e "${GREEN}✅ 資料庫初始化完成${NC}"
}

# 匯入 SQL 檔案
import_sql() {
    local sql_file="${1}"
    
    if [ -z "$sql_file" ]; then
        echo -e "${YELLOW}使用方法: $0 import-sql <檔案路徑>${NC}"
        return 1
    fi
    
    if [ ! -f "$sql_file" ]; then
        echo -e "${RED}❌ 檔案不存在: $sql_file${NC}"
        return 1
    fi
    
    echo -e "${GREEN}📥 準備匯入 SQL 檔案: $(basename $sql_file)${NC}"
    
    # 複製檔案到監控目錄
    cp "$sql_file" sql/incoming
    echo -e "${GREEN}✅ 檔案已放入自動導入佇列${NC}"
    echo "請查看 mysql-auto-importer 服務日誌以追蹤進度"
}

# 啟動系統
start() {
    check_env
    setup_dirs
    
    echo -e "${GREEN}🚀 啟動系統...${NC}"
    
    # 可選擇性啟動服務
    if [ "$1" == "--minimal" ]; then
        echo -e "${BLUE}啟動最小服務集（MySQL + ES + 同步）${NC}"
        docker-compose up -d mysql elasticsearch db-direct-sync mysql-auto-importer
    else
        docker-compose up -d
        docker-compose --profile dev up -d
    fi
    
    echo -e "${GREEN}⏳ 等待服務就緒...${NC}"
    sleep 20
    
    echo -e "${GREEN}✅ 系統啟動完成！${NC}"
    show_endpoints
}

# 顯示服務端點
show_endpoints() {
    echo ""
    echo -e "${BLUE}📊 服務端點：${NC}"
    echo "  - Elasticsearch: http://localhost:9200"
    echo "  - Kibana: http://localhost:5601"
    echo "  - RAG API: http://localhost:8010"
    echo "  - API 文檔: http://localhost:8010/docs"
    echo "  - MySQL: localhost:3316"
    echo ""
    echo -e "${YELLOW}💡 提示：${NC}"
    echo "  - 使用 './manage.sh logs [服務]' 查看日誌"
    echo "  - 使用 './manage.sh import-sql <檔案>' 匯入 SQL"
    echo "  - 使用 './manage.sh monitor' 監控同步狀態"
}

# 停止系統
stop() {
    echo -e "${YELLOW}🛑 停止系統...${NC}"
    docker-compose down
    echo -e "${GREEN}✅ 系統已停止${NC}"
}

# 重啟系統
restart() {
    stop
    sleep 2
    start "$@"
}

# 查看日誌
logs() {
    if [ -z "$1" ]; then
        echo -e "${BLUE}可用服務：${NC}"
        docker-compose ps --services
        echo ""
        echo -e "${YELLOW}查看特定服務日誌：${NC}"
        echo "  ./manage.sh logs db-direct-sync    # DB 同步服務"
        echo "  ./manage.sh logs mysql-auto-importer  # SQL 導入服務"
        echo "  ./manage.sh logs vector-generator  # 向量生成"
        echo "  ./manage.sh logs rag-api          # API 服務"
    else
        docker-compose logs -f --tail=100 "$1"
    fi
}

# 監控同步狀態
monitor() {
    echo -e "${GREEN}📊 同步狀態監控${NC}"
    echo "=========================================="
    
    # 檢查同步狀態檔案
    if [ -f scripts/.db_es_sync_state.json ]; then
        echo -e "${BLUE}DB 同步狀態：${NC}"
        cat scripts/.db_es_sync_state.json | python3 -m json.tool
        echo ""
    fi
    
    # 檢查 SQL 導入狀態
    if [ -f data/sql_import/.import_state.json ]; then
        echo -e "${BLUE}SQL 導入狀態：${NC}"
        cat data/sql_import/.import_state.json | python3 -m json.tool
        echo ""
    fi
    
    # 檢查待處理檔案
    echo -e "${BLUE}待處理 SQL 檔案：${NC}"
    ls -la data/sql_import/*.sql 2>/dev/null || echo "  無待處理檔案"
    echo ""
    
    # 檢查 ES 文檔數量
    echo -e "${BLUE}Elasticsearch 文檔統計：${NC}"
    for index in erp-products erp-warehouse erp-complaints; do
        count=$(curl -s -u elastic:admin@12345 "http://localhost:9200/${index}/_count" 2>/dev/null | grep -o '"count":[0-9]*' | cut -d: -f2)
        if [ ! -z "$count" ]; then
            echo "  ${index}: ${count} 筆"
        fi
    done
}

# 檢查系統狀態
status() {
    echo -e "${GREEN}📊 系統狀態：${NC}"
    docker-compose ps
    
    echo ""
    echo -e "${GREEN}🔍 健康檢查：${NC}"
    
    # 檢查 Elasticsearch
    if curl -s -u elastic:admin@12345 http://localhost:9200/_cluster/health 2>/dev/null | grep -q '"status":"green\|yellow"'; then
        echo -e "  Elasticsearch: ${GREEN}✅ 健康${NC}"
    else
        echo -e "  Elasticsearch: ${RED}❌ 異常${NC}"
    fi
    
    # 檢查 MySQL
    if docker-compose exec mysql mysqladmin ping -uroot -proot &>/dev/null 2>&1; then
        echo -e "  MySQL: ${GREEN}✅ 健康${NC}"
    else
        echo -e "  MySQL: ${RED}❌ 異常${NC}"
    fi
    
    # 檢查 RAG API
    if curl -s http://localhost:8010/health 2>/dev/null | grep -q '"status":"healthy\|degraded"'; then
        echo -e "  RAG API: ${GREEN}✅ 健康${NC}"
    else
        echo -e "  RAG API: ${RED}❌ 異常${NC}"
    fi
    
    # 檢查同步服務
    if docker-compose ps | grep -q "db-direct-sync.*Up"; then
        echo -e "  DB 同步: ${GREEN}✅ 運行中${NC}"
    else
        echo -e "  DB 同步: ${RED}❌ 停止${NC}"
    fi
    
    if docker-compose ps | grep -q "mysql-auto-importer.*Up"; then
        echo -e "  SQL 導入: ${GREEN}✅ 運行中${NC}"
    else
        echo -e "  SQL 導入: ${RED}❌ 停止${NC}"
    fi
}

# 清理資料
clean() {
    echo -e "${YELLOW}⚠️  警告：這將刪除所有資料！${NC}"
    read -p "確定要繼續嗎？(y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        stop
        echo -e "${YELLOW}🗑️  清理資料...${NC}"
        rm -rf data/elasticsearch/* data/kibana/* data/sql_import/{.done,.error,.progress}/*
        rm -rf logs/*
        rm -f scripts/.db_es_sync_state.json
        rm -f data/sql_import/.import_state.json
        docker volume prune -f
        echo -e "${GREEN}✅ 清理完成${NC}"
    fi
}

# 重置同步狀態
reset_sync() {
    echo -e "${YELLOW}重置同步狀態...${NC}"
    rm -f scripts/.db_es_sync_state.json
    rm -f data/sql_import/.import_state.json
    echo -e "${GREEN}✅ 同步狀態已重置，下次啟動將執行全量同步${NC}"
}

# 進入容器
exec_container() {
    if [ -z "$1" ]; then
        echo "請指定容器名稱"
        echo "可用容器："
        docker-compose ps --services
        exit 1
    fi
    docker-compose exec "$1" /bin/bash
}

# 測試 RAG 查詢
test_query() {
    local query="${1:-處理中}"
    echo -e "${GREEN}🔍 測試查詢: '$query'${NC}"
    
    curl -X POST http://localhost:8010/query \
        -H "Content-Type: application/json" \
        -d "{
            \"query\": \"$query\",
            \"mode\": \"hybrid\",
            \"top_k\": 5,
            \"use_gpt\": true
        }" 2>/dev/null | python3 -m json.tool
}

# 查看統計資訊
stats() {
    echo -e "${GREEN}📊 系統統計：${NC}"
    curl -s http://localhost:8010/stats 2>/dev/null | python3 -m json.tool
    
    echo ""
    echo -e "${GREEN}📊 資料庫統計：${NC}"
    docker-compose exec mysql mysql -uroot -proot -e "
        USE fuhsin_erp_demo;
        SELECT 'product_master_a' as table_name, COUNT(*) as count FROM product_master_a
        UNION ALL
        SELECT 'product_warehouse_b', COUNT(*) FROM product_warehouse_b
        UNION ALL
        SELECT 'customer_complaint_c', COUNT(*) FROM customer_complaint_c;
    " 2>/dev/null
}

# 效能調優建議
tune() {
    echo -e "${BLUE}🚀 效能調優建議：${NC}"
    echo ""
    echo "1. MySQL 優化："
    echo "   - 增加 innodb_buffer_pool_size (當前: 2G)"
    echo "   - 調整 max_connections (當前: 1000)"
    echo ""
    echo "2. Elasticsearch 優化："
    echo "   - 增加 JVM 記憶體 (當前: 2G)"
    echo "   - 調整 refresh_interval (當前: 30s)"
    echo ""
    echo "3. 同步優化："
    echo "   - 增加 DB_BATCH_SIZE (當前: ${DB_BATCH_SIZE:-2000})"
    echo "   - 增加 PARALLEL_THREADS (當前: ${PARALLEL_THREADS:-4})"
    echo ""
    echo "4. 系統資源："
    docker stats --no-stream
}

# 主程式
case "$1" in
    start)
        start "$2"
        ;;
    stop)
        stop
        ;;
    restart)
        restart "$2"
        ;;
    logs)
        logs "$2"
        ;;
    status)
        status
        ;;
    monitor)
        monitor
        ;;
    clean)
        clean
        ;;
    reset-sync)
        reset_sync
        ;;
    exec)
        exec_container "$2"
        ;;
    init-db)
        init_db
        ;;
    import-sql)
        import_sql "$2"
        ;;
    test)
        test_query "$2"
        ;;
    stats)
        stats
        ;;
    tune)
        tune
        ;;
    *)
        echo "使用方法: $0 {start|stop|restart|logs|status|monitor|clean|reset-sync|exec|init-db|import-sql|test|stats|tune}"
        echo ""
        echo -e "${BLUE}基本命令：${NC}"
        echo "  start [--minimal] - 啟動服務 (--minimal 只啟動核心服務)"
        echo "  stop             - 停止所有服務"
        echo "  restart          - 重啟所有服務"
        echo "  status           - 檢查系統狀態"
        echo "  logs [服務]      - 查看日誌"
        echo ""
        echo -e "${BLUE}資料管理：${NC}"
        echo "  init-db          - 初始化資料庫"
        echo "  import-sql <檔案> - 匯入 SQL 檔案"
        echo "  monitor          - 監控同步狀態"
        echo "  reset-sync       - 重置同步狀態"
        echo "  clean            - 清理所有資料"
        echo ""
        echo -e "${BLUE}除錯工具：${NC}"
        echo "  exec [容器]      - 進入容器"
        echo "  test [查詢]      - 測試 RAG 查詢"
        echo "  stats            - 查看統計資訊"
        echo "  tune             - 效能調優建議"
        exit 1
        ;;
esac
