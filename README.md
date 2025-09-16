# Fushin ERP RAG 智能檢索系統

## 系統架構

```
MySQL 資料庫 → DB-Sync (即時同步) → Elasticsearch → 向量生成 → RAG 搜尋
                     ↓
              JSONL 檔案 → Auto-Importer → ES Index
                                              ↓
                                        Vector Service
                                              ↓
                                          RAG API
                                              ↓
                                          Web UI
```

## 主要改進

### 1. 資料一致性修正
- ✅ 將 `product_warehouse_b` 表中的 W 開頭產品改為 P031-P035
- ✅ 在 `product_master_a` 主檔中新增對應的產品資料
- ✅ 確保所有產品都有完整的主檔記錄

### 2. 即時資料同步
- ✅ `db_sync.py` 支援快速檢查模式（每 10 秒檢查最近變更）
- ✅ 自動偵測資料庫新增/修改並同步到 Elasticsearch
- ✅ 加強產品關聯性處理，自動提取產品 ID
- ✅ 產品快取機制，提升關聯查詢效率

### 3. 系統優化
- ✅ 改進日誌記錄，所有服務都有獨立日誌檔案
- ✅ 加入健康檢查和自動重啟機制
- ✅ 優化 Docker 容器啟動順序和相依性
- ✅ 提供便利的管理腳本

## 快速開始

### 1. 前置需求
- Docker 和 Docker Compose
- OpenAI API Key
- 至少 4GB RAM

### 2. 設定環境變數
```bash
# 複製環境變數範本
cp .env.example .env

# 編輯 .env，填入您的 OpenAI API Key
vim .env
```

### 3. 啟動系統
```bash
# 賦予執行權限
chmod +x manage.sh

# 啟動所有服務
./manage.sh start

# 初始化資料庫（首次使用）
./manage.sh init-db
```

### 4. 驗證系統
```bash
# 檢查系統狀態
./manage.sh status

# 測試查詢
./manage.sh test "處理中"

# 查看統計資訊
./manage.sh stats
```

## 服務端點

| 服務 | URL | 說明 |
|------|-----|------|
| Elasticsearch | http://localhost:9200 | 搜尋引擎 |
| Kibana | http://localhost:5601 | 資料視覺化 |
| RAG API | http://localhost:8010 | API 服務 |
| API 文檔 | http://localhost:8010/docs | Swagger UI |
| MySQL | localhost:3316 | 資料庫 |
| Web UI (生產) | http://localhost | React 前端 |
| Web UI (開發) | http://localhost:5174 | 開發模式 |

## 資料流程

### 1. 資料同步流程
```
MySQL 資料變更
    ↓
db-sync 偵測變更（每 10 秒快速檢查）
    ↓
匯出 JSONL 檔案到 /data/import
    ↓
auto-importer 監控目錄
    ↓
批次匯入 Elasticsearch
    ↓
vector-generator 生成向量
```

### 2. 查詢流程
```
使用者查詢
    ↓
RAG API 接收請求
    ↓
關鍵字 + 向量混合搜尋
    ↓
取得相關文件
    ↓
GPT 生成答案
    ↓
返回結果
```

## 管理命令

```bash
# 系統管理
./manage.sh start      # 啟動系統
./manage.sh stop       # 停止系統
./manage.sh restart    # 重啟系統
./manage.sh status     # 檢查狀態

# 日誌查看
./manage.sh logs              # 查看所有日誌
./manage.sh logs db-sync      # 查看特定服務日誌
./manage.sh logs rag-api

# 資料管理
./manage.sh init-db    # 初始化資料庫
./manage.sh clean      # 清理所有資料（慎用）

# 測試功能
./manage.sh test "查詢內容"   # 測試 RAG 查詢
./manage.sh stats             # 查看系統統計

# 進入容器
./manage.sh exec mysql        # 進入 MySQL 容器
./manage.sh exec elasticsearch  # 進入 ES 容器
```

## 監控與除錯

### 查看同步狀態
```bash
# 查看 db-sync 日誌
docker-compose logs -f db-sync

# 查看同步狀態檔案
cat scripts/.db_sync_state.json
```

### 查看向量生成進度
```bash
# 查看 vector-generator 日誌
docker-compose logs -f vector-generator

# 使用 API 查看統計
curl http://localhost:8010/stats | jq .
```

### Elasticsearch 查詢
```bash
# 查看索引
curl -u elastic:admin@12345 http://localhost:9200/_cat/indices?v

# 查詢文件數量
curl -u elastic:admin@12345 http://localhost:9200/erp-*/_count

# 搜尋測試
curl -u elastic:admin@12345 -X POST http://localhost:9200/erp-*/_search \
  -H "Content-Type: application/json" \
  -d '{"query": {"match": {"status": "處理中"}}}'
```

## 資料結構

### 產品主檔 (product_master_a)
- P001-P030: 原有產品
- P031-P035: 新增產品（原 W001-W005）

### 倉儲資料 (product_warehouse_b)
- 所有產品 ID 都對應到主檔
- 支援多倉庫存放
- 自動關聯產品資訊

### 客訴資料 (customer_complaint_c)
- 自動提取描述中的產品 ID
- 關聯相關產品資訊
- 支援狀態追蹤

## 效能優化建議

1. **資料庫索引**
   ```sql
   -- 在 MySQL 中建立索引
   CREATE INDEX idx_last_modified ON product_master_a(last_modified);
   CREATE INDEX idx_last_modified ON product_warehouse_b(last_modified);
   CREATE INDEX idx_last_modified ON customer_complaint_c(last_modified);
   ```

2. **Elasticsearch 優化**
   - 調整 JVM 記憶體：修改 `ES_JAVA_OPTS`
   - 設定適當的分片數量
   - 定期清理舊資料

3. **向量生成優化**
   - 調整批次大小：`VECTOR_BATCH_SIZE`
   - 使用較小的嵌入模型以提升速度

## 故障排除

### 問題 1：Elasticsearch 無法啟動
```bash
# 檢查記憶體限制
sudo sysctl -w vm.max_map_count=262144

# 檢查目錄權限
chmod -R 777 data/elasticsearch
```

### 問題 2：向量生成失敗
```bash
# 確認 OpenAI API Key
echo $OPENAI_API_KEY

# 測試 API 連線
curl https://api.openai.com/v1/models \
  -H "Authorization: Bearer $OPENAI_API_KEY"
```

### 問題 3：資料同步停止
```bash
# 重啟同步服務
docker-compose restart db-sync

# 清理狀態檔案（重新同步）
rm scripts/.db_sync_state.json
docker-compose restart db-sync
```

## 開發模式

### 啟動開發環境
```bash
# 使用開發 profile
docker-compose --profile dev up

# 前端開發（熱重載）
cd web
npm install
npm run dev
```

### API 開發
- Swagger UI: http://localhost:8010/docs
- ReDoc: http://localhost:8010/redoc

## 備份與還原

### 備份資料
```bash
# 備份 MySQL
docker-compose exec mysql mysqldump -uroot -proot fuhsin_erp_demo > backup.sql

# 備份 Elasticsearch
curl -u elastic:admin@12345 -X PUT http://localhost:9200/_snapshot/backup \
  -H "Content-Type: application/json" \
  -d '{"type": "fs", "settings": {"location": "/backup"}}'
```

### 還原資料
```bash
# 還原 MySQL
docker-compose exec -T mysql mysql -uroot -proot fuhsin_erp_demo < backup.sql

# 還原 Elasticsearch
curl -u elastic:admin@12345 -X POST http://localhost:9200/_snapshot/backup/snapshot_1/_restore
```

## 授權與支援

本專案採用 MIT 授權
如有問題請聯繫技術支援團隊
