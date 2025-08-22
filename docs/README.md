# Elasticsearch IK 繁體中文分詞環境

本專案提供了一個完整的 Elasticsearch + IK 分詞器繁體中文環境，使用 OpenCC 進行詞典轉換。

## 快速開始

1. 初始化環境：
   ```bash
   ./init.sh
   ```

2. 啟動服務：
   ```bash
   docker-compose up -d
   ```

3. 測試分詞：
   ```bash
   ./scripts/test_analyzer.sh "測試繁體中文分詞效果"
   ```

## 服務訪問

- Elasticsearch: http://localhost:9200 (elastic/admin@12345)
- Kibana: http://localhost:5601 (elastic/admin@12345)
- 詞典服務: http://localhost:8080

## 目錄結構

請參考專案根目錄的結構說明。

## 維護指南

請參考 `docs/MAINTENANCE.md`
