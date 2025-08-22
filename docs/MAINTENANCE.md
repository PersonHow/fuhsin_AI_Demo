# 維護指南

## 日常維護

### 1. 更新詞典

當需要更新詞典時：

```bash
# 1. 編輯自定義詞典
vim dictionaries/custom/user_dict.dic

# 2. 重新運行轉換
./scripts/update_dicts.sh

# 3. 重啟 Elasticsearch（如需要）
docker-compose restart elasticsearch
```

### 2. 添加熱詞

編輯 `dictionaries/custom/hot_words.dic`，詞典會自動更新。

### 3. 備份

定期執行備份：

```bash
./scripts/backup.sh
```

### 4. 監控

查看 Elasticsearch 健康狀態：

```bash
curl -u elastic:admin@12345 http://localhost:9200/_cluster/health?pretty
```

## 詞典更新流程

### 自動更新（推薦）

1. 修改 `dictionaries/custom/` 下的詞典文件
2. 執行 `./scripts/update_dicts.sh`
3. 系統會自動重新載入詞典

### 手動更新

1. 修改詞典文件
2. 重新運行詞典轉換：
   ```bash
   docker-compose run --rm dict-converter
   ```
3. 重啟 Elasticsearch：
   ```bash
   docker-compose restart elasticsearch
   ```

## 故障排除

請參考 `docs/TROUBLESHOOTING.md`
