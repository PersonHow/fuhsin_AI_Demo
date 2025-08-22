# 故障排除指南

## 常見問題

### 1. Elasticsearch 無法啟動

**症狀**：容器反覆重啟

**解決方案**：
```bash
# 檢查日誌
docker-compose logs elasticsearch

# 檢查權限
sudo chown -R 1000:1000 data/elasticsearch
sudo chmod -R 755 data/elasticsearch
```

### 2. 分詞效果不佳

**症狀**：某些詞彙無法正確分詞

**解決方案**：
1. 檢查詞典是否正確載入：
   ```bash
   docker exec elasticsearch ls -la /usr/share/elasticsearch/config/analysis-ik/
   ```

2. 添加到自定義詞典：
   ```bash
   echo "新詞彙" >> dictionaries/custom/user_dict.dic
   ./scripts/update_dicts.sh
   ```

### 3. 記憶體不足

**症狀**：OutOfMemoryError

**解決方案**：
編輯 `.env` 文件，增加堆記憶體：
```
ES_HEAP_SIZE=2g
```

### 4. 詞典轉換失敗

**症狀**：dict-converter 容器錯誤

**解決方案**：
```bash
# 查看轉換日誌
docker-compose logs dict-converter

# 手動運行轉換
docker-compose run --rm dict-converter bash
# 在容器內手動執行轉換
```

## 性能優化

### 1. 調整分詞模式

- `ik_max_word`：細粒度分詞，召回率高
- `ik_smart`：智能分詞，準確率高

### 2. 索引優化

```json
{
  "settings": {
    "index": {
      "refresh_interval": "30s",
      "number_of_shards": 1,
      "number_of_replicas": 0
    }
  }
}
```

### 3. JVM 調優

編輯 `.env`：
```
ES_JAVA_OPTS=-Xms2g -Xmx2g -XX:+UseG1GC
```
