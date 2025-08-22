#!/bin/bash
# 更新詞典腳本

echo "正在更新詞典..."

# 重新運行轉換
docker-compose run --rm dict-converter

# 重新載入詞典
curl -X POST "http://elastic:admin@12345@localhost:9200/_analyze" \
  -H 'Content-Type: application/json' \
  -d '{
    "analyzer": "ik_max_word",
    "text": "測試詞典更新"
  }'

echo "詞典更新完成！"
