#!/bin/bash
# 測試分詞效果

if [ $# -eq 0 ]; then
    echo "使用方法: $0 \"要測試的文本\""
    exit 1
fi

TEXT="$1"

echo "測試文本: $TEXT"
echo "===================="

echo -e "\n使用 ik_max_word 分詞:"
curl -s -X POST "http://elastic:admin@12345@localhost:9200/_analyze" \
  -H 'Content-Type: application/json' \
  -d "{
    \"analyzer\": \"ik_max_word\",
    \"text\": \"$TEXT\"
  }" | jq -r '.tokens[].token' | tr '\n' ' '

echo -e "\n\n使用 ik_smart 分詞:"
curl -s -X POST "http://elastic:admin@12345@localhost:9200/_analyze" \
  -H 'Content-Type: application/json' \
  -d "{
    \"analyzer\": \"ik_smart\",
    \"text\": \"$TEXT\"
  }" | jq -r '.tokens[].token' | tr '\n' ' '

echo -e "\n"
