#!/usr/bin/env python3
"""測試跨表搜索索引"""

import requests
import json

# Elasticsearch 設定
ES_URL = "http://localhost:9200"
AUTH = ('elastic', 'admin@12345')

def test_cross_search_index():
    """測試跨表搜索索引"""
    
    # 檢查索引是否存在
    response = requests.get(f"{ES_URL}/erp-cross-search", auth=AUTH)
    
    if response.status_code == 200:
        print("✓ 跨表搜索索引已創建")
        
        # 取得索引統計
        stats_response = requests.get(f"{ES_URL}/erp-cross-search/_stats", auth=AUTH)
        if stats_response.status_code == 200:
            stats = stats_response.json()
            doc_count = stats['indices']['erp-cross-search']['primaries']['docs']['count']
            print(f"  文檔數量: {doc_count}")
        
        # 測試搜索
        search_query = {
            "query": {
                "multi_match": {
                    "query": "手機",
                    "fields": ["title^2", "content", "content_simplified", "content_traditional"]
                }
            }
        }
        
        search_response = requests.post(
            f"{ES_URL}/erp-cross-search/_search",
            auth=AUTH,
            json=search_query
        )
        
        if search_response.status_code == 200:
            results = search_response.json()
            print(f"  搜索測試: 找到 {results['hits']['total']['value']} 個結果")
    else:
        print("✗ 跨表搜索索引不存在")

if __name__ == "__main__":
    test_cross_search_index()
