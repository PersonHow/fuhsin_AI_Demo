#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速搜尋 API - 提供簡單的搜尋介面
放在 scripts/quick_search.py
"""

import requests
import json
import sys
from requests.auth import HTTPBasicAuth

class QuickSearch:
    def __init__(self, es_url="http://localhost:9200", username="elastic", password="admin@12345"):
        self.es_url = es_url
        self.auth = HTTPBasicAuth(username, password)
    
    def search_keyword(self, keyword, table_filter=None, limit=10):
        """搜尋關鍵字"""
        query = {
            "size": limit,
            "query": {
                "multi_match": {
                    "query": keyword,
                    "fields": [
                        "keyword_id^5",      # 關鍵字ID最高權重
                        "content^3",         # 內容次高權重
                        "content.traditional^3",
                        "content.simplified^3", 
                        "field_*^2"         # 其他欄位
                    ],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            },
            "highlight": {
                "fields": {
                    "content": {"fragment_size": 200},
                    "field_*": {"fragment_size": 150}
                }
            },
            "aggs": {
                "tables": {
                    "terms": {"field": "table_name", "size": 20}
                },
                "sources": {
                    "terms": {"field": "metadata.source_file", "size": 10}
                }
            }
        }
        
        # 如果指定了表名篩選
        if table_filter:
            query["query"] = {
                "bool": {
                    "must": [query["query"]],
                    "filter": [{"term": {"table_name": table_filter}}]
                }
            }
        
        try:
            response = requests.post(
                f"{self.es_url}/*/_search",
                json=query,
                auth=self.auth,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"搜尋失敗: {e}")
            return None
    
    def print_results(self, results, keyword):
        """美化輸出搜尋結果"""
        if not results:
            print("❌ 搜尋失敗")
            return
        
        hits = results.get('hits', {})
        total = hits.get('total', {}).get('value', 0)
        took = results.get('took', 0)
        
        print(f"\n🔍 搜尋關鍵字: '{keyword}'")
        print(f"📊 找到 {total} 筆結果 (耗時 {took} ms)")
        print("=" * 80)
        
        # 顯示搜尋結果
        for i, hit in enumerate(hits.get('hits', [])[:10], 1):
            source = hit['_source']
            score = hit['_score']
            
            print(f"\n📄 結果 {i} (相關度: {score:.2f})")
            print(f"📁 來源檔案: {source.get('metadata', {}).get('source_file', 'N/A')}")
            print(f"🏷️ 表名: {source.get('table_name', 'N/A')}")
            
            if source.get('keyword_id'):
                print(f"🔑 關鍵字ID: {source['keyword_id']}")
            
            # 顯示內容
            content = source.get('content', '')
            if content:
                print(f"📝 內容: {content[:200]}...")
            
            # 顯示高亮
            if 'highlight' in hit:
                print("✨ 相關片段:")
                for field, fragments in hit['highlight'].items():
                    for fragment in fragments[:2]:
                        print(f"   {fragment}")
            
            # 顯示關鍵欄位
            field_data = []
            for key, value in source.items():
                if key.startswith('field_') and not key.endswith('.traditional') and not key.endswith('.simplified'):
                    field_name = key.replace('field_', '')
                    field_data.append(f"{field_name}: {str(value)[:50]}")
            
            if field_data:
                print(f"📋 相關欄位: {' | '.join(field_data[:3])}")
        
        # 顯示統計資訊
        aggs = results.get('aggregations', {})
        if 'tables' in aggs:
            print(f"\n📊 按表分布:")
            for bucket in aggs['tables']['buckets'][:5]:
                print(f"   {bucket['key']}: {bucket['doc_count']} 筆")
        
        if 'sources' in aggs:
            print(f"\n📁 按檔案分布:")
            for bucket in aggs['sources']['buckets'][:5]:
                print(f"   {bucket['key']}: {bucket['doc_count']} 筆")

def main():
    if len(sys.argv) < 2:
        print("使用方式:")
        print("  python quick_search.py <關鍵字> [表名篩選]")
        print("")
        print("範例:")
        print("  python quick_search.py P001")
        print("  python quick_search.py 產品資訊 products")
        print("  python quick_search.py 客戶")
        return
    
    keyword = sys.argv[1]
    table_filter = sys.argv[2] if len(sys.argv) > 2 else None
    
    searcher = QuickSearch()
    results = searcher.search_keyword(keyword, table_filter)
    searcher.print_results(results, keyword)

if __name__ == "__main__":
    main()