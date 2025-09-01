#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
向量生成服務 - 使用 OpenAI API 生成文本向量
"""
import os
import time
import json
import requests
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import openai
from openai import OpenAI
import numpy as np

# 環境變數配置
ES_URL = os.environ.get("ES_URL", "http://es01:9200")
ES_USER = os.environ.get("ES_USER", "elastic")
ES_PASS = os.environ.get("ES_PASS", "admin@12345")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
BATCH_SIZE = int(os.environ.get("VECTOR_BATCH_SIZE", "100"))
SLEEP_SEC = int(os.environ.get("SLEEP", "10"))

# 初始化 OpenAI 客戶端
client = OpenAI(
    api_key=OPENAI_API_KEY,
    base_url=OPENAI_BASE_URL  # 支援自訂端點（如 Azure OpenAI）
)

# Elasticsearch 連線
session = requests.Session()
session.auth = (ES_USER, ES_PASS)
session.headers.update({"Content-Type": "application/json"})

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

class VectorGenerator:
    """向量生成器"""
    
    def __init__(self):
        self.dimension = 1536  # text-embedding-3-small 的維度
        if "text-embedding-3-large" in EMBEDDING_MODEL:
            self.dimension = 3072
        elif "ada" in EMBEDDING_MODEL:
            self.dimension = 1536
            
    def generate_embedding(self, text: str) -> List[float]:
        """生成單個文本的向量"""
        try:
            response = client.embeddings.create(
                model=EMBEDDING_MODEL,
                input=text[:8000],  # OpenAI 限制
                encoding_format="float"
            )
            return response.data[0].embedding
        except Exception as e:
            log(f"⚠️ 生成向量失敗: {e}")
            return None
    
    def batch_generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """批量生成向量"""
        try:
            # OpenAI 支援批量請求
            truncated_texts = [t[:8000] for t in texts]
            response = client.embeddings.create(
                model=EMBEDDING_MODEL,
                input=truncated_texts,
                encoding_format="float"
            )
            return [data.embedding for data in response.data]
        except Exception as e:
            log(f"⚠️ 批量生成向量失敗: {e}")
            # 降級為逐個生成
            results = []
            for text in texts:
                emb = self.generate_embedding(text)
                results.append(emb if emb else [0.0] * self.dimension)
                time.sleep(0.1)  # 避免 rate limit
            return results

class ElasticsearchVectorUpdater:
    """Elasticsearch 向量更新器"""
    
    def __init__(self, vector_gen: VectorGenerator):
        self.vector_gen = vector_gen
        
    def update_index_mapping(self, index_pattern: str = "erp-*"):
        """更新索引映射，添加向量欄位"""
        mapping_update = {
            "properties": {
                "content_vector": {
                    "type": "dense_vector",
                    "dims": self.vector_gen.dimension,
                    "index": True,
                    "similarity": "cosine"
                },
                "vector_generated_at": {
                    "type": "date"
                }
            }
        }
        
        # 獲取所有符合模式的索引
        r = session.get(f"{ES_URL}/{index_pattern}")
        if r.status_code == 200:
            indices = list(r.json().keys())
            for index in indices:
                try:
                    r = session.put(
                        f"{ES_URL}/{index}/_mapping",
                        json=mapping_update
                    )
                    if r.status_code == 200:
                        log(f"✅ 更新索引映射: {index}")
                except Exception as e:
                    log(f"⚠️ 更新索引 {index} 失敗: {e}")
    
    def find_documents_without_vectors(self, index_pattern: str = "erp-*", size: int = 100):
        """查找沒有向量的文檔"""
        query = {
            "size": size,
            "_source": ["searchable_content", "all_content", "field_*"],
            "query": {
                "bool": {
                    "must_not": [
                        {"exists": {"field": "content_vector"}}
                    ]
                }
            }
        }
        
        try:
            r = session.post(f"{ES_URL}/{index_pattern}/_search", json=query)
            if r.status_code == 200:
                return r.json()["hits"]["hits"]
        except Exception as e:
            log(f"⚠️ 查詢失敗: {e}")
        return []
    
    def update_document_vectors(self, documents: List[Dict]):
        """更新文檔向量"""
        if not documents:
            return
        
        # 準備文本
        texts = []
        for doc in documents:
            source = doc["_source"]
            # 優先使用 searchable_content，其次 all_content
            text = source.get("searchable_content", "") or source.get("all_content", "")
            if not text:
                # 組合所有 field_ 開頭的欄位
                field_texts = []
                for key, value in source.items():
                    if key.startswith("field_") and value:
                        field_texts.append(str(value))
                text = " ".join(field_texts)
            texts.append(text)
        
        # 批量生成向量
        log(f"🔄 生成 {len(texts)} 個向量...")
        embeddings = self.vector_gen.batch_generate_embeddings(texts)
        
        # 批量更新
        bulk_body = []
        for doc, embedding in zip(documents, embeddings):
            if embedding:
                bulk_body.append(json.dumps({
                    "update": {
                        "_index": doc["_index"],
                        "_id": doc["_id"]
                    }
                }))
                bulk_body.append(json.dumps({
                    "doc": {
                        "content_vector": embedding,
                        "vector_generated_at": datetime.now().isoformat()
                    }
                }))
        
        if bulk_body:
            payload = "\n".join(bulk_body) + "\n"
            try:
                r = session.post(
                    f"{ES_URL}/_bulk",
                    data=payload,
                    headers={"Content-Type": "application/x-ndjson"}
                )
                if r.status_code == 200:
                    result = r.json()
                    if not result.get("errors"):
                        log(f"✅ 成功更新 {len(documents)} 個文檔的向量")
                    else:
                        log(f"⚠️ 部分更新失敗")
            except Exception as e:
                log(f"❌ 批量更新失敗: {e}")

def main():
    """主程序"""
    if not OPENAI_API_KEY:
        log("❌ 未設置 OPENAI_API_KEY")
        return
    
    log("🚀 向量生成服務啟動")
    log(f"📊 使用模型: {EMBEDDING_MODEL}")
    
    vector_gen = VectorGenerator()
    updater = ElasticsearchVectorUpdater(vector_gen)
    
    # 初次運行時更新映射
    updater.update_index_mapping()
    
    while True:
        try:
            # 查找需要生成向量的文檔
            docs = updater.find_documents_without_vectors(size=BATCH_SIZE)
            
            if docs:
                log(f"📝 找到 {len(docs)} 個需要生成向量的文檔")
                updater.update_document_vectors(docs)
            else:
                log("😴 所有文檔都已有向量，等待中...")
            
            time.sleep(SLEEP_SEC)
            
        except Exception as e:
            log(f"❌ 主循環錯誤: {e}")
            time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()
