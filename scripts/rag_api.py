#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RAG (Retrieval-Augmented Generation) API 服務
提供智能問答介面，結合向量搜尋和 OpenAI GPT
"""
import os
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import requests
from openai import OpenAI
import uvicorn

# 環境變數配置
ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
ES_USER = os.environ.get("ES_USER", "elastic")
ES_PASS = os.environ.get("ES_PASS", "admin@12345")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
GPT_MODEL = os.environ.get("GPT_MODEL", "gpt-4o-mini")

# 初始化
app = FastAPI(title="RAG 檢索 API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# OpenAI 客戶端
client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL)

# Elasticsearch 連線
es_session = requests.Session()
es_session.auth = (ES_USER, ES_PASS)
es_session.headers.update({"Content-Type": "application/json"})

# === 資料模型 ===
class QueryRequest(BaseModel):
    query: str
    mode: str = "hybrid"  # "keyword", "vector", "hybrid"
    top_k: int = 5
    index_pattern: str = "erp-*"
    use_gpt: bool = True
    temperature: float = 0.7

class QueryResponse(BaseModel):
    query: str
    answer: Optional[str]
    sources: List[Dict[str, Any]]
    search_mode: str
    total_hits: int
    processing_time_ms: int

class HealthResponse(BaseModel):
    status: str
    elasticsearch: bool
    openai: bool
    timestamp: str

# === 核心功能 ===
class RAGEngine:
    """RAG 引擎"""
    
    @staticmethod
    def generate_embedding(text: str) -> List[float]:
        """生成查詢向量"""
        try:
            response = client.embeddings.create(
                model=EMBEDDING_MODEL,
                input=text[:8000]
            )
            return response.data[0].embedding
        except Exception as e:
            print(f"生成向量失敗: {e}")
            return None
    
    @staticmethod
    def keyword_search(query: str, index_pattern: str, size: int = 5) -> Dict:
        """關鍵字搜尋（使用 IK 分詞）"""
        search_body = {
            "size": size,
            "_source": {
                "excludes": ["content_vector"]
            },
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": [
                        "searchable_content^3",
                        "all_content^2",
                        "field_*"
                    ],
                    "type": "best_fields",
                    "analyzer": "ik_smart"
                }
            },
            "highlight": {
                "fields": {
                    "searchable_content": {"fragment_size": 150},
                    "all_content": {"fragment_size": 150},
                    "field_*": {"fragment_size": 100}
                }
            }
        }
        
        r = es_session.post(f"{ES_URL}/{index_pattern}/_search", json=search_body)
        if r.status_code == 200:
            return r.json()
        return {"hits": {"hits": [], "total": {"value": 0}}}
    
    @staticmethod
    def vector_search(query_vector: List[float], index_pattern: str, size: int = 5) -> Dict:
        """向量搜尋"""
        search_body = {
            "size": size,
            "_source": {
                "excludes": ["content_vector"]
            },
            "query": {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": "cosineSimilarity(params.query_vector, 'content_vector') + 1.0",
                        "params": {
                            "query_vector": query_vector
                        }
                    }
                }
            }
        }
        
        # 或使用 knn 搜尋（ES 8.0+）
        knn_body = {
            "size": size,
            "_source": {
                "excludes": ["content_vector"]
            },
            "knn": {
                "field": "content_vector",
                "query_vector": query_vector,
                "k": size,
                "num_candidates": size * 10
            }
        }
        
        try:
            r = es_session.post(f"{ES_URL}/{index_pattern}/_search", json=knn_body)
            if r.status_code == 200:
                return r.json()
        except:
            # 降級使用 script_score
            r = es_session.post(f"{ES_URL}/{index_pattern}/_search", json=search_body)
            if r.status_code == 200:
                return r.json()
        
        return {"hits": {"hits": [], "total": {"value": 0}}}
    
    @staticmethod
    def hybrid_search(query: str, query_vector: List[float], index_pattern: str, size: int = 5) -> Dict:
        """混合搜尋（結合關鍵字和向量）"""
        search_body = {
            "size": size * 2,  # 取更多結果後重排序
            "_source": {
                "excludes": ["content_vector"]
            },
            "query": {
                "bool": {
                    "should": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": ["searchable_content^2", "all_content", "field_*"],
                                "type": "best_fields",
                                "analyzer": "ik_smart",
                                "boost": 0.5
                            }
                        },
                        {
                            "script_score": {
                                "query": {"match_all": {}},
                                "script": {
                                    "source": "cosineSimilarity(params.query_vector, 'content_vector') + 1.0",
                                    "params": {
                                        "query_vector": query_vector
                                    }
                                },
                                "boost": 0.5
                            }
                        }
                    ]
                }
            },
            "highlight": {
                "fields": {
                    "searchable_content": {"fragment_size": 150},
                    "all_content": {"fragment_size": 150},
                    "field_*": {"fragment_size": 100}
                }
            }
        }
        
        r = es_session.post(f"{ES_URL}/{index_pattern}/_search", json=search_body)
        if r.status_code == 200:
            result = r.json()
            # 只返回前 size 個結果
            result["hits"]["hits"] = result["hits"]["hits"][:size]
            return result
        return {"hits": {"hits": [], "total": {"value": 0}}}
    
    @staticmethod
    def format_context(search_results: Dict) -> str:
        """格式化搜尋結果為上下文"""
        contexts = []
        for hit in search_results.get("hits", {}).get("hits", [])[:5]:
            source = hit["_source"]
            metadata = source.get("metadata", {})
            
            # 收集重要欄位
            content_parts = []
            
            # 優先顯示高亮內容
            if "highlight" in hit:
                for field, highlights in hit["highlight"].items():
                    content_parts.extend(highlights[:2])
            
            # 如果沒有高亮，使用原始內容
            if not content_parts:
                if source.get("searchable_content"):
                    content_parts.append(source["searchable_content"][:200])
                elif source.get("all_content"):
                    content_parts.append(source["all_content"][:200])
            
            # 組合上下文
            context = f"[來源: {metadata.get('source_file', 'unknown')}, "
            context += f"表: {metadata.get('table_name', 'unknown')}]\n"
            context += "\n".join(content_parts)
            contexts.append(context)
        
        return "\n---\n".join(contexts)
    
    @staticmethod
    def generate_answer(query: str, context: str, temperature: float = 0.7) -> str:
        """使用 GPT 生成答案"""
        system_prompt = """你是一個專業的資料庫查詢助手。請根據提供的上下文資訊，回答用戶的問題。
        
規則：
1. 只根據提供的上下文回答，不要編造資訊
2. 如果上下文中沒有相關資訊，請明確說明
3. 回答要準確、簡潔、有條理
4. 使用繁體中文回答
5. 如果涉及數據，請引用具體來源"""
        
        user_prompt = f"""問題：{query}

相關上下文：
{context}

請根據上述上下文回答問題。"""
        
        try:
            response = client.chat.completions.create(
                model=GPT_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=temperature,
                max_tokens=1000
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"生成答案時發生錯誤：{str(e)}"

# === API 端點 ===
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """健康檢查"""
    es_health = False
    openai_health = False
    
    # 檢查 Elasticsearch
    try:
        r = es_session.get(f"{ES_URL}/_cluster/health")
        es_health = r.status_code == 200
    except:
        pass
    
    # 檢查 OpenAI
    try:
        if OPENAI_API_KEY:
            # 簡單測試
            test_embedding = client.embeddings.create(
                model=EMBEDDING_MODEL,
                input="test"
            )
            openai_health = True
    except:
        pass
    
    status = "healthy" if (es_health and openai_health) else "degraded"
    
    return HealthResponse(
        status=status,
        elasticsearch=es_health,
        openai=openai_health,
        timestamp=datetime.now().isoformat()
    )

@app.post("/query", response_model=QueryResponse)
async def query_data(request: QueryRequest):
    """智能查詢端點"""
    start_time = datetime.now()
    
    # 生成查詢向量
    query_vector = None
    if request.mode in ["vector", "hybrid"]:
        query_vector = RAGEngine.generate_embedding(request.query)
        if not query_vector:
            raise HTTPException(status_code=500, detail="向量生成失敗")
    
    # 執行搜尋
    if request.mode == "keyword":
        search_results = RAGEngine.keyword_search(
            request.query, request.index_pattern, request.top_k
        )
    elif request.mode == "vector":
        search_results = RAGEngine.vector_search(
            query_vector, request.index_pattern, request.top_k
        )
    else:  # hybrid
        search_results = RAGEngine.hybrid_search(
            request.query, query_vector, request.index_pattern, request.top_k
        )
    
    # 格式化結果
    sources = []
    for hit in search_results.get("hits", {}).get("hits", []):
        source = hit["_source"]
        sources.append({
            "score": hit.get("_score", 0),
            "index": hit["_index"],
            "metadata": source.get("metadata", {}),
            "content": source.get("searchable_content", "")[:200],
            "highlights": hit.get("highlight", {})
        })
    
    # 生成答案
    answer = None
    if request.use_gpt and sources:
        context = RAGEngine.format_context(search_results)
        answer = RAGEngine.generate_answer(
            request.query, context, request.temperature
        )
    
    # 計算處理時間
    processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
    
    return QueryResponse(
        query=request.query,
        answer=answer,
        sources=sources,
        search_mode=request.mode,
        total_hits=search_results.get("hits", {}).get("total", {}).get("value", 0),
        processing_time_ms=processing_time
    )

@app.get("/stats")
async def get_statistics():
    """獲取系統統計資訊"""
    stats = {}
    
    # 獲取索引統計
    try:
        r = es_session.get(f"{ES_URL}/erp-*/_stats")
        if r.status_code == 200:
            data = r.json()
            total_docs = sum(idx["primaries"]["docs"]["count"] 
                           for idx in data["indices"].values())
            total_size = sum(idx["primaries"]["store"]["size_in_bytes"] 
                           for idx in data["indices"].values())
            
            stats["indices"] = {
                "count": len(data["indices"]),
                "total_documents": total_docs,
                "total_size_mb": round(total_size / 1024 / 1024, 2)
            }
    except:
        pass
    
    # 檢查向量化進度
    try:
        # 有向量的文檔數
        vector_query = {
            "size": 0,
            "query": {"exists": {"field": "content_vector"}}
        }
        r1 = es_session.post(f"{ES_URL}/erp-*/_count", json=vector_query)
        
        # 總文檔數
        r2 = es_session.post(f"{ES_URL}/erp-*/_count", json={"query": {"match_all": {}}})
        
        if r1.status_code == 200 and r2.status_code == 200:
            with_vector = r1.json()["count"]
            total = r2.json()["count"]
            stats["vectorization"] = {
                "completed": with_vector,
                "total": total,
                "progress_percent": round((with_vector / total * 100) if total > 0 else 0, 2)
            }
    except:
        pass
    
    stats["timestamp"] = datetime.now().isoformat()
    return stats

# === 主程序 ===
if __name__ == "__main__":
    if not OPENAI_API_KEY:
        print("⚠️ 警告：未設置 OPENAI_API_KEY，部分功能將無法使用")
    
    print(f"🚀 RAG API 服務啟動")
    print(f"📊 使用模型：Embedding={EMBEDDING_MODEL}, GPT={GPT_MODEL}")
    print(f"🌐 API 文檔：http://localhost:8000/docs")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)
