
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hybrid Search API - 提供 BM25 + 語義搜尋混合排序的 API 服務
"""

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Union
import json
import asyncio
import aiohttp
import logging
from datetime import datetime
import os
from dataclasses import dataclass
import numpy as np

# 設定日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ERP Hybrid Search API", version="1.0.0")

# 配置
ES_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
ES_USER = os.getenv("ELASTICSEARCH_USER", "elastic")
ES_PASS = os.getenv("ELASTICSEARCH_PASSWORD", "admin@12345")
EMBEDDING_API_URL = os.getenv("EMBEDDING_API_URL", "")  # 可選的向量化服務

class SearchRequest(BaseModel):
    query: str
    filters: Optional[Dict[str, Any]] = None
    size: int = 10
    use_vector: bool = False
    boost_recent: bool = True

class SearchResponse(BaseModel):
    took: int
    total: int
    max_score: float
    hits: List[Dict[str, Any]]
    aggregations: Optional[Dict[str, Any]] = None

@dataclass
class RRFConfig:
    """Reciprocal Rank Fusion 配置"""
    k: int = 60  # RRF 常數
    bm25_weight: float = 0.7
    vector_weight: float = 0.3

class HybridSearchEngine:
    """混合搜尋引擎"""
    
    def __init__(self):
        self.session = None
        self.rrf_config = RRFConfig()
    
    async def get_session(self):
        """取得 HTTP session"""
        if self.session is None:
            auth = aiohttp.BasicAuth(ES_USER, ES_PASS)
            self.session = aiohttp.ClientSession(
                auth=auth,
                headers={"Content-Type": "application/json"}
            )
        return self.session
    
    async def close(self):
        """關閉 session"""
        if self.session:
            await self.session.close()
    
    async def bm25_search(self, query: str, filters: Dict, size: int = 50) -> Dict[str, Any]:
        """BM25 文本搜尋"""
        search_query = {
            "size": size,
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": [
                                    "supplier_name.text^3",
                                    "customer_name.text^3",
                                    "employee_name^2",
                                    "department_name.text^2",
                                    "position^2",
                                    "searchable_content^1"
                                ],
                                "type": "best_fields",
                                "fuzziness": "AUTO"
                            }
                        }
                    ],
                    "filter": self._build_filters(filters)
                }
            },
            "sort": [
                {"_score": {"order": "desc"}},
                {"@timestamp": {"order": "desc"}}
            ],
            "_source": {
                "excludes": ["searchable_content"]
            }
        }
        
        session = await self.get_session()
        async with session.post(f"{ES_URL}/erp_*/_search", json=search_query) as response:
            if response.status != 200:
                raise HTTPException(status_code=500, detail="BM25 search failed")
            return await response.json()
    
    async def vector_search(self, query: str, filters: Dict, size: int = 50) -> Dict[str, Any]:
        """語義向量搜尋 (需要向量化服務)"""
        if not EMBEDDING_API_URL:
            return {"hits": {"hits": []}}
        
        try:
            # 取得查詢向量
            query_vector = await self._get_query_embedding(query)
            if not query_vector:
                return {"hits": {"hits": []}}
            
            # kNN 搜尋
            knn_query = {
                "size": size,
                "knn": {
                    "field": "content_vector",
                    "query_vector": query_vector,
                    "k": size,
                    "num_candidates": size * 3,
                    "filter": {
                        "bool": {
                            "filter": self._build_filters(filters)
                        }
                    }
                },
                "_source": {
                    "excludes": ["content_vector", "searchable_content"]
                }
            }
            
            session = await self.get_session()
            async with session.post(f"{ES_URL}/erp_*/_search", json=knn_query) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning("Vector search failed, falling back to BM25 only")
                    return {"hits": {"hits": []}}
                    
        except Exception as e:
            logger.error(f"Vector search error: {e}")
            return {"hits": {"hits": []}}
    
    async def hybrid_search(self, request: SearchRequest) -> SearchResponse:
        """混合搜尋 (BM25 + Vector + RRF)"""
        start_time = datetime.now()
        
        # 並行執行 BM25 和向量搜尋
        tasks = [
            self.bm25_search(request.query, request.filters or {}, request.size * 2)
        ]
        
        if request.use_vector and EMBEDDING_API_URL:
            tasks.append(self.vector_search(request.query, request.filters or {}, request.size * 2))
        
        try:
            results = await asyncio.gather(*tasks)
            bm25_result = results[0]
            vector_result = results[1] if len(results) > 1 else {"hits": {"hits": []}}
            
            # RRF 融合排序
            if request.use_vector and vector_result["hits"]["hits"]:
                final_hits = self._rrf_fusion(
                    bm25_result["hits"]["hits"],
                    vector_result["hits"]["hits"],
                    request.size
                )
            else:
                final_hits = bm25_result["hits"]["hits"][:request.size]
            
            # 時間加權 (可選)
            if request.boost_recent:
                final_hits = self._apply_recency_boost(final_hits)
            
            # 計算執行時間
            took = int((datetime.now() - start_time).total_seconds() * 1000)
            
            return SearchResponse(
                took=took,
                total=bm25_result["hits"]["total"]["value"],
                max_score=final_hits[0]["_score"] if final_hits else 0.0,
                hits=final_hits,
                aggregations=bm25_result.get("aggregations")
            )
            
        except Exception as e:
            logger.error(f"Hybrid search error: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    def _build_filters(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """建構過濾條件"""
        filter_clauses = []
        
        if not filters:
            return filter_clauses
        
        # 表格篩選
        if "table_name" in filters:
            filter_clauses.append({
                "term": {"table_name": filters["table_name"]}
            })
        
        # 日期範圍篩選
        if "date_range" in filters:
            date_filter = filters["date_range"]
            range_query = {}
            if "gte" in date_filter:
                range_query["gte"] = date_filter["gte"]
            if "lte" in date_filter:
                range_query["lte"] = date_filter["lte"]
            
            if range_query:
                filter_clauses.append({
                    "range": {"@timestamp": range_query}
                })
        
        # 狀態篩選
        if "status" in filters:
            filter_clauses.append({
                "term": {"status": filters["status"]}
            })
        
        # 金額範圍
        if "amount_range" in filters:
            amount_filter = filters["amount_range"]
            range_query = {}
            if "gte" in amount_filter:
                range_query["gte"] = amount_filter["gte"]
            if "lte" in amount_filter:
                range_query["lte"] = amount_filter["lte"]
            
            if range_query:
                filter_clauses.append({
                    "range": {"total_amount": range_query}
                })
        
        return filter_clauses
    
    def _rrf_fusion(self, bm25_hits: List, vector_hits: List, final_size: int) -> List[Dict]:
        """Reciprocal Rank Fusion 融合排序"""
        
        # 建立文檔 ID 到結果的映射
        doc_scores = {}
        
        # 處理 BM25 結果
        for rank, hit in enumerate(bm25_hits):
            doc_id = hit["_id"]
            rrf_score = self.rrf_config.bm25_weight / (self.rrf_config.k + rank + 1)
            doc_scores[doc_id] = {
                "hit": hit,
                "bm25_score": hit["_score"],
                "bm25_rank": rank + 1,
                "rrf_score": rrf_score,
                "vector_score": 0.0,
                "vector_rank": 0
            }
        
        # 處理向量結果
        for rank, hit in enumerate(vector_hits):
            doc_id = hit["_id"]
            rrf_score = self.rrf_config.vector_weight / (self.rrf_config.k + rank + 1)
            
            if doc_id in doc_scores:
                # 文檔同時出現在兩個結果中
                doc_scores[doc_id]["rrf_score"] += rrf_score
                doc_scores[doc_id]["vector_score"] = hit["_score"]
                doc_scores[doc_id]["vector_rank"] = rank + 1
            else:
                # 只在向量搜尋中出現
                doc_scores[doc_id] = {
                    "hit": hit,
                    "bm25_score": 0.0,
                    "bm25_rank": 0,
                    "rrf_score": rrf_score,
                    "vector_score": hit["_score"],
                    "vector_rank": rank + 1
                }
        
        # 按 RRF 分數排序
        sorted_docs = sorted(doc_scores.values(), key=lambda x: x["rrf_score"], reverse=True)
        
        # 組合最終結果
        final_hits = []
        for doc_info in sorted_docs[:final_size]:
            hit = doc_info["hit"].copy()
            hit["_score"] = doc_info["rrf_score"]
            
            # 添加調試資訊
            hit["_explanation"] = {
                "rrf_score": doc_info["rrf_score"],
                "bm25_score": doc_info["bm25_score"],
                "bm25_rank": doc_info["bm25_rank"],
                "vector_score": doc_info["vector_score"],
                "vector_rank": doc_info["vector_rank"]
            }
            
            final_hits.append(hit)
        
        return final_hits
    
    def _apply_recency_boost(self, hits: List[Dict]) -> List[Dict]:
        """應用時間新近性加權"""
        now = datetime.now()
        
        for hit in hits:
            timestamp_str = hit["_source"].get("@timestamp")
            if timestamp_str:
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    days_old = (now - timestamp).days
                    
                    # 時間衰減函數：越新的文檔加權越高
                    recency_boost = 1.0 / (1.0 + days_old * 0.01)
                    hit["_score"] *= recency_boost
                    
                except Exception:
                    pass  # 無法解析時間就不加權
        
        # 重新排序
        hits.sort(key=lambda x: x["_score"], reverse=True)
        return hits
    
    async def _get_query_embedding(self, query: str) -> Optional[List[float]]:
        """取得查詢文本的向量表示"""
        try:
            async with aiohttp.ClientSession() as session:
                payload = {"text": query}
                async with session.post(EMBEDDING_API_URL, json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get("embedding")
        except Exception as e:
            logger.error(f"Embedding API error: {e}")
        return None

# 全域搜尋引擎實例
search_engine = HybridSearchEngine()

@app.on_event("startup")
async def startup_event():
    """應用啟動事件"""
    logger.info("Hybrid Search API 啟動")

@app.on_event("shutdown")
async def shutdown_event():
    """應用關閉事件"""
    await search_engine.close()
    logger.info("Hybrid Search API 關閉")

@app.get("/health")
async def health_check():
    """健康檢查"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.post("/search", response_model=SearchResponse)
async def search(request: SearchRequest):
    """主要搜尋端點"""
    if not request.query:
        raise HTTPException(status_code=400, detail="查詢字串不能為空")
    
    return await search_engine.hybrid_search(request)

@app.get("/search")
async def search_get(
    q: str = Query(..., description="搜尋關鍵字"),
    size: int = Query(10, ge=1, le=100, description="回傳結果數量"),
    table: Optional[str] = Query(None, description="表格篩選"),
    use_vector: bool = Query(False, description="是否使用向量搜尋"),
    boost_recent: bool = Query(True, description="是否加權最近的結果")
):
    """GET 方式的搜尋端點"""
    filters = {}
    if table:
        filters["table_name"] = table
    
    request = SearchRequest(
        query=q,
        filters=filters,
        size=size,
        use_vector=use_vector,
        boost_recent=boost_recent
    )
    
    return await search_engine.hybrid_search(request)

@app.get("/suggestions")
async def get_suggestions(
    q: str = Query(..., description="搜尋前綴"),
    field: str = Query("supplier_name", description="建議欄位")
):
    """搜尋建議 API"""
    suggest_query = {
        "suggest": {
            "text": q,
            "suggestions": {
                "completion": {
                    "field": f"{field}.suggest",
                    "size": 10
                }
            }
        }
    }
    
    session = await search_engine.get_session()
    async with session.post(f"{ES_URL}/erp_*/_search", json=suggest_query) as response:
        if response.status == 200:
            result = await response.json()
            suggestions = []
            for suggestion in result.get("suggest", {}).get("suggestions", []):
                for option in suggestion.get("options", []):
                    suggestions.append(option["text"])
            return {"suggestions": suggestions}
        else:
            raise HTTPException(status_code=500, detail="建議查詢失敗")

@app.get("/aggregations")
async def get_aggregations(
    field: str = Query("table_name", description="聚合欄位")
):
    """取得欄位聚合統計"""
    agg_query = {
        "size": 0,
        "aggs": {
            "field_stats": {
                "terms": {
                    "field": field,
                    "size": 20
                }
            }
        }
    }
    
    session = await search_engine.get_session()
    async with session.post(f"{ES_URL}/erp_*/_search", json=agg_query) as response:
        if response.status == 200:
            result = await response.json()
            return result.get("aggregations", {})
        else:
            raise HTTPException(status_code=500, detail="聚合查詢失敗")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
