#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
改進的 RAG API 服務
配合實際的 Elasticsearch 資料結構
修正向量欄位名稱為 content_vector
"""

import os, json, requests, uvicorn, logging, time, re
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from openai import OpenAI
from opencc import OpenCC

# ============================================================================
# 配置管理模組
# ============================================================================

@dataclass
class Config:
    """系統配置類別"""
    
    # Elasticsearch 配置
    es_url: str = os.environ.get("ES_URL", "http://localhost:9200")
    es_user: str = os.environ.get("ES_USER", "elastic")
    es_pass: str = os.environ.get("ES_PASS", "admin@12345")
    
    # OpenAI 配置
    openai_api_key: str = os.getenv("OPENAI_API_KEY")
    openai_base_url: str = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
    embedding_model: str = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
    gpt_model: str = os.environ.get("GPT_MODEL", "gpt-4o-mini")
    
    # API 服務配置
    api_host: str = "0.0.0.0"
    api_port: int = 8010
    api_title: str = "RAG 檢索 API"
    api_version: str = "2.1.0"
    
    # 搜尋配置
    default_index_pattern: str = "erp-*"
    default_top_k: int = 5
    default_batch_size: int = 100
    
    # 請求超時設定（秒）
    request_timeout: int = 30
    
    def validate(self) -> bool:
        """驗證必要配置是否存在"""
        if not self.openai_api_key:
            logging.warning("⚠️ 未設置 OPENAI_API_KEY，GPT 功能將無法使用")
            return False
        return True

# ============================================================================
# 日誌設定
# ============================================================================

def setup_logging():
    """設定日誌格式和等級"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)

# ============================================================================
# 資料模型定義
# ============================================================================

class SearchMode(str, Enum):
    """搜尋模式列舉"""
    KEYWORD = "keyword"    # 關鍵字搜尋
    VECTOR = "vector"      # 向量搜尋
    HYBRID = "hybrid"      # 混合搜尋

class QueryRequest(BaseModel):
    """查詢請求模型"""
    query: str = Field(..., description="查詢字串", min_length=1, max_length=1000)
    mode: SearchMode = Field(default=SearchMode.HYBRID, description="搜尋模式")
    top_k: int = Field(default=5, ge=1, le=100, description="返回結果數量")
    index_pattern: str = Field(default="erp-*", description="索引模式")
    use_gpt: bool = Field(default=True, description="是否使用 GPT 生成答案")
    temperature: float = Field(default=0.7, ge=0, le=2, description="GPT 生成溫度")
    convert_to_traditional: bool = Field(default=True, description="是否將簡體查詢轉為繁體")

class SearchResult(BaseModel):
    """單筆搜尋結果"""
    score: float = Field(..., description="相關性分數")
    index: str = Field(..., description="來源索引")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="元資料")
    content: str = Field(..., description="內容摘要")
    highlights: Dict[str, List[str]] = Field(default_factory=dict, description="高亮片段")

class QueryResponse(BaseModel):
    """查詢回應模型"""
    query: str = Field(..., description="原始查詢")
    processed_query: str = Field(..., description="處理後的查詢（繁體）")
    answer: Optional[str] = Field(None, description="GPT 生成的答案")
    sources: List[SearchResult] = Field(default_factory=list, description="搜尋結果來源")
    search_mode: str = Field(..., description="使用的搜尋模式")
    total_hits: int = Field(0, description="總命中數")
    processing_time_ms: int = Field(..., description="處理時間（毫秒）")

class HealthResponse(BaseModel):
    """健康檢查回應"""
    status: str
    elasticsearch: bool
    openai: bool
    timestamp: str

# ============================================================================
# 文字處理工具
# ============================================================================

class TextProcessor:
    """文字處理工具類別"""
    
    def __init__(self):
        try:
            self.s2t_converter = OpenCC('s2t')  # 簡體轉繁體
            self.t2s_converter = OpenCC('t2s')  # 繁體轉簡體
        except Exception as e:
            logging.warning(f"OpenCC 初始化失敗: {e}")
            self.s2t_converter = None
            self.t2s_converter = None
    
    def prepare_search_query(self, query: str) -> Tuple[str, List[str]]:
        """準備搜尋查詢變體"""
        queries = [query.strip()]
        
        # 添加繁簡轉換
        if self.s2t_converter:
            traditional = self.s2t_converter.convert(query)
            if traditional != query:
                queries.append(traditional)
        
        if self.t2s_converter:
            simplified = self.t2s_converter.convert(query)
            if simplified != query:
                queries.append(simplified)
        
        # 使用繁體作為主要查詢
        main_query = queries[1] if len(queries) > 1 and self.s2t_converter else query
        
        return main_query, queries

# ============================================================================
# 向量生成器
# ============================================================================

class VectorGenerator:
    """向量生成器 - 整合 OpenAI Embeddings API"""
    
    def __init__(self, config: Config):
        self.config = config
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)
        
        if config.openai_api_key:
            try:
                self.client = OpenAI(
                    api_key=config.openai_api_key,
                    base_url=config.openai_base_url,
                    timeout=30
                )
                self.logger.info(f"✅ 向量生成器初始化成功，模型: {config.embedding_model}")
            except Exception as e:
                self.logger.error(f"❌ 向量生成器初始化失敗: {e}")
    
    def generate(self, text: str) -> Optional[List[float]]:
        """生成單個文本的向量"""
        if not self.client:
            return None
        
        try:
            # 限制文本長度（OpenAI token 限制）
            text = text[:8000]
            
            response = self.client.embeddings.create(
                model=self.config.embedding_model,
                input=text,
                encoding_format="float"
            )
            return response.data[0].embedding
        except Exception as e:
            self.logger.error(f"向量生成失敗: {e}")
            return None
    
    def health_check(self) -> bool:
        """檢查 OpenAI API 是否可用"""
        if not self.client:
            return False
        
        try:
            # 嘗試生成一個簡單的測試向量
            self.generate("test")
            return True
        except:
            return False

# ============================================================================
# Elasticsearch 客戶端
# ============================================================================

class ElasticsearchClient:
    """Elasticsearch 客戶端管理"""
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = self._create_session()
    
    def _create_session(self):
        """建立 requests session 並配置認證"""
        session = requests.Session()
        session.auth = (self.config.es_user, self.config.es_pass)
        session.headers.update({"Content-Type": "application/json"})
        return session
    
    def search(self, index_pattern: str, query_body: Dict[str, Any]) -> Dict[str, Any]:
        """執行搜尋請求"""
        try:
            self.logger.info(f"執行搜尋請求: {index_pattern}")
            self.logger.debug(f"查詢體: {json.dumps(query_body, indent=2)}")
            
            response = self.session.post(
                f"{self.config.es_url}/{index_pattern}/_search",
                json=query_body,
                timeout=self.config.request_timeout,
            )
            response.raise_for_status()
            result = response.json()
            
            self.logger.info(f"搜尋結果: {result.get('hits', {}).get('total', {}).get('value', 0)} 筆")
            return result
        except requests.exceptions.RequestException as e:
            self.logger.error(f"搜尋請求失敗: {e}")
            return {"hits": {"hits": [], "total": {"value": 0}}}
    
    def health_check(self) -> bool:
        """檢查 Elasticsearch 健康狀態"""
        try:
            response = self.session.get(f"{self.config.es_url}/_cluster/health", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def get_stats(self, index_pattern: str = "erp-*") -> Dict[str, Any]:
        """取得索引統計資訊"""
        stats = {}
        try:
            # 取得索引統計
            response = self.session.get(
                f"{self.config.es_url}/{index_pattern}/_stats",
                timeout=self.config.request_timeout,
            )
            if response.status_code == 200:
                data = response.json()
                total_docs = sum(
                    idx["primaries"]["docs"]["count"]
                    for idx in data["indices"].values()
                )
                total_size = sum(
                    idx["primaries"]["store"]["size_in_bytes"]
                    for idx in data["indices"].values()
                )
                
                stats["indices"] = {
                    "count": len(data["indices"]),
                    "total_documents": total_docs,
                    "total_size_mb": round(total_size / 1024 / 1024, 2),
                }
            
            # 檢查向量化進度 - 使用正確的欄位名稱 "content_vector"
            vector_query = {"size": 0, "query": {"exists": {"field": "content_vector"}}}
            response = self.session.post(
                f"{self.config.es_url}/{index_pattern}/_count",
                json=vector_query,
                timeout=self.config.request_timeout,
            )
            if response.status_code == 200:
                stats["vectorized_documents"] = response.json().get("count", 0)
        except Exception as e:
            self.logger.error(f"取得統計資訊失敗: {e}")
        
        return stats

# ============================================================================
# 搜尋引擎
# ============================================================================

class SearchEngine:
    """搜尋引擎 - 實現關鍵字、向量和混合搜尋"""
    
    def __init__(self, es_client: ElasticsearchClient, vector_gen: VectorGenerator, text_processor: TextProcessor):
        self.es_client = es_client
        self.vector_gen = vector_gen
        self.text_processor = text_processor
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def keyword_search(self, query: str, index_pattern: str, size: int = 5) -> Dict[str, Any]:
        """執行關鍵字搜尋 - 針對實際資料結構優化"""
        
        # 準備查詢變體（繁簡體）
        main_query, query_variants = self.text_processor.prepare_search_query(query)
        
        # 檢測是否為產品編號查詢（P或W開頭加數字）
        product_id_pattern = re.compile(r"^[PW]\d{3}$")
        is_product_id_query = bool(product_id_pattern.match(query.strip().upper()))
        
        # 構建查詢 DSL - 針對實際資料結構
        search_body = {
            "size": size,
            "_source": {"excludes": ["content_vector"]},  # 排除向量欄位
            "query": {
                "bool": {
                    "should": []
                }
            },
            "highlight": {
                "fields": {
                    # 產品相關欄位
                    "product_name": {"fragment_size": 150, "number_of_fragments": 2},
                    "product_id": {"fragment_size": 50, "number_of_fragments": 1},
                    "product_model": {"fragment_size": 100, "number_of_fragments": 1},
                    "category": {"fragment_size": 50, "number_of_fragments": 1},
                    "supplier": {"fragment_size": 50, "number_of_fragments": 1},
                    
                    # 客訴相關欄位
                    "customer_name": {"fragment_size": 50, "number_of_fragments": 1},
                    "issue_description": {"fragment_size": 150, "number_of_fragments": 2},
                    "solution": {"fragment_size": 150, "number_of_fragments": 2},
                    "complaint_id": {"fragment_size": 50, "number_of_fragments": 1},
                    
                    # 文件相關欄位
                    "doc_number": {"fragment_size": 50, "number_of_fragments": 1},
                    "file_name": {"fragment_size": 100, "number_of_fragments": 1},
                    "summary": {"fragment_size": 150, "number_of_fragments": 2},
                    "applicant": {"fragment_size": 50, "number_of_fragments": 1},
                    "department": {"fragment_size": 50, "number_of_fragments": 1}
                }
            }
        }
        
        # 為每個查詢變體添加搜尋條件
        for q_variant in query_variants:
            if is_product_id_query:
                # 產品ID精確搜尋
                search_body["query"]["bool"]["should"].extend([
                    {"term": {"product_id.keyword": {"value": q_variant.upper(), "boost": 3.0}}},
                    {"term": {"product_id": {"value": q_variant.upper(), "boost": 3.0}}},
                ])
            else:
                # 一般文本搜尋
                search_body["query"]["bool"]["should"].extend([
                    # 高優先級欄位
                    {"match": {"product_name": {"query": q_variant, "boost": 2.5, "operator": "and"}}},
                    {"match": {"product_id": {"query": q_variant, "boost": 2.0}}},
                    {"match": {"product_model": {"query": q_variant, "boost": 2.0}}},
                    {"match": {"complaint_id": {"query": q_variant, "boost": 2.0}}},
                    {"match": {"doc_number": {"query": q_variant, "boost": 2.0}}},
                    
                    # 中等優先級欄位
                    {"match": {"category": {"query": q_variant, "boost": 1.5}}},
                    {"match": {"supplier": {"query": q_variant, "boost": 1.5}}},
                    {"match": {"issue_description": {"query": q_variant, "boost": 1.5}}},
                    {"match": {"solution": {"query": q_variant, "boost": 1.5}}},
                    {"match": {"summary": {"query": q_variant, "boost": 1.5}}},
                    {"match": {"file_name": {"query": q_variant, "boost": 1.3}}},
                    
                    # 一般優先級欄位
                    {"match": {"customer_name": {"query": q_variant, "boost": 1.0}}},
                    {"match": {"applicant": {"query": q_variant, "boost": 1.0}}},
                    {"match": {"department": {"query": q_variant, "boost": 1.0}}},
                    {"match": {"status": {"query": q_variant, "boost": 1.0}}},
                ])
        
        # 如果沒有搜尋條件，使用 match_all
        if not search_body["query"]["bool"]["should"]:
            search_body["query"] = {"match_all": {}}
        
        self.logger.info(f"執行關鍵字搜尋: {query} (產品編號查詢: {is_product_id_query})")
        result = self.es_client.search(index_pattern, search_body)
        
        return result
    
    def vector_search(self, query: str, index_pattern: str, size: int = 5) -> Dict[str, Any]:
        """執行向量搜尋 - 使用正確的 content_vector 欄位"""
        
        # 生成查詢向量
        query_vector = self.vector_gen.generate(query)
        if not query_vector:
            self.logger.warning("向量生成失敗，返回空結果")
            return {"hits": {"hits": [], "total": {"value": 0}}}
        
        # 構建 KNN 查詢 - 使用正確的欄位名稱
        search_body = {
            "size": size,
            "_source": {"excludes": ["content_vector"]},  # 排除向量欄位
            "knn": {
                "field": "content_vector",  # 使用正確的欄位名稱 "content_vector"
                "query_vector": query_vector,
                "k": size,
                "num_candidates": size * 10,
            },
        }
        
        self.logger.info(f"執行向量搜尋: {query}")
        return self.es_client.search(index_pattern, search_body)
    
    def hybrid_search(self, query: str, index_pattern: str, size: int = 5) -> Dict[str, Any]:
        """執行混合搜尋 - 結合關鍵字和向量搜尋"""
        
        # 生成查詢向量
        query_vector = self.vector_gen.generate(query)
        if not query_vector:
            self.logger.warning("向量生成失敗，回退到關鍵字搜尋")
            return self.keyword_search(query, index_pattern, size)
        
        # 準備查詢變體
        main_query, query_variants = self.text_processor.prepare_search_query(query)
        
        # 構建混合搜尋查詢
        search_body = {
            "size": size,
            "_source": {"excludes": ["content_vector"]},
            "query": {
                "bool": {
                    "should": []
                }
            },
            "knn": {
                "field": "content_vector",  # 使用正確的欄位名稱
                "query_vector": query_vector,
                "k": size,
                "num_candidates": size * 5,
                "boost": 0.5  # 向量搜尋權重
            },
            "highlight": {
                "fields": {
                    "product_name": {"fragment_size": 150, "number_of_fragments": 2},
                    "product_model": {"fragment_size": 100, "number_of_fragments": 1},
                    "category": {"fragment_size": 50, "number_of_fragments": 1},
                    "supplier": {"fragment_size": 50, "number_of_fragments": 1},
                    "issue_description": {"fragment_size": 150, "number_of_fragments": 2},
                    "solution": {"fragment_size": 150, "number_of_fragments": 2},
                    "summary": {"fragment_size": 150, "number_of_fragments": 2},
                }
            }
        }
        
        # 添加文本匹配條件
        for q_variant in query_variants:
            search_body["query"]["bool"]["should"].extend([
                {"match": {"product_name": {"query": q_variant, "boost": 1.5}}},
                {"match": {"product_model": {"query": q_variant, "boost": 1.3}}},
                {"match": {"category": {"query": q_variant, "boost": 1.2}}},
                {"match": {"supplier": {"query": q_variant, "boost": 1.2}}},
                {"match": {"issue_description": {"query": q_variant, "boost": 1.2}}},
                {"match": {"solution": {"query": q_variant, "boost": 1.2}}},
                {"match": {"summary": {"query": q_variant, "boost": 1.2}}},
                {"match": {"file_name": {"query": q_variant, "boost": 1.0}}},
            ])
        
        self.logger.info(f"執行混合搜尋: {query}")
        return self.es_client.search(index_pattern, search_body)

# ============================================================================
# GPT 答案生成器
# ============================================================================

class GPTAnswerGenerator:
    """GPT 答案生成器"""
    
    def __init__(self, config: Config):
        self.config = config
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)
        
        if config.openai_api_key:
            try:
                self.client = OpenAI(
                    api_key=config.openai_api_key,
                    base_url=config.openai_base_url,
                    timeout=30
                )
            except Exception as e:
                self.logger.error(f"GPT 客戶端初始化失敗: {e}")
    
    def generate_answer(self, query: str, search_results: List[Dict], temperature: float = 0.7) -> Optional[str]:
        """根據搜尋結果生成答案"""
        if not self.client or not search_results:
            return None
        
        try:
            # 準備上下文
            context_parts = []
            for i, result in enumerate(search_results[:5], 1):
                source = result.get("_source", {})
                
                # 根據不同類型的文檔構建上下文
                if "product_id" in source:  # 產品資料
                    context = f"產品{i}: {source.get('product_name', '')} ({source.get('product_id', '')})\n"
                    context += f"型號: {source.get('product_model', '')}\n"
                    context += f"分類: {source.get('category', '')}\n"
                    context += f"供應商: {source.get('supplier', '')}\n"
                    context += f"價格: {source.get('price', '')}\n"
                    context += f"庫存: {source.get('stock_qty', '')}\n"
                elif "complaint_id" in source:  # 客訴資料
                    context = f"客訴{i}: {source.get('complaint_id', '')}\n"
                    context += f"客戶: {source.get('customer_name', '')}\n"
                    context += f"問題: {source.get('issue_description', '')}\n"
                    context += f"解決方案: {source.get('solution', '')}\n"
                    context += f"狀態: {source.get('status', '')}\n"
                elif "doc_number" in source:  # 文件資料
                    context = f"文件{i}: {source.get('doc_number', '')}\n"
                    context += f"類型: {source.get('doc_type', '')}\n"
                    context += f"摘要: {source.get('summary', '')}\n"
                    context += f"申請人: {source.get('applicant', '')}\n"
                    context += f"部門: {source.get('department', '')}\n"
                else:  # 通用處理
                    context = f"資料{i}: "
                    for key, value in source.items():
                        if key not in ["content_vector", "vector_generated_at"] and value:
                            context += f"{key}: {value}\n"
                
                context_parts.append(context.strip())
            
            context = "\n\n".join(context_parts)
            
            # 構建提示
            prompt = f"""基於以下搜尋結果，請用繁體中文回答用戶的問題。

用戶問題: {query}

搜尋結果:
{context}

請提供準確、有用的答案，如果搜尋結果中沒有相關資訊，請誠實說明。"""
            
            response = self.client.chat.completions.create(
                model=self.config.gpt_model,
                messages=[
                    {"role": "system", "content": "你是一個專業的企業資料檢索助手，負責分析搜尋結果並提供準確的答案。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=temperature,
                max_tokens=1000,
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            self.logger.error(f"GPT 答案生成失敗: {e}")
            return None

# ============================================================================
# RAG 服務主類別
# ============================================================================

class RAGService:
    """RAG 服務主類別 - 整合所有功能"""
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 初始化各個組件
        self.text_processor = TextProcessor()
        self.es_client = ElasticsearchClient(config)
        self.vector_gen = VectorGenerator(config)
        self.search_engine = SearchEngine(self.es_client, self.vector_gen, self.text_processor)
        self.gpt_generator = GPTAnswerGenerator(config)
    
    def process_query(self, request: QueryRequest) -> QueryResponse:
        """處理查詢請求"""
        start_time = time.time()
        
        # 準備查詢
        main_query, _ = self.text_processor.prepare_search_query(request.query)
        
        # 執行搜尋
        if request.mode == SearchMode.KEYWORD:
            search_result = self.search_engine.keyword_search(
                request.query, request.index_pattern, request.top_k
            )
        elif request.mode == SearchMode.VECTOR:
            search_result = self.search_engine.vector_search(
                request.query, request.index_pattern, request.top_k
            )
        else:  # HYBRID
            search_result = self.search_engine.hybrid_search(
                request.query, request.index_pattern, request.top_k
            )
        
        # 處理搜尋結果
        sources = self._process_search_results(search_result)
        
        # 生成 GPT 答案
        answer = None
        if request.use_gpt and sources:
            answer = self.gpt_generator.generate_answer(
                request.query, search_result.get("hits", {}).get("hits", []), request.temperature
            )
        
        # 計算處理時間
        processing_time = int((time.time() - start_time) * 1000)
        
        return QueryResponse(
            query=request.query,
            processed_query=main_query,
            answer=answer,
            sources=sources,
            search_mode=request.mode,
            total_hits=search_result.get("hits", {}).get("total", {}).get("value", 0),
            processing_time_ms=processing_time,
        )
    
    def _process_search_results(self, search_result: Dict) -> List[SearchResult]:
        """處理搜尋結果"""
        sources = []
        
        for hit in search_result.get("hits", {}).get("hits", []):
            source = hit.get("_source", {})
            highlights = hit.get("highlight", {})
            
            # 生成內容摘要
            content = self._generate_content_summary(source)
            
            # 提取元資料
            metadata = {
                "id": hit.get("_id"),
                "type": self._detect_document_type(source),
                "index": hit.get("_index"),
            }
            
            sources.append(SearchResult(
                score=hit.get("_score", 0),
                index=hit.get("_index", ""),
                metadata=metadata,
                content=content,
                highlights=highlights
            ))
        
        return sources
    
    def _generate_content_summary(self, source: Dict) -> str:
        """生成內容摘要"""
        if "product_id" in source:  # 產品資料
            parts = [
                f"產品: {source.get('product_name', '')}",
                f"代碼: {source.get('product_id', '')}",
                f"型號: {source.get('product_model', '')}",
                f"分類: {source.get('category', '')}",
                f"供應商: {source.get('supplier', '')}",
                f"價格: {source.get('price', '')}",
                f"庫存: {source.get('stock_qty', '')}",
            ]
        elif "complaint_id" in source:  # 客訴資料
            parts = [
                f"客訴編號: {source.get('complaint_id', '')}",
                f"客戶: {source.get('customer_name', '')}",
                f"狀態: {source.get('status', '')}",
            ]
            if source.get('issue_description'):
                parts.append(f"問題: {source['issue_description'][:100]}...")
        elif "doc_number" in source:  # 文件資料
            parts = [
                f"文件: {source.get('doc_number', '')}",
                f"類型: {source.get('doc_type', '')}",
                f"申請人: {source.get('applicant', '')}",
            ]
            if source.get('summary'):
                parts.append(f"摘要: {source['summary'][:100]}...")
        else:  # 通用處理
            parts = []
            for key, value in source.items():
                if key not in ["content_vector", "vector_generated_at"] and value:
                    parts.append(f"{key}: {str(value)[:50]}")
                if len(parts) >= 4:
                    break
        
        return " | ".join(filter(None, parts))
    
    def _detect_document_type(self, source: Dict) -> str:
        """檢測文檔類型"""
        if "product_id" in source:
            return "product"
        elif "complaint_id" in source:
            return "complaint"
        elif "doc_number" in source:
            return "document"
        else:
            return "general"
    
    def health_check(self) -> HealthResponse:
        """執行健康檢查"""
        es_health = self.es_client.health_check()
        openai_health = self.vector_gen.health_check()
        
        status = "healthy" if (es_health and openai_health) else "degraded"
        if not es_health and not openai_health:
            status = "unhealthy"
        
        return HealthResponse(
            status=status,
            elasticsearch=es_health,
            openai=openai_health,
            timestamp=datetime.now().isoformat(),
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """取得系統統計資訊"""
        return self.es_client.get_stats(self.config.default_index_pattern)

# ============================================================================
# FastAPI 應用程式
# ============================================================================

# 初始化配置和日誌
config = Config()
logger = setup_logging()

# 建立 FastAPI 應用
app = FastAPI(
    title=config.api_title,
    version=config.api_version,
    description="智能檢索和問答系統 API - 針對實際資料結構優化",
    docs_url="/docs",
    redoc_url="/redoc",
)

# 配置 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生產環境應該設定具體的來源
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 初始化 RAG 服務
rag_service = None

@app.on_event("startup")
async def startup_event():
    """應用啟動事件"""
    global rag_service
    
    logger.info("🚀 正在啟動 RAG API 服務...")
    
    # 驗證配置
    if not config.validate():
        logger.warning("⚠️ 配置驗證失敗，部分功能可能無法使用")
    
    # 初始化 RAG 服務
    rag_service = RAGService(config)
    
    logger.info(f"📊 使用模型：Embedding={config.embedding_model}, GPT={config.gpt_model}")
    logger.info(f"🌐 API 文檔：http://{config.api_host}:{config.api_port}/docs")

# ============================================================================
# API 端點
# ============================================================================

@app.get("/", tags=["根目錄"])
async def root():
    """根目錄端點"""
    return {
        "message": "RAG API 服務運行中",
        "version": config.api_version,
        "docs": "/docs",
    }

@app.get("/health", response_model=HealthResponse, tags=["系統"])
async def health_check():
    """健康檢查端點"""
    if not rag_service:
        raise HTTPException(status_code=503, detail="服務尚未初始化")
    
    return rag_service.health_check()

@app.post("/query", response_model=QueryResponse, tags=["查詢"])
async def query_data(request: QueryRequest):
    """智能查詢端點 - 配合實際的 Elasticsearch 資料結構"""
    if not rag_service:
        raise HTTPException(status_code=503, detail="服務尚未初始化")
    
    try:
        return rag_service.process_query(request)
    except Exception as e:
        logger.error(f"查詢處理失敗: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"查詢處理失敗: {str(e)}")

@app.get("/stats", tags=["系統"])
async def get_statistics():
    """取得系統統計資訊"""
    if not rag_service:
        raise HTTPException(status_code=503, detail="服務尚未初始化")
    
    try:
        return rag_service.get_stats()
    except Exception as e:
        logger.error(f"取得統計資訊失敗: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"取得統計資訊失敗: {str(e)}")

# ============================================================================
# 錯誤處理
# ============================================================================

@app.exception_handler(ValueError)
async def value_error_handler(request, exc):
    """處理值錯誤"""
    logger.error(f"值錯誤: {exc}")
    return JSONResponse(status_code=400, content={"detail": f"無效的輸入: {str(exc)}"})

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """全域錯誤處理"""
    logger.error(f"未預期的錯誤: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "內部伺服器錯誤"})

# ============================================================================
# 主程式入口
# ============================================================================

def main():
    """主程式入口"""
    if not config.openai_api_key:
        logger.warning("⚠️ 警告：未設置 OPENAI_API_KEY，部分功能將無法使用")
    
    logger.info(f"🚀 啟動 RAG API 服務")
    logger.info(f"📊 使用模型配置：")
    logger.info(f"   - Embedding: {config.embedding_model}")
    logger.info(f"   - GPT: {config.gpt_model}")
    logger.info(f"🌐 API 文檔：http://{config.api_host}:{config.api_port}/docs")
    
    # 啟動 Uvicorn 伺服器
    uvicorn.run(
        app,
        host=config.api_host,
        port=config.api_port,
        log_level="info",
        reload=False,  # 生產環境設為 False
    )

if __name__ == "__main__":
    main()
