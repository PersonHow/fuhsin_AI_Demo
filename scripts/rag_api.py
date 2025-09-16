#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RAG (Retrieval-Augmented Generation) API 服務
提供智能問答介面，結合向量搜尋和 OpenAI GPT

主要功能：
1. 關鍵字搜尋 - 使用 IK 分詞器進行中文分詞
2. 向量搜尋 - 使用語意相似度搜尋
3. 混合搜尋 - 結合關鍵字與向量搜尋
4. GPT 答案生成 - 根據搜尋結果生成自然語言答案
5. 簡繁轉換 - 自動處理簡體繁體查詢

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
    openai_base_url: str = os.environ.get(
        "OPENAI_BASE_URL", "https://api.openai.com/v1"
    )
    embedding_model: str = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
    gpt_model: str = os.environ.get("GPT_MODEL", "gpt-4o-mini")

    # API 服務配置
    api_host: str = "0.0.0.0"
    api_port: int = 8010
    api_title: str = "RAG 檢索 API"
    api_version: str = "2.0.0"

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

    KEYWORD = "keyword"  # 關鍵字搜尋
    VECTOR = "vector"  # 向量搜尋
    HYBRID = "hybrid"  # 混合搜尋


class QueryRequest(BaseModel):
    """
    查詢請求模型
    定義 API 接收的查詢參數
    """

    query: str = Field(..., description="查詢字串", min_length=1, max_length=1000)
    mode: SearchMode = Field(default=SearchMode.HYBRID, description="搜尋模式")
    top_k: int = Field(default=5, ge=1, le=100, description="返回結果數量")
    index_pattern: str = Field(default="erp-*", description="索引模式")
    use_gpt: bool = Field(default=True, description="是否使用 GPT 生成答案")
    temperature: float = Field(default=0.7, ge=0, le=2, description="GPT 生成溫度")
    convert_to_traditional: bool = Field(
        default=True, description="是否將簡體查詢轉為繁體"
    )

    class Config:
        schema_extra = {
            "example": {
                "query": "產品退貨流程",
                "mode": "hybrid",
                "top_k": 5,
                "use_gpt": True,
                "temperature": 0.7,
            }
        }


class SearchResult(BaseModel):
    """單筆搜尋結果"""

    score: float = Field(..., description="相關性分數")
    index: str = Field(..., description="來源索引")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="元資料")
    content: str = Field(..., description="內容摘要")
    highlights: Dict[str, List[str]] = Field(
        default_factory=dict, description="高亮片段"
    )


class QueryResponse(BaseModel):
    """
    查詢回應模型
    定義 API 返回的結果格式
    """

    query: str = Field(..., description="原始查詢")
    processed_query: str = Field(..., description="處理後的查詢（繁體）")
    answer: Optional[str] = Field(None, description="GPT 生成的答案")
    sources: List[SearchResult] = Field(
        default_factory=list, description="搜尋結果來源"
    )
    search_mode: str = Field(..., description="使用的搜尋模式")
    total_hits: int = Field(0, description="總命中數")
    processing_time_ms: int = Field(..., description="處理時間（毫秒）")

    class Config:
        schema_extra = {
            "example": {
                "query": "產品退貨",
                "processed_query": "產品退貨",
                "answer": "根據查詢結果，產品退貨流程如下...",
                "sources": [],
                "search_mode": "hybrid",
                "total_hits": 10,
                "processing_time_ms": 250,
            }
        }


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
    """
    文字處理器
    負責簡繁轉換和文字清理
    """

    def __init__(self):
        """初始化簡繁轉換器"""
        self.s2t = OpenCC("s2t")  # 簡體轉繁體
        self.t2s = OpenCC("t2s")  # 繁體轉簡體
        self.logger = logging.getLogger(self.__class__.__name__)

    def to_traditional(self, text: str) -> str:
        """
        將文字轉換為繁體中文

        Args:
            text: 輸入文字

        Returns:
            繁體中文文字
        """
        try:
            return self.s2t.convert(text)
        except Exception as e:
            self.logger.error(f"簡轉繁失敗: {e}")
            return text

    def to_simplified(self, text: str) -> str:
        """
        將文字轉換為簡體中文

        Args:
            text: 輸入文字

        Returns:
            簡體中文文字
        """
        try:
            return self.t2s.convert(text)
        except Exception as e:
            self.logger.error(f"繁轉簡失敗: {e}")
            return text

    def prepare_search_query(
        self, query: str, convert_to_traditional: bool = True
    ) -> Tuple[str, List[str]]:
        """
        準備搜尋查詢，生成多種變體以提高召回率

        Args:
            query: 原始查詢
            convert_to_traditional: 是否轉為繁體

        Returns:
            (處理後的主查詢, 查詢變體列表)
        """
        # 清理查詢字串
        query = query.strip()

        # 生成查詢變體
        variants = [query]

        # 加入繁體版本
        traditional = self.to_traditional(query)
        if traditional != query:
            variants.append(traditional)

        # 加入簡體版本
        simplified = self.to_simplified(query)
        if simplified != query:
            variants.append(simplified)

        # 決定主查詢
        main_query = traditional if convert_to_traditional else query

        return main_query, list(set(variants))


# ============================================================================
# Elasticsearch 客戶端
# ============================================================================


class ElasticsearchClient:
    """
    Elasticsearch 客戶端封裝
    提供搜尋和索引管理功能
    """

    def __init__(self, config: Config):
        """
        初始化 ES 客戶端

        Args:
            config: 系統配置
        """
        self.config = config
        self.session = self._create_session()
        self.logger = logging.getLogger(self.__class__.__name__)

    def _create_session(self) -> requests.Session:
        """建立 HTTP Session 並配置認證"""
        session = requests.Session()
        session.auth = (self.config.es_user, self.config.es_pass)
        session.headers.update({"Content-Type": "application/json"})
        return session

    def search(self, index_pattern: str, query_body: Dict[str, Any]) -> Dict[str, Any]:
        """
        執行搜尋請求

        Args:
            index_pattern: 索引模式
            query_body: ES 查詢 DSL

        Returns:
            搜尋結果
        """
        try:
            response = self.session.post(
                f"{self.config.es_url}/{index_pattern}/_search",
                json=query_body,
                timeout=self.config.request_timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"搜尋請求失敗: {e}")
            return {"hits": {"hits": [], "total": {"value": 0}}}

    def health_check(self) -> bool:
        """
        檢查 Elasticsearch 健康狀態

        Returns:
            True 如果健康，否則 False
        """
        try:
            response = self.session.get(
                f"{self.config.es_url}/_cluster/health", timeout=5
            )
            return response.status_code == 200
        except:
            return False

    def get_stats(self, index_pattern: str = "erp-*") -> Dict[str, Any]:
        """
        取得索引統計資訊

        Args:
            index_pattern: 索引模式

        Returns:
            統計資訊
        """
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

            # 檢查向量化進度
            vector_query = {"size": 0, "query": {"exists": {"field": "content_vector"}}}
            response = self.session.post(
                f"{self.config.es_url}/{index_pattern}/_count",
                json=vector_query,
                timeout=self.config.request_timeout,
            )
            if response.status_code == 200:
                with_vector = response.json()["count"]

                # 取得總文檔數
                total_response = self.session.post(
                    f"{self.config.es_url}/{index_pattern}/_count",
                    json={"query": {"match_all": {}}},
                    timeout=self.config.request_timeout,
                )
                if total_response.status_code == 200:
                    total = total_response.json()["count"]
                    stats["vectorization"] = {
                        "completed": with_vector,
                        "total": total,
                        "progress_percent": round(
                            (with_vector / total * 100) if total > 0 else 0, 2
                        ),
                    }
        except Exception as e:
            self.logger.error(f"取得統計資訊失敗: {e}")

        stats["timestamp"] = datetime.now().isoformat()
        return stats


# ============================================================================
# 向量生成器
# ============================================================================


class VectorGenerator:
    """
    向量生成器
    使用 OpenAI Embeddings API 生成文本向量
    """

    def __init__(self, config: Config):
        """
        初始化向量生成器

        Args:
            config: 系統配置
        """
        self.config = config
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)

        if config.openai_api_key:
            try:
                self.client = OpenAI(
                    api_key=config.openai_api_key, base_url=config.openai_base_url
                )
                self.logger.info(
                    f"✅ OpenAI 客戶端初始化成功，模型: {config.embedding_model}"
                )
            except Exception as e:
                self.logger.error(f"❌ OpenAI 客戶端初始化失敗: {e}")

    def generate(self, text: str) -> Optional[List[float]]:
        """
        生成單個文本的向量

        Args:
            text: 輸入文本

        Returns:
            向量列表，失敗時返回 None
        """
        if not self.client:
            return None

        try:
            # 限制文本長度（OpenAI 有 token 限制）
            text = text[:8000]

            response = self.client.embeddings.create(
                model=self.config.embedding_model, input=text
            )
            return response.data[0].embedding
        except Exception as e:
            self.logger.error(f"向量生成失敗: {e}")
            return None

    def health_check(self) -> bool:
        """
        檢查 OpenAI API 是否可用

        Returns:
            True 如果可用，否則 False
        """
        if not self.client:
            return False

        try:
            # 嘗試生成一個簡單的測試向量
            self.generate("test")
            return True
        except:
            return False


# ============================================================================
# 搜尋引擎
# ============================================================================


class SearchEngine:
    """
    搜尋引擎
    實現關鍵字、向量和混合搜尋
    """

    def __init__(
        self,
        es_client: ElasticsearchClient,
        vector_gen: VectorGenerator,
        text_processor: TextProcessor,
    ):
        """
        初始化搜尋引擎

        Args:
            es_client: Elasticsearch 客戶端
            vector_gen: 向量生成器
            text_processor: 文字處理器
        """
        self.es_client = es_client
        self.vector_gen = vector_gen
        self.text_processor = text_processor
        self.logger = logging.getLogger(self.__class__.__name__)

    def keyword_search(
        self, query: str, index_pattern: str, size: int = 5
    ) -> Dict[str, Any]:
        """
        執行關鍵字搜尋 - 優化版本
        特別加強對狀態欄位的搜尋支援

        Args:
            query: 查詢字串
            index_pattern: 索引模式
            size: 返回結果數

        Returns:
            搜尋結果
        """
        # 準備查詢變體（繁簡體）
        _, query_variants = self.text_processor.prepare_search_query(query)

        # 檢測是否為產品編號查詢（P或W開頭加數字）
        product_id_pattern = re.compile(r"^[PW]\d{3}$")
        is_product_id_query = bool(product_id_pattern.match(query.strip().upper()))

        # 構建查詢 DSL - 優化版本
        search_body = {
            "size": size * 2,  # 取更多結果以提高召回率
            "_source": {"excludes": ["content_vector"]},
            "query": {"bool": {"should": []}},
            "highlight": {
                "fields": {
                    "field_status": {"fragment_size": 50, "number_of_fragments": 1},
                    "field_complaint_status": {
                        "fragment_size": 50,
                        "number_of_fragments": 1,
                    },
                    "searchable_content": {
                        "fragment_size": 150,
                        "number_of_fragments": 3,
                    },
                    "all_content": {"fragment_size": 150, "number_of_fragments": 2},
                    "content": {"fragment_size": 150, "number_of_fragments": 2},
                    "text": {"fragment_size": 150, "number_of_fragments": 2},
                    "field_*": {"fragment_size": 100, "number_of_fragments": 2},
                },
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
            },
        }

        # 如果是產品編號查詢，優先精確匹配
        if is_product_id_query:
            product_id = query.strip().upper()

            # 1. 精確匹配 product_ids 陣列（最高優先）
            search_body["query"]["bool"]["should"].append(
                {"term": {"product_ids": {"value": product_id, "boost": 50.0}}}
            )

            # 2. 精確匹配 metadata.product_id
            search_body["query"]["bool"]["should"].append(
                {"term": {"metadata.product_id": {"value": product_id, "boost": 40.0}}}
            )

            # 3. 精確匹配 source_meta.product_id
            search_body["query"]["bool"]["should"].append(
                {
                    "term": {
                        "source_meta.product_id": {"value": product_id, "boost": 40.0}
                    }
                }
            )

            # 4. 在標題中搜尋
            search_body["query"]["bool"]["should"].append(
                {"match_phrase": {"title": {"query": product_id, "boost": 30.0}}}
            )

            # 5. 在內容中搜尋
            search_body["query"]["bool"]["should"].append(
                {"match_phrase": {"content": {"query": product_id, "boost": 20.0}}}
            )

            # 6. 在文本中搜尋
            search_body["query"]["bool"]["should"].append(
                {"match_phrase": {"text": {"query": product_id, "boost": 20.0}}}
            )

            # 7. 在描述中搜尋（客訴記錄）
            search_body["query"]["bool"]["should"].append(
                {
                    "match_phrase": {
                        "field_description": {"query": product_id, "boost": 15.0}
                    }
                }
            )

            # 8. 全文搜尋作為後備
            search_body["query"]["bool"]["should"].append(
                {
                    "multi_match": {
                        "query": product_id,
                        "fields": ["all_content", "searchable_content"],
                        "type": "phrase",
                        "boost": 10.0,
                    }
                }
            )

        else:
            # 非產品編號查詢，使用原有邏輯
            # ... [保持原有的狀態搜尋和一般搜尋邏輯]

            # 1. 精確匹配狀態欄位（最高優先）
            search_body["query"]["bool"]["should"].extend(
                [
                    {"term": {"field_status.keyword": {"value": query, "boost": 20.0}}},
                    {
                        "term": {
                            "field_complaint_status.keyword": {
                                "value": query,
                                "boost": 20.0,
                            }
                        }
                    },
                    {
                        "term": {
                            "field_handling_status.keyword": {
                                "value": query,
                                "boost": 20.0,
                            }
                        }
                    },
                ]
            )

            # 2. 短語匹配
            search_body["query"]["bool"]["should"].extend(
                [
                    {"match_phrase": {"field_status": {"query": query, "boost": 15.0}}},
                    {"match_phrase": {"all_content": {"query": query, "boost": 5.0}}},
                ]
            )

            # 3. 多欄位搜尋（使用變體）
            for variant in query_variants:
                search_body["query"]["bool"]["should"].append(
                    {
                        "multi_match": {
                            "query": variant,
                            "fields": [
                                "field_status^20",
                                "field_complaint_status^20",
                                "field_handling_status^20",
                                "field_process_status^20",
                                "field_state^20",
                                "field_complaint_id^10",
                                "field_product_id^8",
                                "field_product_name^8",
                                "field_description^5",
                                "field_complaint_description^5",
                                "field_complaint_content^5",
                                "searchable_content^3",
                                "all_content^2",
                                "content^2",
                                "text^2",
                                "field_*^1",
                            ],
                            "type": "best_fields",
                            "analyzer": "ik_smart",
                            "boost": 1.0 if variant == query else 0.8,
                        }
                    }
                )

        search_body["query"]["bool"]["minimum_should_match"] = 1

        # 加入聚合以了解結果分佈
        search_body["aggs"] = {
            "type_distribution": {"terms": {"field": "type", "size": 10}},
            "product_distribution": {"terms": {"field": "product_ids", "size": 20}},
        }

        self.logger.info(
            f"執行關鍵字搜尋: {query} (是否產品編號: {is_product_id_query})"
        )
        result = self.es_client.search(index_pattern, search_body)

        # 後處理：去重並限制結果數量
        seen_ids = set()
        filtered_hits = []

        for hit in result.get("hits", {}).get("hits", []):
            # 使用文檔的唯一ID來去重
            doc_id = hit.get("_id")
            if doc_id not in seen_ids:
                seen_ids.add(doc_id)
                filtered_hits.append(hit)
                if len(filtered_hits) >= size:
                    break

        result["hits"]["hits"] = filtered_hits

        # 記錄聚合結果
        if "aggregations" in result:
            if "type_distribution" in result["aggregations"]:
                type_buckets = result["aggregations"]["type_distribution"]["buckets"]
                self.logger.info(f"類型分佈: {type_buckets}")
            if "product_distribution" in result["aggregations"]:
                product_buckets = result["aggregations"]["product_distribution"][
                    "buckets"
                ]
                self.logger.info(f"產品分佈: {product_buckets[:5]}")  # 只顯示前5個

        return result

    def vector_search(
        self, query: str, index_pattern: str, size: int = 5
    ) -> Dict[str, Any]:
        """
        執行向量搜尋
        基於語意相似度

        Args:
            query: 查詢字串
            index_pattern: 索引模式
            size: 返回結果數

        Returns:
            搜尋結果
        """
        # 生成查詢向量
        query_vector = self.vector_gen.generate(query)
        if not query_vector:
            self.logger.warning("向量生成失敗，返回空結果")
            return {"hits": {"hits": [], "total": {"value": 0}}}

        # 構建 KNN 查詢
        search_body = {
            "size": size,
            "_source": {"excludes": ["content_vector"]},
            "knn": {
                "field": "content_vector",
                "query_vector": query_vector,
                "k": size,
                "num_candidates": size * 10,
            },
        }

        self.logger.info(f"執行向量搜尋: {query}")
        return self.es_client.search(index_pattern, search_body)

    def hybrid_search(
        self, query: str, index_pattern: str, size: int = 5
    ) -> Dict[str, Any]:
        """
        執行混合搜尋 - 優化版本
        結合關鍵字和向量搜尋，特別優化狀態搜尋

        Args:
            query: 查詢字串
            index_pattern: 索引模式
            size: 返回結果數

        Returns:
            搜尋結果
        """
        # 準備查詢變體
        _, query_variants = self.text_processor.prepare_search_query(query)

        # 生成查詢向量
        query_vector = self.vector_gen.generate(query)

        # 構建混合查詢 - 優化版本
        search_body = {
            "size": size * 3,  # 取更多結果後重排序
            "_source": {"excludes": ["content_vector"]},
            "query": {
                "bool": {
                    "should": [
                        # 關鍵字搜尋部分（提高權重）
                        {
                            "bool": {
                                "should": [
                                    # 精確匹配狀態
                                    {
                                        "term": {
                                            "field_status.keyword": {
                                                "value": variant,
                                                "boost": 10.0,
                                            }
                                        }
                                    }
                                    for variant in query_variants
                                ]
                                + [
                                    # 短語匹配
                                    {
                                        "match_phrase": {
                                            "all_content": {
                                                "query": query,
                                                "boost": 3.0,
                                            }
                                        }
                                    }
                                ]
                                + [
                                    # 多欄位搜尋
                                    {
                                        "multi_match": {
                                            "query": variant,
                                            "fields": [
                                                "field_status^15",
                                                "field_complaint_status^15",
                                                "field_complaint_id^10",
                                                "field_product_id^8",
                                                "field_product_name^8",
                                                "field_description^5",
                                                "searchable_content^3",
                                                "all_content^2",
                                                "field_*",
                                            ],
                                            "type": "best_fields",
                                            "analyzer": "ik_smart",
                                            "boost": 0.7,  # 混合模式中降低關鍵字權重
                                        }
                                    }
                                    for variant in query_variants
                                ],
                                "minimum_should_match": 1,
                                "boost": 0.6,  # 整體關鍵字部分權重
                            }
                        }
                    ]
                }
            },
            "highlight": {
                "fields": {
                    "field_status": {"fragment_size": 50},
                    "searchable_content": {"fragment_size": 150},
                    "all_content": {"fragment_size": 150},
                    "field_*": {"fragment_size": 100},
                }
            },
        }

        # 如果有向量，加入向量搜尋
        if query_vector:
            search_body["knn"] = {
                "field": "content_vector",
                "query_vector": query_vector,
                "k": size * 2,  # 增加候選數量
                "num_candidates": size * 20,  # 增加候選池大小
                "boost": 0.4,  # 混合模式中的向量權重
            }

        self.logger.info(f"執行混合搜尋: {query}")
        result = self.es_client.search(index_pattern, search_body)

        # 重新排序和去重
        hits_by_id = {}
        for hit in result.get("hits", {}).get("hits", []):
            doc_id = hit["_source"].get("field_complaint_id", hit["_id"])

            # 如果已存在，保留分數較高的
            if doc_id not in hits_by_id or hit["_score"] > hits_by_id[doc_id]["_score"]:
                hits_by_id[doc_id] = hit

        # 按分數排序並限制數量
        sorted_hits = sorted(
            hits_by_id.values(), key=lambda x: x["_score"], reverse=True
        )
        result["hits"]["hits"] = sorted_hits[:size]

        self.logger.info(
            f"混合搜尋結果: 原始 {len(result.get('hits', {}).get('hits', []))} 筆，去重後 {len(sorted_hits)} 筆，返回 {len(result['hits']['hits'])} 筆"
        )

        return result


# ============================================================================
# 答案生成器
# ============================================================================


class AnswerGenerator:
    """
    答案生成器
    使用 GPT 根據搜尋結果生成自然語言答案
    """

    def __init__(self, config: Config):
        """
        初始化答案生成器

        Args:
            config: 系統配置
        """
        self.config = config
        self.client = None
        self.logger = logging.getLogger(self.__class__.__name__)

        if config.openai_api_key:
            try:
                self.client = OpenAI(
                    api_key=config.openai_api_key, base_url=config.openai_base_url
                )
                self.logger.info(f"✅ GPT 客戶端初始化成功，模型: {config.gpt_model}")
            except Exception as e:
                self.logger.error(f"❌ GPT 客戶端初始化失敗: {e}")

    def format_context(
        self, search_results: Dict[str, Any], max_contexts: int = 5
    ) -> str:
        """
        格式化搜尋結果為上下文

        Args:
            search_results: ES 搜尋結果
            max_contexts: 最多使用的文檔數

        Returns:
            格式化的上下文字串
        """
        contexts = []
        hits = search_results.get("hits", {}).get("hits", [])[:max_contexts]

        for hit in hits:
            source = hit["_source"]
            metadata = source.get("metadata", {})

            # 收集重要欄位
            content_parts = []

            # 優先使用高亮內容
            if "highlight" in hit:
                for field, highlights in hit["highlight"].items():
                    # 只取前兩個高亮片段
                    content_parts.extend(highlights[:2])

            # 如果沒有高亮，使用原始內容
            if not content_parts:
                if source.get("searchable_content"):
                    content_parts.append(source["searchable_content"][:300])
                elif source.get("all_content"):
                    content_parts.append(source["all_content"][:300])

            # 組合上下文
            context = f"【來源檔案: {metadata.get('source_file', '未知')}, "
            context += f"資料表: {metadata.get('table_name', '未知')}】\n"
            context += "\n".join(content_parts)
            contexts.append(context)

        return "\n\n---\n\n".join(contexts)

    def generate(self, query: str, context: str, temperature: float = 0.7) -> str:
        """
        生成答案

        Args:
            query: 用戶查詢
            context: 搜尋結果上下文
            temperature: 生成溫度

        Returns:
            生成的答案
        """
        if not self.client:
            return "抱歉，GPT 服務目前不可用。"

        # 系統提示詞
        system_prompt = """你是一個專業的資料庫查詢助手，擅長分析企業資料並提供準確的答案。
        
請遵循以下規則：
1. 僅根據提供的上下文資訊回答，不要編造或推測
2. 如果上下文中沒有相關資訊，請明確說明「根據現有資料無法回答此問題」
3. 回答要準確、具體、有條理
4. 使用繁體中文回答
5. 如果涉及數據或具體資訊，請引用來源
6. 適當使用項目符號或編號來組織資訊
7. 保持專業但友善的語氣"""

        # 用戶提示詞
        user_prompt = f"""問題：{query}

相關資料：
{context}

請根據上述資料回答問題。如果資料不足，請說明需要哪些額外資訊。"""

        try:
            response = self.client.chat.completions.create(
                model=self.config.gpt_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=temperature,
                max_tokens=1000,
                top_p=0.95,
                frequency_penalty=0.5,
                presence_penalty=0.5,
            )

            answer = response.choices[0].message.content
            self.logger.info(f"成功生成答案，長度: {len(answer)} 字元")
            return answer

        except Exception as e:
            self.logger.error(f"生成答案失敗: {e}")
            return f"生成答案時發生錯誤：{str(e)}"


# ============================================================================
# RAG 服務主類別
# ============================================================================


class RAGService:
    """
    RAG 服務主類別
    整合所有元件提供完整的 RAG 功能
    """

    def __init__(self, config: Config):
        """
        初始化 RAG 服務

        Args:
            config: 系統配置
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

        # 初始化各元件
        self.text_processor = TextProcessor()
        self.es_client = ElasticsearchClient(config)
        self.vector_gen = VectorGenerator(config)
        self.search_engine = SearchEngine(
            self.es_client, self.vector_gen, self.text_processor
        )
        self.answer_gen = AnswerGenerator(config)

        self.logger.info("✅ RAG 服務初始化完成")

    def process_query(self, request: QueryRequest) -> QueryResponse:
        """
        處理查詢請求

        Args:
            request: 查詢請求

        Returns:
            查詢回應
        """
        start_time = datetime.now()

        # 處理查詢字串（簡繁轉換）
        processed_query, _ = self.text_processor.prepare_search_query(
            request.query, request.convert_to_traditional
        )

        self.logger.info(f"處理查詢: {request.query} -> {processed_query}")

        # 執行搜尋
        if request.mode == SearchMode.KEYWORD:
            search_results = self.search_engine.keyword_search(
                request.query, request.index_pattern, request.top_k
            )
        elif request.mode == SearchMode.VECTOR:
            search_results = self.search_engine.vector_search(
                request.query, request.index_pattern, request.top_k
            )
        else:  # HYBRID
            search_results = self.search_engine.hybrid_search(
                request.query, request.index_pattern, request.top_k
            )

        # 格式化搜尋結果
        sources = []
        for hit in search_results.get("hits", {}).get("hits", []):
            source = hit["_source"]
            customdata = source.get("metadata", {})
            customdata["status"] = source.get("status")
            sources.append(
                SearchResult(
                    score=hit.get("_score", 0),
                    index=hit["_index"],
                    metadata=customdata,
                    content=source.get("searchable_content", "")[:200],
                    highlights=hit.get("highlight", {}),
                )
            )

        # 生成答案（如果需要）
        answer = None
        if request.use_gpt and sources:
            # 修正：使用實際搜尋結果的數量，確保所有結果都被包含在上下文中
            actual_results_count = len(search_results.get("hits", {}).get("hits", []))

            # 使用 AI 生成的回答預設最多是五筆，這邊自己調整，EX: 搜十筆，若只有七筆就只顯示七筆
            max_contexts = min(actual_results_count, request.top_k)

            self.logger.info(
                f"GPT 上下文使用 {max_contexts} 筆結果（共 {actual_results_count} 筆）"
            )

            context = self.answer_gen.format_context(
                search_results, max_contexts=max_contexts
            )
            answer = self.answer_gen.generate(
                processed_query, context, request.temperature
            )

        # 計算處理時間
        processing_time = int((datetime.now() - start_time).total_seconds() * 1000)

        return QueryResponse(
            query=request.query,
            processed_query=processed_query,
            answer=answer,
            sources=sources,
            search_mode=request.mode.value,
            total_hits=search_results.get("hits", {}).get("total", {}).get("value", 0),
            processing_time_ms=processing_time,
        )

    def health_check(self) -> HealthResponse:
        """
        執行健康檢查

        Returns:
            健康狀態
        """
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
        """
        取得系統統計資訊

        Returns:
            統計資訊
        """
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
    description="智能檢索和問答系統 API",
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

    logger.info(
        f"📊 使用模型：Embedding={config.embedding_model}, GPT={config.gpt_model}"
    )
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
    """
    健康檢查端點

    檢查系統各元件的健康狀態
    """
    if not rag_service:
        raise HTTPException(status_code=503, detail="服務尚未初始化")

    return rag_service.health_check()


@app.post("/query", response_model=QueryResponse, tags=["查詢"])
async def query_data(request: QueryRequest):
    """
    智能查詢端點

    支援關鍵字、向量和混合搜尋模式，
    可選擇使用 GPT 生成自然語言答案
    """
    if not rag_service:
        raise HTTPException(status_code=503, detail="服務尚未初始化")

    try:
        return rag_service.process_query(request)
    except Exception as e:
        logger.error(f"查詢處理失敗: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"查詢處理失敗: {str(e)}")


@app.get("/stats", tags=["系統"])
async def get_statistics():
    """
    取得系統統計資訊

    包含索引數量、文檔數量、向量化進度等
    """
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
