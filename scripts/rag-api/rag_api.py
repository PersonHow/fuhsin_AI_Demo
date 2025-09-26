#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件管理 RAG API 服務
專注於技術文件的智慧檢索與回應生成

主要功能：
1. 混合搜尋：關鍵字 + 向量 + MySQL索引
2. 產品編號精確查詢
3. 關鍵字加速查詢
4. GPT 智慧回應生成
5. 文件下載連結提供
"""

import os, json, logging, requests, pymysql
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from requests.auth import HTTPBasicAuth
from pymysql.cursors import DictCursor

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# ===== 環境配置 =====
# Elasticsearch
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", "admin@12345")
ES_INDEX = "erp-documents"  # 固定使用 erp-documents 索引

# MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "fuhsin_erp_demo")

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
GPT_MODEL = os.getenv("GPT_MODEL", "gpt-4o-mini")

# 檔案服務
FILE_SERVICE_BASE_URL = os.getenv("FILE_SERVICE_URL", "http://file-service:8080/files")

# ===== 日誌配置 =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/logs/rag_api.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ===== FastAPI 初始化 =====
app = FastAPI(
    title="文件管理 RAG API",
    description="技術文件智慧檢索服務",
    version="2.0.0"
)

# CORS 設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== 資料模型 =====
class SearchRequest(BaseModel):
    """搜尋請求模型"""
    query: str = Field(..., description="搜尋查詢字串")
    mode: str = Field("hybrid", description="搜尋模式: keyword | vector | hybrid")
    top_k: int = Field(10, ge=1, le=50, description="返回結果數量")
    use_gpt: bool = Field(True, description="是否使用 GPT 生成回應")
    doc_type_filter: Optional[List[str]] = Field(None, description="文件類型過濾")
    date_from: Optional[str] = Field(None, description="起始日期 (YYYY-MM-DD)")
    date_to: Optional[str] = Field(None, description="結束日期 (YYYY-MM-DD)")
    department: Optional[str] = Field(None, description="部門過濾")

class DocumentInfo(BaseModel):
    """文件資訊模型"""
    doc_id: str
    doc_number: str
    doc_type: Optional[str]
    title: Optional[str]
    summary: Optional[str]
    issue_date: Optional[str]
    department: Optional[str]
    applicant: Optional[str]
    product_codes: Optional[List[str]]
    keywords: Optional[List[str]]
    file_url: Optional[str]
    file_name: Optional[str]
    score: float = 0.0
    highlight: Optional[Dict] = None

class SearchResponse(BaseModel):
    """搜尋回應模型"""
    success: bool
    query: str
    mode: str
    total: int
    documents: List[DocumentInfo]
    gpt_response: Optional[str] = None
    search_time_ms: int
    metadata: Dict[str, Any] = {}

# ===== 向量生成器 =====
class VectorGenerator:
    """向量生成器"""
    
    def __init__(self):
        self.client = None
        if OPENAI_API_KEY and OpenAI:
            self.client = OpenAI(
                api_key=OPENAI_API_KEY,
                base_url=OPENAI_BASE_URL
            )
            self.model = EMBEDDING_MODEL
            self.dimension = 1536 if "small" in EMBEDDING_MODEL else 3072
            logger.info(f"向量生成器初始化: {EMBEDDING_MODEL} (維度: {self.dimension})")
    
    def generate(self, text: str) -> Optional[List[float]]:
        """生成文本向量"""
        if not self.client or not text.strip():
            return None
        
        try:
            response = self.client.embeddings.create(
                model=self.model,
                input=text[:8000],  # 限制輸入長度
                encoding_format="float"
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"向量生成失敗: {e}")
            return None

# ===== MySQL 管理器 =====
class MySQLManager:
    """MySQL 資料庫管理器"""
    
    def __init__(self):
        self.connection = None
        self.connect()
    
    def connect(self):
        """建立資料庫連線"""
        try:
            self.connection = pymysql.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE,
                charset='utf8mb4',
                cursorclass=DictCursor,
                autocommit=True
            )
            logger.info("MySQL 連線成功")
        except Exception as e:
            logger.error(f"MySQL 連線失敗: {e}")
            self.connection = None
    
    def ensure_connection(self):
        """確保連線有效"""
        try:
            if not self.connection or not self.connection.ping(reconnect=False):
                self.connect()
        except:
            self.connect()
    
    def search_by_product_ids(self, product_ids: List[str]) -> List[str]:
        """透過產品編號查詢相關文件 ID"""
        if not product_ids:
            return []
        
        self.ensure_connection()
        if not self.connection:
            return []
        
        try:
            with self.connection.cursor() as cursor:
                placeholders = ','.join(['%s'] * len(product_ids))
                sql = f"""
                SELECT DISTINCT doc_id 
                FROM product_document_mapping 
                WHERE product_id IN ({placeholders})
                """
                cursor.execute(sql, product_ids)
                return [row['doc_id'] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"產品編號查詢失敗: {e}")
            return []
    
    def search_by_keywords(self, keywords: List[str], limit: int = 20) -> Dict[str, float]:
        """透過關鍵字查詢相關文件及權重"""
        if not keywords:
            return {}
        
        self.ensure_connection()
        if not self.connection:
            return {}
        
        try:
            with self.connection.cursor() as cursor:
                placeholders = ','.join(['%s'] * len(keywords))
                sql = f"""
                SELECT doc_id, SUM(frequency) as total_freq
                FROM document_keywords
                WHERE keyword IN ({placeholders})
                GROUP BY doc_id
                ORDER BY total_freq DESC
                LIMIT %s
                """
                cursor.execute(sql, keywords + [limit])
                return {row['doc_id']: float(row['total_freq']) for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"關鍵字查詢失敗: {e}")
            return {}
    
    def get_document_details(self, doc_ids: List[str]) -> List[Dict]:
        """獲取文件詳細資訊"""
        if not doc_ids:
            return []
        
        self.ensure_connection()
        if not self.connection:
            return []
        
        try:
            with self.connection.cursor() as cursor:
                placeholders = ','.join(['%s'] * len(doc_ids))
                sql = f"""
                SELECT 
                    original_doc_id as doc_id,
                    doc_number,
                    doc_type,
                    doc_date,
                    summary,
                    department,
                    applicant,
                    product_codes,
                    keywords,
                    file_name,
                    file_url,
                    related_doc_numbers
                FROM structured_documents
                WHERE original_doc_id IN ({placeholders})
                """
                cursor.execute(sql, doc_ids)
                results = []
                for row in cursor.fetchall():
                    # 解析 JSON 欄位
                    for field in ['product_codes', 'keywords', 'related_doc_numbers']:
                        if row.get(field) and isinstance(row[field], str):
                            try:
                                row[field] = json.loads(row[field])
                            except:
                                row[field] = []
                    results.append(row)
                return results
        except Exception as e:
            logger.error(f"獲取文件詳情失敗: {e}")
            return []

# ===== 文件搜尋服務 =====
class DocumentSearchService:
    """文件搜尋服務"""
    
    def __init__(self):
        self.es_session = self._init_es_session()
        self.vector_gen = VectorGenerator()
        self.mysql = MySQLManager()
        self.gpt_client = None
        
        if OPENAI_API_KEY and OpenAI:
            self.gpt_client = OpenAI(
                api_key=OPENAI_API_KEY,
                base_url=OPENAI_BASE_URL
            )
    
    def _init_es_session(self):
        """初始化 Elasticsearch 會話"""
        session = requests.Session()
        session.auth = HTTPBasicAuth(ES_USER, ES_PASS)
        session.headers.update({"Content-Type": "application/json"})
        return session
    
    def extract_product_ids(self, query: str) -> List[str]:
        """從查詢中提取產品編號"""
        import re
        patterns = [
            r"[FG]\d{2}-[A-Z0-9]+",
            r"L\d{6}[A-Z]?\d?",
            r"OB\d-[A-Z0-9]+",
            r"EC-K-\d{2}-[A-Z]-\d{3}",
        ]
        
        product_ids = []
        for pattern in patterns:
            matches = re.findall(pattern, query.upper())
            product_ids.extend(matches)
        
        return list(set(product_ids))
    
    def extract_keywords(self, query: str) -> List[str]:
        """提取查詢關鍵字"""
        # 移除常見停用詞
        stopwords = {'的', '是', '在', '和', '了', '有', '我', '你', '這', '那', '與'}
        
        # 基本分詞（實際應用可使用 jieba）
        words = [w.strip() for w in query.split() if len(w.strip()) > 1]
        keywords = [w for w in words if w not in stopwords]
        
        # 加入專業術語檢測
        technical_terms = ['ECN', 'SPEC', 'DFMEA', 'COMPLAINT', '規格', '變更', '投訴']
        for term in technical_terms:
            if term.lower() in query.lower():
                keywords.append(term)
        
        return list(set(keywords))[:10]
    
    def keyword_search(self, query: str, size: int = 10, filters: Dict = None) -> Dict:
        """關鍵字搜尋"""
        search_body = {
            "size": size,
            "_source": {
                "excludes": ["original_extracted_content", "content_vector"]
            },
            "query": {
                "bool": {
                    "should": [
                        {"match": {"doc_number": {"query": query, "boost": 3}}},
                        {"match": {"summary": {"query": query, "boost": 2}}},
                        {"match": {"original_extracted_content": query}},
                        {"match": {"keywords": query}},
                        {"match": {"product_codes": query}}
                    ],
                    "minimum_should_match": 1
                }
            },
            "highlight": {
                "fields": {
                    "summary": {"fragment_size": 150},
                    "original_extracted_content": {"fragment_size": 200}
                }
            }
        }
        
        # 添加過濾條件
        if filters:
            filter_conditions = []
            
            if filters.get('doc_type_filter'):
                filter_conditions.append({
                    "terms": {"doc_type": filters['doc_type_filter']}
                })
            
            if filters.get('date_from') or filters.get('date_to'):
                date_range = {}
                if filters.get('date_from'):
                    date_range['gte'] = filters['date_from']
                if filters.get('date_to'):
                    date_range['lte'] = filters['date_to']
                filter_conditions.append({
                    "range": {"doc_date": date_range}
                })
            
            if filters.get('department'):
                filter_conditions.append({
                    "match": {"department": filters['department']}
                })
            
            if filter_conditions:
                search_body["query"]["bool"]["filter"] = filter_conditions
        
        try:
            response = self.es_session.post(
                f"{ES_URL}/{ES_INDEX}/_search",
                json=search_body,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"關鍵字搜尋失敗: {e}")
            return {"hits": {"hits": [], "total": {"value": 0}}}
    
    def vector_search(self, query: str, size: int = 10, filters: Dict = None) -> Dict:
        """向量相似度搜尋"""
        query_vector = self.vector_gen.generate(query)
        if not query_vector:
            logger.warning("向量生成失敗，返回空結果")
            return {"hits": {"hits": [], "total": {"value": 0}}}
        
        search_body = {
            "size": size,
            "_source": {
                "excludes": ["original_extracted_content", "content_vector"]
            },
            "knn": {
                "field": "content_vector",
                "query_vector": query_vector,
                "k": size,
                "num_candidates": size * 10
            }
        }
        
        # 添加過濾（如果有）
        if filters and any(filters.values()):
            # knn 搜尋的過濾需要特別處理
            filter_conditions = []
            if filters.get('doc_type_filter'):
                filter_conditions.append({
                    "terms": {"doc_type": filters['doc_type_filter']}
                })
            
            if filter_conditions:
                search_body["knn"]["filter"] = {"bool": {"must": filter_conditions}}
        
        try:
            response = self.es_session.post(
                f"{ES_URL}/{ES_INDEX}/_search",
                json=search_body,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"向量搜尋失敗: {e}")
            return {"hits": {"hits": [], "total": {"value": 0}}}
    
    def hybrid_search(self, request: SearchRequest) -> SearchResponse:
        """混合搜尋 - 整合多種搜尋策略"""
        start_time = datetime.now()
        query = request.query
        
        # 1. 提取產品編號和關鍵字
        product_ids = self.extract_product_ids(query)
        keywords = self.extract_keywords(query)
        
        logger.info(f"搜尋查詢: {query}")
        logger.info(f"識別產品編號: {product_ids}")
        logger.info(f"提取關鍵字: {keywords}")
        
        # 2. MySQL 輔助查詢
        mysql_doc_ids = set()
        mysql_scores = {}
        
        # 產品編號查詢
        if product_ids:
            product_doc_ids = self.mysql.search_by_product_ids(product_ids)
            mysql_doc_ids.update(product_doc_ids)
            for doc_id in product_doc_ids:
                mysql_scores[doc_id] = mysql_scores.get(doc_id, 0) + 10
        
        # 關鍵字加權查詢
        if keywords:
            keyword_scores = self.mysql.search_by_keywords(keywords)
            mysql_doc_ids.update(keyword_scores.keys())
            for doc_id, score in keyword_scores.items():
                mysql_scores[doc_id] = mysql_scores.get(doc_id, 0) + score * 2
        
        # 3. Elasticsearch 搜尋
        filters = {
            'doc_type_filter': request.doc_type_filter,
            'date_from': request.date_from,
            'date_to': request.date_to,
            'department': request.department
        }
        
        # 執行搜尋
        if request.mode == "keyword":
            es_result = self.keyword_search(query, request.top_k * 2, filters)
        elif request.mode == "vector":
            es_result = self.vector_search(query, request.top_k * 2, filters)
        else:  # hybrid
            keyword_result = self.keyword_search(query, request.top_k, filters)
            vector_result = self.vector_search(query, request.top_k, filters)
            es_result = self._merge_results(keyword_result, vector_result)
        
        # 4. 合併和排序結果
        final_documents = self._process_results(es_result, mysql_scores, mysql_doc_ids)
        
        # 5. 限制結果數量
        final_documents = final_documents[:request.top_k]
        
        # 6. 獲取額外的文件詳情（如果需要）
        doc_ids = [doc.doc_id for doc in final_documents]
        if doc_ids:
            details = self.mysql.get_document_details(doc_ids)
            detail_map = {d['doc_id']: d for d in details}
            
            for doc in final_documents:
                if doc.doc_id in detail_map:
                    detail = detail_map[doc.doc_id]
                    if detail.get('related_doc_numbers'):
                        doc.keywords = (doc.keywords or []) + [f"相關:{num}" for num in detail['related_doc_numbers'][:3]]
        
        # 7. 生成 GPT 回應
        gpt_response = None
        if request.use_gpt and self.gpt_client and final_documents:
            gpt_response = self._generate_gpt_response(query, final_documents)
        
        # 計算搜尋時間
        search_time = int((datetime.now() - start_time).total_seconds() * 1000)
        
        return SearchResponse(
            success=True,
            query=query,
            mode=request.mode,
            total=len(final_documents),
            documents=final_documents,
            gpt_response=gpt_response,
            search_time_ms=search_time,
            metadata={
                "mysql_hits": len(mysql_doc_ids),
                "product_ids_found": product_ids,
                "keywords_used": keywords
            }
        )
    
    def _merge_results(self, keyword_result: Dict, vector_result: Dict) -> Dict:
        """合併關鍵字和向量搜尋結果"""
        merged_hits = []
        seen_ids = set()
        
        # 合併兩種搜尋結果
        all_hits = keyword_result.get("hits", {}).get("hits", []) + \
                  vector_result.get("hits", {}).get("hits", [])
        
        for hit in all_hits:
            doc_id = hit["_source"].get("original_doc_id") or hit["_id"]
            if doc_id not in seen_ids:
                seen_ids.add(doc_id)
                merged_hits.append(hit)
        
        return {
            "hits": {
                "hits": merged_hits,
                "total": {"value": len(merged_hits)}
            }
        }
    
    def _process_results(self, es_result: Dict, mysql_scores: Dict, mysql_doc_ids: set) -> List[DocumentInfo]:
        """處理搜尋結果，整合評分"""
        documents = []
        
        for hit in es_result.get("hits", {}).get("hits", []):
            source = hit["_source"]
            doc_id = source.get("original_doc_id") or hit["_id"]
            
            # 計算綜合評分
            es_score = hit.get("_score", 0)
            mysql_score = mysql_scores.get(doc_id, 0)
            total_score = es_score + mysql_score
            
            # 解析 JSON 欄位
            product_codes = source.get("product_codes", [])
            if isinstance(product_codes, str):
                try:
                    product_codes = json.loads(product_codes)
                except:
                    product_codes = []
            
            keywords = source.get("keywords", [])
            if isinstance(keywords, str):
                try:
                    keywords = json.loads(keywords)
                except:
                    keywords = []
            
            doc_info = DocumentInfo(
                doc_id=doc_id,
                doc_number=source.get("doc_number", ""),
                doc_type=source.get("doc_type"),
                title=source.get("file_name", "").replace(".pdf", ""),
                summary=source.get("summary"),
                issue_date=source.get("doc_date"),
                department=source.get("department"),
                applicant=source.get("applicant"),
                product_codes=product_codes,
                keywords=keywords,
                file_url=source.get("file_url"),
                file_name=source.get("file_name"),
                score=total_score,
                highlight=hit.get("highlight")
            )
            documents.append(doc_info)
        
        # 加入僅在 MySQL 中找到的文件
        es_doc_ids = {doc.doc_id for doc in documents}
        mysql_only_ids = mysql_doc_ids - es_doc_ids
        
        if mysql_only_ids:
            mysql_docs = self.mysql.get_document_details(list(mysql_only_ids))
            for doc_data in mysql_docs:
                doc_info = DocumentInfo(
                    doc_id=doc_data["doc_id"],
                    doc_number=doc_data.get("doc_number", ""),
                    doc_type=doc_data.get("doc_type"),
                    title=doc_data.get("file_name", "").replace(".pdf", ""),
                    summary=doc_data.get("summary"),
                    department=doc_data.get("department"),
                    applicant=doc_data.get("applicant"),
                    product_codes=doc_data.get("product_codes", []),
                    keywords=doc_data.get("keywords", []),
                    file_url=doc_data.get("file_url"),
                    file_name=doc_data.get("file_name"),
                    score=mysql_scores.get(doc_data["doc_id"], 0)
                )
                documents.append(doc_info)
        
        # 按分數排序
        documents.sort(key=lambda x: x.score, reverse=True)
        
        return documents
    
    def _generate_gpt_response(self, query: str, documents: List[DocumentInfo]) -> str:
        """生成 GPT 智慧回應"""
        try:
            # 準備上下文
            context_parts = []
            for i, doc in enumerate(documents[:5], 1):
                context_parts.append(f"""
                文件 {i}:
                - 編號: {doc.doc_number}
                - 類型: {doc.doc_type or '未分類'}
                - 摘要: {doc.summary or '無摘要'}
                - 產品: {', '.join(doc.product_codes) if doc.product_codes else '無'}
                """.strip())
            
            context = "\n\n".join(context_parts)
            
            # 生成回應
            messages = [
                {
                    "role": "system",
                    "content": """你是一位專業的技術文件助理。根據搜尋到的文件內容，
                    提供準確、有條理的回答。如果文件中沒有相關資訊，請明確告知。
                    回答要：1) 引用具體文件編號 2) 保持專業性 3) 簡潔明瞭"""
                },
                {
                    "role": "user",
                    "content": f"查詢: {query}\n\n相關文件:\n{context}\n\n請根據以上文件回答查詢。"
                }
            ]
            
            response = self.gpt_client.chat.completions.create(
                model=GPT_MODEL,
                messages=messages,
                max_tokens=500,
                temperature=0.7
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"GPT 回應生成失敗: {e}")
            return None

# ===== 初始化服務 =====
search_service = DocumentSearchService()

# ===== API 端點 =====
@app.get("/health")
async def health_check():
    """健康檢查端點"""
    try:
        # 檢查 ES
        es_health = search_service.es_session.get(f"{ES_URL}/_cluster/health", timeout=5)
        es_status = es_health.status_code == 200
        
        # 檢查 MySQL
        search_service.mysql.ensure_connection()
        mysql_status = search_service.mysql.connection is not None
        
        # 檢查 OpenAI
        openai_status = search_service.gpt_client is not None
        
        return {
            "status": "healthy" if (es_status and mysql_status) else "degraded",
            "elasticsearch": es_status,
            "mysql": mysql_status,
            "openai": openai_status,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"健康檢查失敗: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.post("/query", response_model=SearchResponse)
async def search_documents(request: SearchRequest):
    """文件搜尋端點"""
    try:
        logger.info(f"收到搜尋請求: {request.query}, 模式: {request.mode}")
        response = search_service.hybrid_search(request)
        return response
    except Exception as e:
        logger.error(f"搜尋失敗: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/document/{doc_id}")
async def get_document(doc_id: str):
    """獲取單一文件詳情"""
    try:
        docs = search_service.mysql.get_document_details([doc_id])
        if not docs:
            raise HTTPException(status_code=404, detail="文件不存在")
        
        doc = docs[0]
        
        # 構建下載連結
        if doc.get('file_url'):
            doc['download_url'] = doc['file_url']
        elif doc.get('file_path'):
            doc['download_url'] = f"{FILE_SERVICE_BASE_URL}/{doc_id}"
        
        return {
            "success": True,
            "document": doc,
            "related_documents": doc.get('related_doc_numbers', [])
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"獲取文件失敗: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_statistics():
    """獲取系統統計資訊"""
    try:
        stats = {}
        
        # ES 文件數量
        count_response = search_service.es_session.get(
            f"{ES_URL}/{ES_INDEX}/_count",
            timeout=5
        )
        if count_response.status_code == 200:
            stats["total_documents"] = count_response.json()["count"]
        
        # 文件類型分布
        agg_body = {
            "size": 0,
            "aggs": {
                "doc_types": {
                    "terms": {"field": "doc_type", "size": 20}
                },
                "departments": {
                    "terms": {"field": "department.keyword", "size": 20}
                }
            }
        }
        
        agg_response = search_service.es_session.post(
            f"{ES_URL}/{ES_INDEX}/_search",
            json=agg_body,
            timeout=5
        )
        
        if agg_response.status_code == 200:
            agg_data = agg_response.json()
            stats["doc_types"] = {
                bucket["key"]: bucket["doc_count"]
                for bucket in agg_data["aggregations"]["doc_types"]["buckets"]
            }
            stats["departments"] = {
                bucket["key"]: bucket["doc_count"]
                for bucket in agg_data["aggregations"]["departments"]["buckets"]
            }
        
        # MySQL 索引統計
        search_service.mysql.ensure_connection()
        if search_service.mysql.connection:
            with search_service.mysql.connection.cursor() as cursor:
                # 產品映射數量
                cursor.execute("SELECT COUNT(DISTINCT product_id) as count FROM product_document_mapping")
                stats["indexed_products"] = cursor.fetchone()["count"]
                
                # 關鍵字數量
                cursor.execute("SELECT COUNT(DISTINCT keyword) as count FROM document_keywords")
                stats["indexed_keywords"] = cursor.fetchone()["count"]
        
        return {
            "success": True,
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"獲取統計失敗: {e}")
        return {"success": False, "error": str(e)}

# ===== 啟動事件 =====
@app.on_event("startup")
async def startup_event():
    """應用啟動事件"""
    logger.info("=" * 50)
    logger.info("文件管理 RAG API 服務啟動")
    logger.info(f"Elasticsearch: {ES_URL}")
    logger.info(f"MySQL: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
    logger.info(f"GPT Model: {GPT_MODEL if search_service.gpt_client else 'Disabled'}")
    logger.info(f"Embedding Model: {EMBEDDING_MODEL if search_service.vector_gen.client else 'Disabled'}")
    logger.info("=" * 50)

@app.on_event("shutdown")
async def shutdown_event():
    """應用關閉事件"""
    logger.info("文件管理 RAG API 服務關閉")
    if search_service.mysql.connection:
        search_service.mysql.connection.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8010,
        log_level="info"
    )
