#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件管理 RAG API 服務 - 多索引版本
"""

import os, json, logging, requests, pymysql, re
from datetime import datetime
from typing import List, Dict, Any, Optional
from requests.auth import HTTPBasicAuth
from pymysql.cursors import DictCursor
from urllib.parse import quote

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# ==================== 環境配置 ====================
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", "admin@12345")
ES_INDEX_PATTERN = (
    "erp-ecn-notices,erp-ecn-applications,erp-complaint-records,erp-fmea,erp-structure"
)

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "fuhsin_erp_demo")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
GPT_MODEL = os.getenv("GPT_MODEL", "gpt-4o-mini")

FILE_SERVICE_PUBLIC_URL = os.getenv("FILE_SERVICE_PUBLIC_URL", "http://localhost:8088")

# ==================== 日誌配置 ====================
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ==================== FastAPI 初始化 ====================
app = FastAPI(
    title="文件管理 RAG API",
    description="支持跨多個索引的技術文件智慧檢索服務",
    version="3.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== 工具函數 ====================
def clean_content(text: str, preserve_line_breaks: bool = True) -> str:
    """清理文本中的無用標記"""
    if not text:
        return text

    # 移除頁碼標記
    text = re.sub(r"\[第\s*\d+\s*頁\]", "", text)
    text = re.sub(r"【第\s*\d+\s*頁】", "", text)
    text = re.sub(r"Page\s+\d+", "", text, flags=re.IGNORECASE)

    if preserve_line_breaks:
        # 保留換行，只清理多餘空格
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = "\n".join(line.strip() for line in text.split("\n"))
    else:
        # 合併所有空白字符
        text = re.sub(r"\s+", " ", text)

    return text.strip()


# ==================== 數據模型 ====================
class SearchRequest(BaseModel):
    query: str = Field(..., description="搜尋查詢字串")
    mode: str = Field("hybrid", description="搜尋模式: keyword | vector | hybrid")
    top_k: int = Field(10, ge=1, le=50, description="返回結果數量")
    use_gpt: bool = Field(True, description="是否使用 GPT 生成回應")
    doc_type_filter: Optional[List[str]] = Field(None, description="文件類型過濾")
    date_from: Optional[str] = Field(None, description="起始日期 (YYYY-MM-DD)")
    date_to: Optional[str] = Field(None, description="結束日期 (YYYY-MM-DD)")
    department: Optional[str] = Field(None, description="部門過濾")


class DocumentInfo(BaseModel):
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
    index_name: Optional[str] = None


class SearchResponse(BaseModel):
    success: bool
    query: str
    mode: str
    total: int
    documents: List[DocumentInfo]
    gpt_response: Optional[str] = None
    search_time_ms: int
    metadata: Dict[str, Any] = {}


# ==================== 文件 URL 處理器 ====================
class FileURLHandler:
    """處理文件 URL 生成"""

    @staticmethod
    def generate_file_url(file_path: str, file_name: str = None) -> str:
        """生成文件下載 URL"""
        if not file_path:
            return None

        if file_path.startswith("http://") or file_path.startswith("https://"):
            return file_path

        # 移除路徑前綴
        path_prefixes = ["/mnt/pdf/done/", "/mnt/pdf/done", "pdf/done/", "./pdf/done/"]
        cleaned_path = file_path
        for prefix in path_prefixes:
            if cleaned_path.startswith(prefix):
                cleaned_path = cleaned_path.replace(prefix, "", 1)
                break

        cleaned_path = cleaned_path.strip("/")
        if not cleaned_path and file_name:
            cleaned_path = file_name

        if not cleaned_path:
            logger.warning("無法生成文件 URL: 路徑為空")
            return None

        encoded_path = quote(cleaned_path, safe="/")
        public_url = f"{FILE_SERVICE_PUBLIC_URL}/{encoded_path}"
        logger.debug(f"生成文件 URL: {file_path} -> {public_url}")

        return public_url


# ==================== 向量生成器 ====================
class VectorGenerator:
    def __init__(self):
        self.client = None
        if OPENAI_API_KEY and OpenAI:
            self.client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL)
            self.model = EMBEDDING_MODEL
            logger.info(f"向量生成器初始化: {EMBEDDING_MODEL}")

    def generate(self, text: str) -> Optional[List[float]]:
        if not self.client or not text:
            return None
        try:
            response = self.client.embeddings.create(
                model=self.model, input=text[:8000]
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"向量生成失敗: {e}")
            return None


# ==================== MySQL 管理器 ====================
class MySQLManager:
    def __init__(self):
        self.connection = None

    def ensure_connection(self):
        """確保 MySQL 連接"""
        try:
            if not self.connection or not self.connection.open:
                self.connection = pymysql.connect(
                    host=MYSQL_HOST,
                    port=MYSQL_PORT,
                    user=MYSQL_USER,
                    password=MYSQL_PASSWORD,
                    database=MYSQL_DATABASE,
                    cursorclass=DictCursor,
                    charset="utf8mb4",
                )
                logger.info("✅ MySQL 連接成功")
        except Exception as e:
            logger.error(f"MySQL 連接失敗: {e}")
            self.connection = None

    def search_by_product_ids(self, product_ids: List[str]) -> set:
        """從多個表搜尋產品相關文件"""
        self.ensure_connection()
        if not self.connection:
            return set()

        doc_ids = set()
        try:
            with self.connection.cursor() as cursor:
                # structured_documents
                for pid in product_ids:
                    cursor.execute(
                        """
                        SELECT original_doc_id FROM structured_documents 
                        WHERE JSON_CONTAINS(product_codes, %s)
                    """,
                        (json.dumps(pid),),
                    )
                    doc_ids.update(row["original_doc_id"] for row in cursor.fetchall())

                # 其他表
                if product_ids:
                    placeholders = ",".join(["%s"] * len(product_ids))
                    for table in [
                        "ecn_notices",
                        "ecn_applications",
                        "complaint_records",
                    ]:
                        cursor.execute(
                            f"""
                            SELECT doc_id FROM {table} WHERE product_code IN ({placeholders})
                        """,
                            tuple(product_ids),
                        )
                        doc_ids.update(row["doc_id"] for row in cursor.fetchall())
        except Exception as e:
            logger.error(f"MySQL 產品搜尋失敗: {e}")

        return doc_ids

    def search_by_keywords(self, keywords: List[str]) -> Dict[str, float]:
        """從多個表的關鍵字欄位搜尋"""
        self.ensure_connection()
        if not self.connection:
            return {}

        doc_scores = {}
        try:
            with self.connection.cursor() as cursor:
                for keyword in keywords:
                    keyword_pattern = f"%{keyword}%"

                    # 搜尋 structured_documents 的 summary
                    cursor.execute(
                        """
                        SELECT original_doc_id, 
                               (LENGTH(summary) - LENGTH(REPLACE(LOWER(summary), LOWER(%s), ''))) / LENGTH(%s) as score
                        FROM structured_documents 
                        WHERE summary LIKE %s
                    """,
                        (keyword, keyword, keyword_pattern),
                    )

                    for row in cursor.fetchall():
                        doc_id = row["original_doc_id"]
                        doc_scores[doc_id] = doc_scores.get(doc_id, 0) + float(
                            row["score"] or 0
                        )

                    # 搜尋 technical_documents 的 content
                    cursor.execute(
                        """
                        SELECT doc_id,
                               (LENGTH(content) - LENGTH(REPLACE(LOWER(content), LOWER(%s), ''))) / LENGTH(%s) * 0.5 as score
                        FROM technical_documents
                        WHERE content LIKE %s
                        LIMIT 100
                    """,
                        (keyword, keyword, keyword_pattern),
                    )

                    for row in cursor.fetchall():
                        doc_id = row["doc_id"]
                        doc_scores[doc_id] = doc_scores.get(doc_id, 0) + float(
                            row["score"] or 0
                        )

        except Exception as e:
            logger.error(f"MySQL 關鍵字搜尋失敗: {e}")

        return doc_scores

    def get_full_content(self, doc_ids: List[str]) -> Dict[str, str]:
        """獲取文件的完整內容"""
        self.ensure_connection()
        if not self.connection or not doc_ids:
            return {}

        try:
            with self.connection.cursor() as cursor:
                placeholders = ",".join(["%s"] * len(doc_ids))
                cursor.execute(
                    f"""
                    SELECT doc_id, content FROM technical_documents
                    WHERE doc_id IN ({placeholders})
                """,
                    tuple(doc_ids),
                )

                result = {
                    row["doc_id"]: row["content"] or "" for row in cursor.fetchall()
                }
                logger.info(f"✅ 獲取 {len(result)} 個文件的完整內容")
                return result
        except Exception as e:
            logger.error(f"獲取完整內容失敗: {e}")
            return {}

    def extract_content_snippet(
        self, content: str, keywords: List[str], max_length: int = 300
    ) -> str:
        """從完整內容中提取包含關鍵字的片段"""
        if not content or not keywords:
            return content[:max_length] if content else ""

        # 查找第一個關鍵字出現的位置
        min_pos = len(content)
        for keyword in keywords:
            pos = content.lower().find(keyword.lower())
            if pos != -1 and pos < min_pos:
                min_pos = pos

        # 向前後擴展
        if min_pos < len(content):
            start = max(0, min_pos - 100)
            end = min(len(content), min_pos + max_length - 100)
            snippet = content[start:end]

            if start > 0:
                snippet = "..." + snippet
            if end < len(content):
                snippet = snippet + "..."

            return snippet

        return content[:max_length] + ("..." if len(content) > max_length else "")

    def extract_smart_snippets(
        self,
        content: str,
        summary: str,
        keywords: List[str],
        max_snippets: int = 3,
        snippet_length: int = 400,
    ) -> List[str]:
        """
        智能提取內容片段，確保與摘要不重複

        Args:
            content: 完整內容
            summary: 摘要（用於去重）
            keywords: 關鍵字列表
            max_snippets: 最大片段數
            snippet_length: 每個片段長度

        Returns:
            不重複的內容片段列表
        """
        if not content or not keywords:
            return []

        # 清理內容
        content_clean = clean_content(content, preserve_line_breaks=True)
        summary_clean = (
            clean_content(summary, preserve_line_breaks=False) if summary else ""
        )

        snippets = []
        used_positions = set()

        # 按關鍵字查找片段
        for keyword in keywords[: max_snippets * 2]:
            keyword_lower = keyword.lower()
            content_lower = content_clean.lower()

            pos = 0
            while pos < len(content_lower):
                pos = content_lower.find(keyword_lower, pos)
                if pos == -1:
                    break

                # 避免位置重疊
                if any(
                    abs(pos - used_pos) < snippet_length // 2
                    for used_pos in used_positions
                ):
                    pos += 1
                    continue

                # 提取片段
                start = max(0, pos - 100)
                end = min(len(content_clean), pos + snippet_length - 100)
                snippet = content_clean[start:end]

                # 在句子邊界調整
                if start > 0:
                    for i in range(min(50, len(snippet))):
                        if snippet[i] in "。！？\n；":
                            snippet = snippet[i + 1 :]
                            break
                    snippet = "..." + snippet

                if end < len(content_clean):
                    for i in range(len(snippet) - 1, max(0, len(snippet) - 50), -1):
                        if snippet[i] in "。！？\n；":
                            snippet = snippet[: i + 1]
                            break
                    snippet = snippet + "..."

                snippet = snippet.strip()

                # 檢查是否與 summary 重複
                if self._is_content_similar(snippet, summary_clean):
                    pos += 1
                    continue

                # 檢查是否與已有片段重複
                if any(
                    self._is_content_similar(snippet, existing) for existing in snippets
                ):
                    pos += 1
                    continue

                # 檢查片段長度
                if len(snippet.strip(".\n ")) < 20:
                    pos += 1
                    continue

                snippets.append(snippet)
                used_positions.add(pos)

                if len(snippets) >= max_snippets:
                    return snippets

                pos += 1

        return snippets

    def _is_content_similar(
        self, text1: str, text2: str, threshold: float = 0.7
    ) -> bool:
        """檢查兩段文本是否相似"""
        if not text1 or not text2:
            return False

        # 移除標點和空白
        clean1 = re.sub(r"[^\w]", "", text1.lower())
        clean2 = re.sub(r"[^\w]", "", text2.lower())

        if not clean1 or not clean2:
            return False

        # 包含檢查
        if clean1 in clean2 or clean2 in clean1:
            return True

        # Jaccard 相似度
        set1 = set(clean1[i : i + 3] for i in range(len(clean1) - 2))
        set2 = set(clean2[i : i + 3] for i in range(len(clean2) - 2))

        if not set1 or not set2:
            return False

        intersection = len(set1 & set2)
        union = len(set1 | set2)
        similarity = intersection / union if union > 0 else 0

        return similarity > threshold


# ==================== 文件搜尋服務 ====================
class DocumentSearchService:
    def __init__(self):
        self.es_session = requests.Session()
        if ES_USER and ES_PASS:
            self.es_session.auth = HTTPBasicAuth(ES_USER, ES_PASS)
        self.es_session.headers.update({"Content-Type": "application/json"})

        self.vector_gen = VectorGenerator()
        self.mysql = MySQLManager()
        self.file_handler = FileURLHandler()
        self.gpt_client = None

        if OPENAI_API_KEY and OpenAI:
            self.gpt_client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL)

    def extract_product_ids(self, query: str) -> List[str]:
        """提取產品編號"""
        patterns = [
            r"[A-Z]{2,4}[\d]{2,4}[A-Z]?[\d]{0,4}[A-Z]{0,4}[\d]{0,4}[A-Z]{0,4}",
            r"\d{2,3}[-]\d{1,4}",
        ]
        product_ids = []
        for pattern in patterns:
            matches = re.findall(pattern, query.upper())
            product_ids.extend(matches)
        return list(set(product_ids))

    def extract_keywords(self, query: str) -> List[str]:
        stop_words = {"的", "是", "在", "和", "將", "或", "有", "為", "等", "了", "請", "所有", "來", "出", "未來", "改善", "統整", "列出"}
    
        # 1️⃣ 先提取英文數字組合（產品代碼）
        product_codes = re.findall(r'[A-Z0-9]{2,}[-]?[A-Z0-9]*', query)
        
        # 2️⃣ 提取中文關鍵詞：按停用詞分割
        cleaned = query
        for stop_word in stop_words:
            cleaned = cleaned.replace(stop_word, '|')
        
        # 按分隔符切分，過濾短詞
        chinese_words = [w.strip() for w in cleaned.split('|') 
                        if w.strip() and len(w.strip()) >= 2]
        
        # 3️⃣ 合併所有關鍵字
        all_keywords = product_codes + chinese_words
        
        logger.info(f"🔍 從查詢 '{query}' 提取到關鍵字: {all_keywords}")
        
        # 如果還是沒有
        if not all_keywords:
            # 嘗試提取任何 2 個字以上的詞
            words = re.findall(r'[\u4e00-\u9fff]{2,}', query)  # 只匹配中文 2+ 字
            if words:
                all_keywords = words
                logger.info(f"⚠️ 使用中文詞組: {all_keywords}")
            else:
                all_keywords = [query.strip()]
                logger.info(f"⚠️ 使用整個查詢: {all_keywords}")
        
        return list(set(all_keywords))[:10]

    def keyword_search(self, query: str, size: int = 10, filters: Dict = None) -> Dict:
        """多索引關鍵字搜尋"""
        search_body = {
            "size": size,
            "_source": {"excludes": ["original_extracted_content", "content_vector"]},
            "query": {
                "bool": {
                    "should": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": [
                                    "doc_number^10",
                                    "file_name^7",
                                    "summary^5",
                                    "keywords^7",
                                ],
                                "type": "best_fields",
                                "fuzziness": "AUTO",
                            }
                        }
                    ],
                    "minimum_should_match": 1,
                }
            },
            "highlight": {
                "fields": {
                    "summary": {"fragment_size": 150, "number_of_fragments": 2},
                    "change_description": {
                        "fragment_size": 150,
                        "number_of_fragments": 2,
                    },
                    "complaint_description": {
                        "fragment_size": 150,
                        "number_of_fragments": 2,
                    },
                },
                "pre_tags": ["<em>"],
                "post_tags": ["</em>"],
            },
        }

        try:
            response = self.es_session.post(
                f"{ES_URL}/{ES_INDEX_PATTERN}/_search", json=search_body, timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"關鍵字搜尋失敗: {e}")
            return {"hits": {"hits": [], "total": {"value": 0}}}

    def vector_search(self, query: str, size: int = 10, filters: Dict = None) -> Dict:
        """多索引向量搜尋"""
        query_vector = self.vector_gen.generate(query)
        if not query_vector:
            return {"hits": {"hits": [], "total": {"value": 0}}}

        search_body = {
            "size": size,
            "_source": {"excludes": ["original_extracted_content", "content_vector"]},
            "knn": {
                "field": "content_vector",
                "query_vector": query_vector,
                "k": size,
                "num_candidates": size * 10,
            },
        }

        try:
            response = self.es_session.post(
                f"{ES_URL}/{ES_INDEX_PATTERN}/_search", json=search_body, timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"向量搜尋失敗: {e}")
            return {"hits": {"hits": [], "total": {"value": 0}}}

    def _merge_results(self, keyword_result: Dict, vector_result: Dict) -> Dict:
        """合併關鍵字和向量搜尋結果"""
        merged_hits = []
        seen_ids = set()

        all_hits = keyword_result.get("hits", {}).get("hits", []) + vector_result.get(
            "hits", {}
        ).get("hits", [])

        for hit in all_hits:
            doc_id = hit.get("_id")
            if doc_id not in seen_ids:
                seen_ids.add(doc_id)
                merged_hits.append(hit)

        return {"hits": {"hits": merged_hits, "total": {"value": len(merged_hits)}}}

    def _process_results(
        self, es_result: Dict, mysql_scores: Dict, mysql_doc_ids: set, query: str = ""
    ) -> List[DocumentInfo]:
        """處理搜尋結果"""
        documents = []

        # 批量獲取完整內容
        doc_ids = [
            hit["_source"].get("original_doc_id")
            or hit["_source"].get("doc_id")
            or hit["_id"]
            for hit in es_result.get("hits", {}).get("hits", [])
        ]
        full_contents = self.mysql.get_full_content(doc_ids) if doc_ids else {}
        query_keywords = self.extract_keywords(query) if query else []

        for hit in es_result.get("hits", {}).get("hits", []):
            source = hit["_source"]
            doc_id = source.get("original_doc_id") or source.get("doc_id") or hit["_id"]

            # 計算評分
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

            # 清理內容
            summary = clean_content(
                source.get("summary", ""), preserve_line_breaks=True
            )

            highlight = hit.get("highlight", {})
            cleaned_highlight = {}
            for field, values in highlight.items():
                if isinstance(values, list):
                    cleaned_highlight[field] = [
                        clean_content(v, preserve_line_breaks=True) for v in values
                    ]
                else:
                    cleaned_highlight[field] = clean_content(
                        values, preserve_line_breaks=True
                    )

            # 構建預覽
            searchable_preview = ""
            if cleaned_highlight:
                first_highlight = next(iter(cleaned_highlight.values()), [])
                if isinstance(first_highlight, list) and first_highlight:
                    searchable_preview = first_highlight[0]

            if not searchable_preview and summary:
                searchable_preview = summary[:200] + (
                    "..." if len(summary) > 200 else ""
                )

            if searchable_preview:
                cleaned_highlight["_searchable_preview"] = [searchable_preview]

            # 提取內容片段
            full_content = full_contents.get(doc_id, "")
            content_snippets = []
            if full_content and query_keywords:
                # 🔥 使用智能片段提取方法
                content_snippets = self.mysql.extract_smart_snippets(
                    content=full_content,
                    summary=summary,  # 傳入摘要用於去重
                    keywords=query_keywords,
                    max_snippets=5,  # 🔥 最多5個片段（原本是3個）
                    snippet_length=500,  # 🔥 每個片段500字元（原本是300）
                )
            logger.info(content_snippets)
            if content_snippets:
                cleaned_highlight["content_snippets"] = content_snippets

            # 生成文件 URL
            file_url = None
            file_name = source.get("file_name")
            file_path = source.get("file_path")

            if file_path:
                file_url = self.file_handler.generate_file_url(file_path, file_name)
            elif file_name:
                file_url = self.file_handler.generate_file_url(file_name)

            # 生成標題
            title = (
                file_name.replace(".pdf", "")
                if file_name
                else (
                    source.get("title")
                    or f"{source.get('doc_type', '文件')} - {source.get('doc_number')}"
                    if source.get("doc_number")
                    else "技術文件"
                )
            )

            doc_info = DocumentInfo(
                doc_id=doc_id,
                doc_number=source.get("doc_number")
                or source.get("notice_number")
                or source.get("application_number")
                or "",
                doc_type=source.get("doc_type"),
                title=title,
                summary=summary,
                issue_date=source.get("doc_date")
                or source.get("ecn_date")
                or source.get("complaint_date"),
                department=source.get("department"),
                applicant=source.get("applicant") or source.get("responsible_person"),
                product_codes=product_codes if product_codes else None,
                keywords=keywords if keywords else None,
                file_url=file_url,
                file_name=file_name,
                score=round(total_score, 3),
                highlight=cleaned_highlight if cleaned_highlight else None,
                index_name=hit.get("_index"),
            )

            documents.append(doc_info)

        documents.sort(key=lambda x: x.score, reverse=True)
        return documents

    def _generate_gpt_response(
        self, query: str, documents: List[DocumentInfo]
    ) -> Optional[str]:
        """使用 GPT 生成智慧回應"""
        if not self.gpt_client or not documents:
            return None

        try:
            context_parts = []
            for idx, doc in enumerate(documents[:5], start=1):
                doc_identifier = doc.doc_number or doc.title or f"文件 {idx}"
                products_str = (
                    ", ".join(doc.product_codes) if doc.product_codes else "無"
                )

                context_parts.append(
                    f"""
【{doc_identifier}】
類型: {doc.doc_type or '技術文件'}
產品: {products_str}
部門: {doc.department or '未指定'}
摘要: {doc.summary[:200] if doc.summary else '無摘要'}
                """.strip()
                )

            context = "\n\n".join(context_parts)

            messages = [
                {
                    "role": "system",
                    "content": """你是專業的技術文件助理。根據搜尋到的文件內容，提供準確、有條理的回答。
                    
回答格式：
【主要發現】
根據文件 XXX，主要內容為...

【相關產品】
涉及產品編號：...

【建議】
建議參考文件 XXX 以了解更多細節。""",
                },
                {
                    "role": "user",
                    "content": f"查詢: {query}\n\n相關文件:\n{context}\n\n請根據以上文件回答查詢。",
                },
            ]

            response = self.gpt_client.chat.completions.create(
                model=GPT_MODEL, messages=messages, max_tokens=500, temperature=0.7
            )

            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"GPT 回應生成失敗: {e}")
            return None

    def hybrid_search(self, request: SearchRequest) -> SearchResponse:
        """混合搜尋"""
        start_time = datetime.now()
        query = request.query

        # 提取產品編號和關鍵字
        product_ids = self.extract_product_ids(query)
        keywords = self.extract_keywords(query)

        logger.info(f"搜尋查詢: {query}")
        logger.info(f"識別產品編號: {product_ids}")
        logger.info(f"提取關鍵字: {keywords}")

        # MySQL 輔助查詢
        mysql_doc_ids = set()
        mysql_scores = {}

        if product_ids:
            product_doc_ids = self.mysql.search_by_product_ids(product_ids)
            mysql_doc_ids.update(product_doc_ids)
            for doc_id in product_doc_ids:
                mysql_scores[doc_id] = mysql_scores.get(doc_id, 0) + 10

        if keywords:
            keyword_scores = self.mysql.search_by_keywords(keywords)
            mysql_doc_ids.update(keyword_scores.keys())
            for doc_id, score in keyword_scores.items():
                mysql_scores[doc_id] = mysql_scores.get(doc_id, 0) + score * 2

        # Elasticsearch 搜尋
        if request.mode == "keyword":
            es_result = self.keyword_search(query, request.top_k * 2)
        elif request.mode == "vector":
            es_result = self.vector_search(query, request.top_k * 2)
        else:
            keyword_result = self.keyword_search(query, request.top_k)
            vector_result = self.vector_search(query, request.top_k)
            es_result = self._merge_results(keyword_result, vector_result)

        # 處理結果
        final_documents = self._process_results(
            es_result, mysql_scores, mysql_doc_ids, query=query
        )
        final_documents = final_documents[: request.top_k]

        # 生成 GPT 回應
        gpt_response = None
        if request.use_gpt and self.gpt_client and final_documents:
            gpt_response = self._generate_gpt_response(query, final_documents)

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
                "keywords_used": keywords,
                "indices_searched": ES_INDEX_PATTERN,
            },
        )


# ==================== 初始化服務 ====================
search_service = DocumentSearchService()


# ==================== API 端點 ====================
@app.get("/")
async def root():
    return {
        "service": "文件管理 RAG API",
        "version": "3.2.0",
        "indices": ES_INDEX_PATTERN,
        "file_service": FILE_SERVICE_PUBLIC_URL,
        "status": "running",
    }


@app.get("/health")
async def health_check():
    try:
        es_health = search_service.es_session.get(
            f"{ES_URL}/_cluster/health", timeout=5
        )
        es_status = es_health.status_code == 200

        search_service.mysql.ensure_connection()
        mysql_status = search_service.mysql.connection is not None

        return {
            "status": "healthy" if (es_status and mysql_status) else "degraded",
            "elasticsearch": es_status,
            "mysql": mysql_status,
            "openai": search_service.gpt_client is not None,
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"健康檢查失敗: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
        }


@app.get("/stats")
async def get_statistics():
    """獲取系統統計資訊"""
    try:
        indices = ES_INDEX_PATTERN.split(",")
        index_counts = {}
        total_docs = 0

        for index in indices:
            try:
                count_response = search_service.es_session.get(
                    f"{ES_URL}/{index}/_count", timeout=5
                )
                if count_response.status_code == 200:
                    count = count_response.json().get("count", 0)
                    index_counts[index] = count
                    total_docs += count
            except:
                index_counts[index] = 0

        return {
            "success": True,
            "stats": {"total_documents": total_docs, "index_counts": index_counts},
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"獲取統計失敗: {e}")
        return {"success": False, "error": str(e)}


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
        file_path = doc.get("file_path") or doc.get("file_name")
        if file_path:
            doc["file_url"] = search_service.file_handler.generate_file_url(file_path)
            doc["download_url"] = doc["file_url"]

        return {
            "success": True,
            "document": doc,
            "related_documents": doc.get("related_doc_numbers", []),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"獲取文件失敗: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.on_event("startup")
async def startup_event():
    logger.info("=" * 50)
    logger.info("文件管理 RAG API 服務啟動")
    logger.info(f"Elasticsearch: {ES_URL}")
    logger.info(f"索引模式: {ES_INDEX_PATTERN}")
    logger.info(f"MySQL: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
    logger.info(f"文件服務: {FILE_SERVICE_PUBLIC_URL}")
    logger.info(f"GPT Model: {GPT_MODEL if search_service.gpt_client else 'Disabled'}")
    logger.info("=" * 50)


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("文件管理 RAG API 服務關閉")
    if search_service.mysql.connection:
        search_service.mysql.connection.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8010, log_level="info")
