#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
向量生成服務 - 優化文本提取版
根據文檔類型提取最相關的文本生成向量
"""

import os
import time
import json
import signal
import requests
import math
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from requests.auth import HTTPBasicAuth
import logging

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

# ========== 環境變數 ==========
ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
ES_USER = os.environ.get("ES_USER", "elastic")
ES_PASS = os.environ.get("ES_PASS", "admin@12345")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
INDEX_PATTERN = os.environ.get("INDEX_PATTERN", "erp-*")
BATCH_SIZE = int(os.environ.get("VECTOR_BATCH_SIZE", "100"))
SLEEP_SEC = int(os.environ.get("SLEEP", "10"))
ES_WAIT_TIMEOUT = int(os.environ.get("ES_WAIT_TIMEOUT", "180"))
REQUESTS_TIMEOUT = int(os.environ.get("REQUESTS_TIMEOUT", "30"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))

# 自動停止配置
AUTO_STOP_ENABLED = os.environ.get("AUTO_STOP_ENABLED", "false").lower() in ("true", "1", "yes")
AUTO_STOP_EMPTY_ROUNDS = int(os.environ.get("AUTO_STOP_EMPTY_ROUNDS", "3"))

# ========== 日誌配置 ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========== 連線物件 ==========
session = requests.Session()
if ES_USER and ES_PASS:
    session.auth = HTTPBasicAuth(ES_USER, ES_PASS)

client: Optional[OpenAI] = None
if OPENAI_API_KEY and OpenAI is not None:
    client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL)

_SHOULD_STOP = False

# ========== 工具方法 ==========
def log(msg: str) -> None:
    logger.info(msg)

def wait_for_es(timeout_sec: int = ES_WAIT_TIMEOUT) -> None:
    """等待 Elasticsearch 至少達到 yellow 健康狀態"""
    deadline = time.time() + timeout_sec
    last_err: Optional[Exception] = None
    while time.time() < deadline:
        try:
            r = session.get(
                f"{ES_URL}/_cluster/health",
                params={"wait_for_status": "yellow", "timeout": "30s"},
                timeout=REQUESTS_TIMEOUT,
            )
            if r.ok:
                status = r.json().get("status")
                if status in ("yellow", "green"):
                    log(f"ES 就緒（status={status}）")
                    return
                log(f"ES 狀態 {status}，繼續等待…")
        except Exception as e:
            last_err = e
        time.sleep(3)
    raise RuntimeError(f"Elasticsearch 在 {timeout_sec}s 內未就緒: {last_err}")

def _sleep_backoff(i: int, base: float = 1.0) -> None:
    time.sleep(base * (2**i))

def http_get(url: str, *, params: Optional[Dict[str, Any]] = None, 
             headers: Optional[Dict[str, str]] = None, retries: int = MAX_RETRIES) -> requests.Response:
    for i in range(retries):
        try:
            r = session.get(url, params=params, headers=headers, timeout=REQUESTS_TIMEOUT)
            if r.status_code in (502, 503, 504):
                raise requests.ConnectionError(f"Transient {r.status_code}")
            return r
        except (requests.ConnectionError, requests.Timeout) as e:
            if i == retries - 1:
                raise
            log(f"GET 重試 {i+1}/{retries-1}: {e}")
            _sleep_backoff(i)
    raise RuntimeError("GET 重試已用盡")

def http_post(url: str, *, json_body: Optional[Dict[str, Any]] = None, data: Optional[str] = None,
              headers: Optional[Dict[str, str]] = None, retries: int = MAX_RETRIES) -> requests.Response:
    for i in range(retries):
        try:
            r = session.post(url, json=json_body, data=data, headers=headers, timeout=REQUESTS_TIMEOUT)
            if r.status_code in (502, 503, 504):
                raise requests.ConnectionError(f"Transient {r.status_code}")
            return r
        except (requests.ConnectionError, requests.Timeout) as e:
            if i == retries - 1:
                raise
            log(f"POST 重試 {i+1}/{retries-1}: {e}")
            _sleep_backoff(i)
    raise RuntimeError("POST 重試已用盡")

def _is_finite_vector(vec: Optional[List[float]], dims: int) -> bool:
    if not isinstance(vec, list) or len(vec) != dims:
        return False
    return all(isinstance(x, (int, float)) and math.isfinite(float(x)) for x in vec)

# ========== 向量生成器 ==========
class VectorGenerator:
    """向量生成器"""
    
    def __init__(self, model: str):
        self.model = model
        if "text-embedding-3-large" in model:
            self.dimension = 3072
        else:
            self.dimension = 1536
    
    def generate(self, text: str) -> Optional[List[float]]:
        if client is None:
            log("❌ OpenAI client 未初始化")
            return None
        try:
            resp = client.embeddings.create(
                model=self.model,
                input=text[:8000],
                encoding_format="float",
            )
            return resp.data[0].embedding
        except Exception as e:
            log(f"⚠️ 向量生成失敗：{e}")
            return None
    
    def batch_generate(self, texts: List[str]) -> List[Optional[List[float]]]:
        if client is None:
            return [None for _ in texts]
        
        # 預處理
        processed: List[str] = []
        for t in texts:
            s = "" if t is None else str(t)
            s = s[:8000].strip()
            processed.append(s)
        
        # 建立過濾後的 inputs
        inputs: List[str] = []
        idx_map: List[int] = []
        for i, s in enumerate(processed):
            if s:
                inputs.append(s)
                idx_map.append(i)
        
        if not inputs:
            return [None for _ in texts]
        
        try:
            resp = client.embeddings.create(
                model=self.model,
                input=inputs,
                encoding_format="float",
            )
            result: List[Optional[List[float]]] = [None for _ in texts]
            for out_vec, orig_idx in zip([d.embedding for d in resp.data], idx_map):
                result[orig_idx] = out_vec
            return result
        except Exception as e:
            log(f"⚠️ 批量生成失敗：{e}")
            return [None for _ in texts]

# ========== ES 更新器 ==========
class ElasticsearchVectorUpdater:
    """Elasticsearch 向量更新器 - 優化文本提取版"""
    
    def __init__(self, vector_gen: VectorGenerator):
        self.vector_gen = vector_gen
        self.es_url = ES_URL
        self.index_pattern = INDEX_PATTERN
        self.dims = vector_gen.dimension
        self.session = requests.Session()
    
    def _list_indices(self, index_pattern: str) -> List[str]:
        try:
            r = http_get(f"{ES_URL}/_cat/indices/{index_pattern}", params={"format": "json"})
            if r.ok:
                return [row["index"] for row in r.json()]
        except Exception:
            pass
        try:
            r = http_get(f"{ES_URL}/{index_pattern}")
            if r.ok and isinstance(r.json(), dict):
                return list(r.json().keys())
        except Exception:
            pass
        return []
    
    def update_index_mapping(self, index_pattern: str = INDEX_PATTERN) -> None:
        """更新索引映射，添加向量欄位"""
        mapping_update = {
            "properties": {
                "content_vector": {
                    "type": "dense_vector",
                    "dims": self.vector_gen.dimension,
                    "index": True,
                    "similarity": "cosine",
                },
                "vector_generated_at": {"type": "date"},
            }
        }
        indices = self._list_indices(index_pattern)
        if not indices:
            log(f"ℹ️ 未找到符合的索引：{index_pattern}")
            return
        for index in indices:
            try:
                r = session.put(
                    f"{ES_URL}/{index}/_mapping",
                    json=mapping_update,
                    timeout=REQUESTS_TIMEOUT,
                )
                if r.ok:
                    log(f"✅ 已更新索引映射：{index}")
                else:
                    log(f"⚠️ 更新索引映射失敗：{index} {r.status_code}")
            except Exception as e:
                log(f"⚠️ 索引 {index} 映射更新例外：{e}")
    
    def find_documents_without_vectors(self, index_pattern: str = INDEX_PATTERN, 
                                      size: int = 100) -> List[Dict[str, Any]]:
        """搜尋尚未建立 content_vector 的文件"""
        query = {
            "size": size,
            "_source": True,
            "query": {"bool": {"must_not": [{"exists": {"field": "content_vector"}}]}},
            "sort": [{"_doc": "asc"}],
        }
        try:
            r = http_post(f"{ES_URL}/{index_pattern}/_search", json_body=query)
            if r.ok:
                body = r.json()
                hits = body.get("hits", {}).get("hits", [])
                if hits:
                    log(f"📋 找到 {len(hits)} 個文檔需要生成向量")
                return hits
            log(f"⚠️ 搜尋失敗 {r.status_code}")
        except Exception as e:
            log(f"⚠️ 搜尋例外：{e}")
        return []
    
    def _extract_text(self, source: Dict[str, Any], index_name: str) -> str:
        """根據索引類型提取最相關的文本 - 優化版"""
        
        # 根據索引名稱判斷類型
        if 'ecn-notice' in index_name:
            return self._extract_ecn_notice_text(source)
        elif 'ecn-application' in index_name:
            return self._extract_ecn_application_text(source)
        elif 'complaint' in index_name:
            return self._extract_complaint_text(source)
        elif 'fmea' in index_name:
            return self._extract_fmea_text(source)
        elif 'document' in index_name:
            return self._extract_structured_document_text(source)
        else:
            return self._extract_generic_text(source)
    
    def _extract_ecn_notice_text(self, source: Dict) -> str:
        """提取設變通知單的關鍵文本"""
        parts = []
        
        # 單號
        if source.get('notice_number'):
            parts.append(f"設變通知單 {source['notice_number']}")
        
        # 產品資訊
        if source.get('product_name'):
            parts.append(source['product_name'])
        if source.get('product_code'):
            parts.append(f"品號 {source['product_code']}")
        
        # 核心內容：設變說明
        if source.get('change_description'):
            parts.append(f"設變說明：{source['change_description']}")
        
        # 設變前後對比
        if source.get('before_change'):
            parts.append(f"設變前：{source['before_change']}")
        if source.get('after_change'):
            parts.append(f"設變後：{source['after_change']}")
        
        # 庫存處理
        if source.get('inventory_handling'):
            parts.append(f"庫存處理：{source['inventory_handling']}")
        
        # 申請人
        if source.get('applicant'):
            parts.append(f"申請人：{source['applicant']}")
        
        return ' '.join(filter(None, parts))
    
    def _extract_ecn_application_text(self, source: Dict) -> str:
        """提取設變申請單的關鍵文本"""
        parts = []
        
        # 單號
        if source.get('application_number'):
            parts.append(f"設變申請單 {source['application_number']}")
        
        # 產品資訊
        if source.get('product_name'):
            parts.append(source['product_name'])
        if source.get('product_code'):
            parts.append(f"品號 {source['product_code']}")
        
        # 核心內容：緣由
        if source.get('reason'):
            parts.append(f"緣由：{source['reason']}")
        
        # 設變項目
        if source.get('change_items'):
            parts.append(f"設變項目：{source['change_items']}")
        
        # 設變前後
        if source.get('change_before'):
            parts.append(f"設變前：{source['change_before']}")
        if source.get('change_after'):
            parts.append(f"設變後：{source['change_after']}")
        
        # 會議建議
        if source.get('meeting_suggestions'):
            parts.append(f"會議建議：{source['meeting_suggestions']}")
        
        # 審查說明
        if source.get('review_notes'):
            parts.append(f"審查說明：{source['review_notes']}")
        
        return ' '.join(filter(None, parts))
    
    def _extract_complaint_text(self, source: Dict) -> str:
        """提取客訴的關鍵文本"""
        parts = []
        
        # 單號
        if source.get('complaint_number'):
            parts.append(f"客訴單 {source['complaint_number']}")
        
        # 客戶資訊
        if source.get('customer_name'):
            parts.append(f"客戶：{source['customer_name']}")
        
        # 產品資訊
        if source.get('product_name'):
            parts.append(source['product_name'])
        if source.get('product_code'):
            parts.append(f"品號 {source['product_code']}")
        
        # 核心內容：抱怨描述
        if source.get('complaint_description'):
            parts.append(f"抱怨內容：{source['complaint_description']}")
        
        # 抱怨分析
        if source.get('complaint_analysis'):
            parts.append(f"分析：{source['complaint_analysis']}")
        
        # 承辦業務
        if source.get('responsible_sales'):
            parts.append(f"承辦：{source['responsible_sales']}")
        
        return ' '.join(filter(None, parts))
    
    def _extract_fmea_text(self, source: Dict) -> str:
        """提取 FMEA 的關鍵文本"""
        parts = []
        
        # 案號與類型
        if source.get('case_number'):
            analysis_type = source.get('analysis_type', 'FMEA')
            parts.append(f"{analysis_type} {source['case_number']}")
        
        # 案件名稱
        if source.get('case_name'):
            parts.append(source['case_name'])
        
        # 產品別
        if source.get('product_type'):
            parts.append(f"產品：{source['product_type']}")
        
        # 核心內容：分析項目
        if source.get('analysis_item'):
            parts.append(f"分析項目：{source['analysis_item']}")
        
        # 失效模式
        if source.get('failure_mode'):
            parts.append(f"失效模式：{source['failure_mode']}")
        
        # 失效影響
        if source.get('failure_effect'):
            parts.append(f"失效影響：{source['failure_effect']}")
        
        # 失效成因
        if source.get('failure_cause'):
            parts.append(f"失效成因：{source['failure_cause']}")
        
        # 風險評分（重要指標）
        risk_info = []
        if source.get('severity_s'):
            risk_info.append(f"嚴重度{source['severity_s']}")
        if source.get('occurrence_o'):
            risk_info.append(f"發生度{source['occurrence_o']}")
        if source.get('detection_d'):
            risk_info.append(f"難檢度{source['detection_d']}")
        if source.get('rpn'):
            risk_info.append(f"RPN{source['rpn']}")
        if risk_info:
            parts.append(' '.join(risk_info))
        
        # 對策方案（重要）
        if source.get('corrective_action'):
            parts.append(f"對策方案：{source['corrective_action']}")
        
        # 改善結果
        if source.get('improvement_result'):
            parts.append(f"改善結果：{source['improvement_result']}")
        
        # 是否為客訴案
        if source.get('is_customer_complaint'):
            parts.append("客訴案")
        
        # 負責人
        if source.get('responsible_person'):
            parts.append(f"負責人：{source['responsible_person']}")
        
        return ' '.join(filter(None, parts))
    
    def _extract_structured_document_text(self, source: Dict) -> str:
        """提取 structured_documents 的文本"""
        parts = []
        
        # 文檔編號
        if source.get('doc_number'):
            parts.append(source['doc_number'])
        
        # 產品資訊
        product_names = source.get('product_names')
        if isinstance(product_names, list):
            parts.extend(product_names[:3])
        elif product_names:
            parts.append(str(product_names))
        
        product_codes = source.get('product_codes')
        if isinstance(product_codes, list):
            parts.extend(product_codes[:3])
        
        # 摘要
        if source.get('summary'):
            parts.append(source['summary'])
        
        # 關鍵字
        keywords = source.get('keywords')
        if isinstance(keywords, list):
            parts.extend(keywords[:5])
        
        return ' '.join(filter(None, parts))
    
    def _extract_generic_text(self, source: Dict) -> str:
        """通用文本提取 (備用)"""
        priority_fields = [
            "summary", "description", "content", "title",
            "product_name", "complaint_description", "change_description"
        ]
        
        for field in priority_fields:
            if field in source and source[field]:
                text = str(source[field])
                if len(text) > 10:
                    return text
        
        # 備用：所有文本欄位
        text_parts = []
        for k, v in source.items():
            if isinstance(v, str) and len(v) > 0 and k not in ['_id', '_index']:
                text_parts.append(v)
        
        return ' '.join(text_parts[:5]) if text_parts else ""
    
    def update_document_vectors(self, docs: List[Dict[str, Any]]) -> Tuple[int, int]:
        """更新文檔向量"""
        if not docs:
            return (0, 0)
        
        # 提取文本
        texts = []
        for i, d in enumerate(docs):
            source = d.get("_source", {})
            index_name = d.get("_index", "")
            text = self._extract_text(source, index_name)
            texts.append(text)
            
            # 日誌預覽
            preview = text[:100].replace('\n', ' ')
            log(f"  文檔 {i+1}: {d['_id'][:8]}... 索引: {index_name} 文本長度: {len(text)} 預覽: {preview}")
        
        # 批次生成向量
        log(f"🔄 開始生成 {len(texts)} 個向量...")
        embeddings = self.vector_gen.batch_generate(texts)
        
        valid_count = sum(1 for e in embeddings if e is not None)
        log(f"  生成結果: {valid_count}/{len(embeddings)} 個有效向量")
        
        if valid_count == 0:
            log(f"❌ 所有向量生成失敗！")
            return (0, 0)
        
        # 準備寫入
        doc_ids = [d["_id"] for d in docs]
        indices = [d.get("_index") for d in docs]
        dims = self.vector_gen.dimension
        
        writer = ESVectorWriter(self.es_url, index=None, field="content_vector", session=session)
        ok, ng = writer.upsert_vectors(doc_ids, indices, embeddings, dims)
        
        if ok > 0:
            log(f"✅ 成功寫入 {ok} 筆向量")
        if ng > 0:
            log(f"❌ 失敗 {ng} 筆")
        
        return (ok, ng)

# ========== ES 向量寫入器 ==========
class ESVectorWriter:
    def __init__(self, base_url: str, index: str, field: str = "content_vector",
                 session: Optional[requests.Session] = None):
        self.base_url = base_url
        self.index = index
        self.field = field
        self.session = session or requests.Session()
    
    def upsert_vectors(self, ids: List[str], indices: List[str], 
                      vectors: List[Optional[List[float]]], dims: int) -> Tuple[int, int]:
        """批次寫入向量"""
        assert len(indices) == len(ids) == len(vectors)
        lines: List[str] = []
        skip_count = 0
        
        for idx, _id, vec in zip(indices, ids, vectors):
            if not idx or "*" in idx or "?" in idx:
                skip_count += 1
                continue
            if not _is_finite_vector(vec, dims):
                skip_count += 1
                continue
            
            lines.append(json.dumps({"update": {"_index": idx, "_id": _id}}))
            lines.append(json.dumps({
                "doc": {
                    self.field: vec,
                    "vector_generated_at": datetime.utcnow().isoformat()
                },
                "doc_as_upsert": True
            }))
        
        if not lines:
            return (0, skip_count)
        
        try:
            bulk_data = "\n".join(lines) + "\n"
            r = self.session.post(
                f"{self.base_url}/_bulk",
                data=bulk_data,
                headers={"Content-Type": "application/x-ndjson"},
                timeout=REQUESTS_TIMEOUT
            )
            
            if r.ok:
                result = r.json()
                success = sum(1 for item in result.get("items", []) 
                            if "error" not in item.get("update", {}))
                failed = len(lines) // 2 - success
                return (success, failed)
            else:
                log(f"❌ bulk 請求失敗: {r.status_code} - {r.text[:200]}")
                return (0, len(lines) // 2)
                
        except Exception as e:
            log(f"❌ 批次寫入失敗: {e}")
            return (0, len(lines) // 2)

# ========== 信號處理 ==========
def _handle_sigterm(signum, frame):
    global _SHOULD_STOP
    _SHOULD_STOP = True
    log("收到停止訊號，準備結束…")

# ========== 主流程 ==========
def main() -> None:
    if not OPENAI_API_KEY:
        log("❌ 未設置 OPENAI_API_KEY")
        return
    if client is None:
        log("❌ OpenAI 套件未正確安裝")
        return
    
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)
    
    log("=" * 60)
    log("🚀 向量服務啟動")
    log(f"📊 模型：{EMBEDDING_MODEL}")
    log(f"🔍 索引模式：{INDEX_PATTERN}")
    log(f"📦 批次大小：{BATCH_SIZE}")
    log(f"🤖 自動停止：{'啟用' if AUTO_STOP_ENABLED else '停用'}")
    if AUTO_STOP_ENABLED:
        log(f"   連續空輪上限：{AUTO_STOP_EMPTY_ROUNDS} 次")
    log("=" * 60)
    
    try:
        wait_for_es()
    except Exception as e:
        log(f"❌ 等待 Elasticsearch 失敗：{e}")
        return
    
    vg = VectorGenerator(EMBEDDING_MODEL)
    updater = ElasticsearchVectorUpdater(vg)
    updater.update_index_mapping(INDEX_PATTERN)
    
    # 自動停止計數器
    empty_rounds = 0
    total_processed = 0
    
    while not _SHOULD_STOP:
        try:
            docs = updater.find_documents_without_vectors(INDEX_PATTERN, size=BATCH_SIZE)
            if docs:
                # 找到文檔，重置計數器
                empty_rounds = 0
                ok_count, _ = updater.update_document_vectors(docs)
                total_processed += ok_count
            else:
                # 沒有找到文檔
                empty_rounds += 1
                log(f"😴 所有文檔都已有向量 (空輪 {empty_rounds}/{AUTO_STOP_EMPTY_ROUNDS if AUTO_STOP_ENABLED else '∞'})")
                
                # 檢查是否需要自動停止
                if AUTO_STOP_ENABLED and empty_rounds >= AUTO_STOP_EMPTY_ROUNDS:
                    log("=" * 60)
                    log(f"✅ 完成！所有文檔都已有向量")
                    log(f"📊 本次運行共處理 {total_processed} 個文檔")
                    log(f"🛑 已連續 {empty_rounds} 輪無新文檔，自動停止服務")
                    log("=" * 60)
                    break
                    
        except Exception as e:
            log(f"❌ 主循環錯誤：{e}")
            # 錯誤時不計入空輪次
        
        time.sleep(SLEEP_SEC)
    
    log("👋 向量服務結束")

if __name__ == "__main__":
    main()
