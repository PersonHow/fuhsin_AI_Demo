#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
向量生成服務 - 使用 OpenAI API 生成文本向量（依照原始結構加強穩定性）

本版重點：
- 啟動時等待 Elasticsearch 就緒（yellow/green）
- 對 ES 請求加入重試（指數退避），避免短暫 50x/連線錯誤造成容器重啟
- 依模型自動設定 dense_vector 維度（text-embedding-3-small=1536, 3-large=3072）
- 批次搜尋缺向量文件並以 _bulk 更新
"""
from __future__ import annotations

import os, time, json, signal, requests, math, re, hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from requests.auth import HTTPBasicAuth

try:
    from openai import OpenAI
except Exception:  # 避免環境暫無 openai 套件
    OpenAI = None  # type: ignore

# -----------------------------
# 環境變數
# -----------------------------
ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
ES_USER = os.environ.get("ES_USER", "elastic")
ES_PASS = os.environ.get("ES_PASS", "admin@12345")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip(
    "/"
)
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
INDEX_PATTERN = os.environ.get("INDEX_PATTERN", "erp-*")
BATCH_SIZE = int(os.environ.get("VECTOR_BATCH_SIZE", "100"))
SLEEP_SEC = int(os.environ.get("SLEEP", "10"))
ES_WAIT_TIMEOUT = int(os.environ.get("ES_WAIT_TIMEOUT", "180"))
REQUESTS_TIMEOUT = int(os.environ.get("REQUESTS_TIMEOUT", "30"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))

# -----------------------------
# 連線物件
# -----------------------------
session = requests.Session()
if ES_USER and ES_PASS:
    session.auth = HTTPBasicAuth(ES_USER, ES_PASS)
# session.headers.update({"Content-Type": "application/json"})

client: Optional[OpenAI] = None
if OPENAI_API_KEY and OpenAI is not None:
    client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL)

_SHOULD_STOP = False

# -----------------------------
# 工具方法 (utils 區域)
# -----------------------------


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def wait_for_es(timeout_sec: int = ES_WAIT_TIMEOUT) -> None:
    """等待 Elasticsearch 至少達到 yellow 健康狀態。超時則丟例外。"""
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
    time.sleep(base * (2**i))  # 1,2,4,8,…


def http_get(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    retries: int = MAX_RETRIES,
) -> requests.Response:
    for i in range(retries):
        try:
            r = session.get(
                url, params=params, headers=headers, timeout=REQUESTS_TIMEOUT
            )
            if r.status_code in (502, 503, 504):
                raise requests.ConnectionError(f"Transient {r.status_code}")
            return r
        except (requests.ConnectionError, requests.Timeout) as e:
            if i == retries - 1:
                raise
            log(f"GET 重試 {i+1}/{retries-1}: {e}")
            _sleep_backoff(i)
    raise RuntimeError("GET 重試已用盡")


def http_post(
    url: str,
    *,
    json_body: Optional[Dict[str, Any]] = None,
    data: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    retries: int = MAX_RETRIES,
) -> requests.Response:
    for i in range(retries):
        try:
            r = session.post(
                url,
                json=json_body,
                data=data,
                headers=headers,
                timeout=REQUESTS_TIMEOUT,
            )
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


# -----------------------------
# 向量生成器（保留原結構/命名）
# -----------------------------
class VectorGenerator:
    """向量生成器"""

    def __init__(self, model: str):
        self.model = model
        if "text-embedding-3-large" in model:
            self.dimension = 3072
        else:
            self.dimension = 1536  # text-embedding-3-small / ada-002 相容

    def generate(self, text: str) -> Optional[List[float]]:
        if client is None:
            log("❌ OpenAI client 未初始化，請確認 OPENAI_API_KEY 與 openai 套件。")
            return None
        try:
            resp = client.embeddings.create(
                model=self.model,
                input=text[:8000],
                encoding_format="float",
            )
            return resp.data[0].embedding  # type: ignore[no-any-return]
        except Exception as e:
            log(f"⚠️ 向量生成失敗：{e}")
            return None

    def batch_generate(self, texts: List[str]) -> List[Optional[List[float]]]:
        if client is None:
            return [None for _ in texts]

        # 預處理：統一成字串、截長、去空白
        processed: List[str] = []
        for t in texts:
            s = "" if t is None else str(t)
            s = s[:8000].strip()
            processed.append(s)

        # 建立過濾後的 inputs 與索引映射
        inputs: List[str] = []
        idx_map: List[int] = []
        for i, s in enumerate(processed):
            if s:  # 非空才送進 API（避免 ["", ...] 致命 400）
                inputs.append(s)
                idx_map.append(i)

        # 若全部是空字串，直接回傳全 None
        if not inputs:
            return [None for _ in texts]

        try:
            resp = client.embeddings.create(
                model=self.model,
                input=inputs,
                encoding_format="float",
            )
            result: List[Optional[List[float]]] = [None for _ in texts]
            for out_vec, orig_idx in zip([d.embedding for d in resp.data], idx_map):  # type: ignore[attr-defined]
                result[orig_idx] = out_vec
            return result
        except Exception as e:
            log(f"⚠️ 批量生成失敗，改為逐筆：{e}")
            out: List[Optional[List[float]]] = []
            for s in processed:
                if not s:
                    out.append(None)
                    continue
                out.append(self.generate(s))
                time.sleep(0.1)
            return out


# -----------------------------
# ES 更新器（保留原類別/方法名）
# -----------------------------
class ElasticsearchVectorUpdater:
    """Elasticsearch 向量更新器"""

    def __init__(self, vector_gen: VectorGenerator):
        self.vector_gen = vector_gen
        self.es_url = ES_URL
        self.index_pattern = INDEX_PATTERN
        self.dims = vector_gen.dimension
        self.session = requests.Session()

    def _list_indices(self, index_pattern: str) -> List[str]:
        # 優先用 _cat/indices；若失敗再退回 GET /{pattern}
        try:
            r = http_get(
                f"{ES_URL}/_cat/indices/{index_pattern}", params={"format": "json"}
            )
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
            log(f"ℹ️ 未找到符合的索引：{index_pattern}，稍後資料寫入再補 mapping")
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
                    log(f"⚠️ 更新索引映射失敗：{index} {r.status_code} {r.text[:200]}")
            except Exception as e:
                log(f"⚠️ 索引 {index} 映射更新例外：{e}")

    def find_documents_without_vectors(
        self, index_pattern: str = INDEX_PATTERN, size: int = 100
    ) -> List[Dict[str, Any]]:
        """搜尋尚未建立 content_vector 的文件 - 修復版"""
        query = {
            "size": size,
            # 修復：不限制 _source，取得所有欄位
            "_source": True,  
            "query": {"bool": {"must_not": [{"exists": {"field": "content_vector"}}]}},
            "sort": [{"_doc": "asc"}],
        }
        try:
            r = http_post(f"{ES_URL}/{index_pattern}/_search", json_body=query)
            if r.ok:
                body = r.json()
                hits = body.get("hits", {}).get("hits", [])
                
                # 加入偵錯日誌
                if hits:
                    log(f"📋 找到 {len(hits)} 個文檔，範例欄位: {list(hits[0]['_source'].keys())[:10]}")
                
                return hits
            log(f"⚠️ 搜尋失敗 {r.status_code}: {r.text[:200]}")
        except Exception as e:
            log(f"⚠️ 搜尋例外：{e}")
        return []

    def _extract_text(self, source: Dict[str, Any]) -> str:
        """修復版文本提取 - 更全面的欄位處理"""
        def _to_text(x) -> str:
            if x is None:
                return ""
            if isinstance(x, str):
                return x
            if isinstance(x, (list, tuple, set)):
                return " ".join(map(str, x))
            if isinstance(x, dict):
                return " ".join(map(str, x.values()))
            return str(x)

        # 擴展優先欄位列表
        priority_fields = [
            # 產品相關
            "product_id", "product_ids", "product_name", "product_names",
            "field_product_id", "field_product_name",
            # 描述相關
            "description", "summary", "title", "content", "text",
            "field_description", "field_summary",
            # 客訴相關
            "complaint_type", "complaint_description", "complaint_content",
            "field_complaint_type", "field_complaint_description",
            # 文件相關
            "file_name", "document_name", "doc_type",
            # 狀態相關
            "status", "field_status", "handling_status",
            # 可搜尋內容
            "searchable_content", "all_content"
        ]
        
        text_parts = []
        
        # 嘗試從優先欄位提取
        for field in priority_fields:
            if field in source and source[field]:
                value = _to_text(source[field])
                if value and value.strip():  # 確保不是空白字串
                    text_parts.append(value)
        
        # 如果沒有找到任何文本，嘗試提取所有 field_ 開頭的欄位
        if not text_parts:
            for key, value in source.items():
                if key.startswith("field_") and value:
                    value_text = _to_text(value)
                    if value_text and value_text.strip():
                        text_parts.append(value_text)
        
        # 如果還是沒有，嘗試使用所有非系統欄位
        if not text_parts:
            skip_fields = {"_id", "_index", "_type", "_score", "content_vector", "vector_generated_at"}
            for key, value in source.items():
                if key not in skip_fields and value:
                    value_text = _to_text(value)
                    if value_text and value_text.strip():
                        text_parts.append(value_text)
        
        result = " ".join(text_parts)
        
        # 記錄警告如果文本太短
        if len(result) < 10:
            log(f"⚠️ 文本過短 ({len(result)} 字元)，可能影響向量品質")
            
        return result

    def update_document_vectors(self, docs: list[dict]) -> tuple[int, int]:
        """修復版向量更新 - 加入更多錯誤處理"""
        if not docs:
            return (0, 0)
            
        # 提取文本並記錄
        texts = []
        for i, d in enumerate(docs):
            text = self._extract_text(d["_source"])
            texts.append(text)
            
            # 偵錯：記錄前幾個文本範例
            if i < 3:
                preview = text[:100] + "..." if len(text) > 100 else text
                log(f"  文檔 {i+1}: {d['_id'][:8]}... 文本長度: {len(text)} 預覽: {preview}")
        
        # 批次生成向量
        log(f"🔄 開始生成 {len(texts)} 個向量...")
        embeddings = self.vector_gen.batch_generate(texts)
        
        # 檢查生成結果
        valid_count = sum(1 for e in embeddings if e is not None)
        log(f"  生成結果: {valid_count}/{len(embeddings)} 個有效向量")
        
        if valid_count == 0:
            log(f"❌ 所有向量生成失敗！請檢查 OpenAI API")
            return (0, 0)
        
        # 準備寫入資料
        doc_ids = [d["_id"] for d in docs]
        indices = [d.get("_index") for d in docs]
        
        # 修復：使用正確的維度
        dims = self.vector_gen.dimension
        log(f"  使用維度: {dims} (模型: {self.vector_gen.model})")
        
        # 寫入向量
        writer = ESVectorWriter(
            self.es_url,
            index=None,
            field="content_vector",
            session=session,
        )
        
        ok, ng = writer.upsert_vectors(doc_ids, indices, embeddings, dims)
        
        # 詳細記錄結果
        if ok > 0:
            log(f"✅ 成功寫入 {ok} 筆向量")
        if ng > 0:
            log(f"❌ 失敗 {ng} 筆")
            
        # 如果全部失敗，顯示更多偵錯資訊
        if ok == 0 and ng == 0 and valid_count > 0:
            log(f"⚠️ 有 {valid_count} 個有效向量但寫入 0 筆，可能的原因：")
            log(f"  - 索引名稱問題: {set(indices[:5])}")
            log(f"  - 文檔 ID 問題: {doc_ids[:5]}")
            log(f"  - 向量維度不符: 期望 {dims}")
            
        return (ok, ng)


class ESVectorWriter:
    def __init__(
        self,
        base_url: str,
        index: str,
        field: str = "content_vector",
        session: Optional[requests.Session] = None,
    ):
        self.base_url = base_url
        self.index = index
        self.field = field
        self.session = session or requests.Session()

    def upsert_vectors(
        self, ids: List[str], indices: List[str], vectors: List[Optional[List[float]]], dims: int
    ) -> Tuple[int, int]:
        """修復版批次寫入"""
        assert len(indices) == len(ids) == len(vectors)
        lines: List[str] = []
        skip_count = 0
        
        for idx, _id, vec in zip(indices, ids, vectors):
            # 檢查索引
            if not idx or "*" in idx or "?" in idx:
                skip_count += 1
                log(f"  跳過：索引無效 '{idx}'")
                continue
                
            # 檢查 ID
            if not _id:
                skip_count += 1
                log(f"  跳過：ID 為空")
                continue
                
            # 檢查向量
            if not _is_finite_vector(vec, dims):
                skip_count += 1
                if vec is None:
                    log(f"  跳過：向量為 None (ID: {_id[:8]}...)")
                else:
                    log(f"  跳過：向量無效 (ID: {_id[:8]}..., 維度: {len(vec) if isinstance(vec, list) else 'N/A'})")
                continue

            # 準備批次更新資料
            action = {"update": {"_index": idx, "_id": str(_id)}}
            doc = {
                "doc": {
                    self.field: vec,
                    "vector_generated_at": datetime.now().isoformat()
                }, 
                "doc_as_upsert": True
            }
            lines.append(json.dumps(action, ensure_ascii=False))
            lines.append(json.dumps(doc, ensure_ascii=False))

        if skip_count > 0:
            log(f"  共跳過 {skip_count} 筆無效資料")
            
        if not lines:
            return (0, 0)

        # 執行批次更新
        body = "\n".join(lines) + "\n"
        try:
            resp = self.session.post(
                f"{self.base_url}/_bulk",
                data=body,
                headers={"Content-Type": "application/x-ndjson"},
                params={"refresh": "wait_for"},
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()

            success, failed = 0, 0
            bad_msgs = []
            
            for it in data.get("items", []):
                op, detail = next(iter(it.items()))
                if "error" in detail:
                    failed += 1
                    if len(bad_msgs) < 5:
                        error_msg = detail["error"]
                        if isinstance(error_msg, dict):
                            error_msg = error_msg.get("reason", str(error_msg))
                        bad_msgs.append(error_msg)
                else:
                    success += 1

            if data.get("errors") and bad_msgs:
                log(f"❗ Bulk 錯誤（前5）：")
                for i, msg in enumerate(bad_msgs, 1):
                    log(f"    {i}. {msg}")

            return (success, failed)
            
        except Exception as e:
            log(f"❌ 批次寫入失敗: {e}")
            return (0, len(lines) // 2)  # 每個文檔產生 2 行


# -----------------------------
# 主流程（保留你的呼叫路徑）
# -----------------------------


def _handle_sigterm(signum, frame):
    global _SHOULD_STOP
    _SHOULD_STOP = True
    log("收到停止訊號，準備結束…")


def main() -> None:
    if not OPENAI_API_KEY:
        log("❌ 未設置 OPENAI_API_KEY，無法生成向量。")
        return
    if client is None:
        log("❌ OpenAI 套件未正確安裝或初始化。請確認 'pip install openai' 版本支援。")
        return

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    log("🚀 向量服務啟動")
    log(f"📊 模型：{EMBEDDING_MODEL}，索引模式：{INDEX_PATTERN}")

    try:
        wait_for_es()
    except Exception as e:
        log(f"❌ 等待 Elasticsearch 失敗：{e}")
        return

    vg = VectorGenerator(EMBEDDING_MODEL)
    updater = ElasticsearchVectorUpdater(vg)
    updater.update_index_mapping(INDEX_PATTERN)

    while not _SHOULD_STOP:
        try:
            docs = updater.find_documents_without_vectors(
                INDEX_PATTERN, size=BATCH_SIZE
            )
            if docs:
                log(f"📝 找到 {len(docs)} 個需要生成向量的文檔")
                updater.update_document_vectors(docs)
            else:
                log("😴 所有文檔都已有向量，等待中…")
        except Exception as e:
            log(f"❌ 主循環錯誤訊息: {e}")
        time.sleep(SLEEP_SEC)

    log("👋 向量服務結束。")


if __name__ == "__main__":
    main()
