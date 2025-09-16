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

import os, time, json, signal, requests
from datetime import datetime
from typing import List, Dict, Any, Optional
from requests.auth import HTTPBasicAuth

try:
    from openai import OpenAI
except Exception:  # 避免環境暫無 openai 套件
    OpenAI = None  # type: ignore

# -----------------------------
# 環境變數
# -----------------------------
ES_URL = os.environ.get("ES_URL", "http://elasticsearch:9200").rstrip("/")
ES_USER = os.getenv("ES_USER")
ES_PASS = os.getenv("ES_PASS")
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
session.headers.update({"Content-Type": "application/json"})

client: Optional[OpenAI] = None
if OPENAI_API_KEY and OpenAI is not None:
    client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL)

_SHOULD_STOP = False

# -----------------------------
# 工具方法
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
        try:
            resp = client.embeddings.create(
                model=self.model,
                input=[t[:8000] for t in texts],
                encoding_format="float",
            )
            return [d.embedding for d in resp.data]  # type: ignore[attr-defined]
        except Exception as e:
            log(f"⚠️ 批量生成失敗，改為逐筆：{e}")
            out: List[Optional[List[float]]] = []
            for t in texts:
                out.append(self.generate(t))
                time.sleep(0.1)
            return out


# -----------------------------
# ES 更新器（保留原類別/方法名）
# -----------------------------
class ElasticsearchVectorUpdater:
    """Elasticsearch 向量更新器"""

    def __init__(self, vector_gen: VectorGenerator):
        self.vector_gen = vector_gen

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
        """搜尋尚未建立 content_vector 的文件"""
        query = {
            "size": size,
            "_source": ["searchable_content", "all_content", "field_*"],
            "query": {"bool": {"must_not": [{"exists": {"field": "content_vector"}}]}},
            "sort": [{"_doc": "asc"}],
        }
        try:
            r = http_post(f"{ES_URL}/{index_pattern}/_search", json_body=query)
            if r.ok:
                body = r.json()
                return body.get("hits", {}).get("hits", [])  # type: ignore[no-any-return]
            log(f"⚠️ 搜尋失敗 {r.status_code}: {r.text[:200]}")
        except Exception as e:
            log(f"⚠️ 搜尋例外：{e}")
        return []

    def _extract_text(self, source: Dict[str, Any]) -> str:
        # 優先使用描述性欄位
        priority_fields = [
            "field_description",
            "field_product_name",
            "field_complaint_type",
        ]
        text_parts = []

        for field in priority_fields:
            if field in source and source[field]:
                text_parts.append(str(source[field]))

        return " ".join(text_parts) if text_parts else source.get("all_content", "")

    def update_document_vectors(self, documents: List[Dict[str, Any]]) -> None:
        """更新文檔向量"""
        if not documents:
            return
        texts = [self._extract_text(doc.get("_source", {})) for doc in documents]
        log(f"🔄 生成 {len(texts)} 個向量…")
        embeddings = self.vector_gen.batch_generate(texts)

        # 構建 _bulk 請求
        lines: List[str] = []
        for doc, emb in zip(documents, embeddings):
            if not emb:
                continue
            lines.append(
                json.dumps(
                    {"update": {"_index": doc.get("_index"), "_id": doc.get("_id")}}
                )
            )
            lines.append(
                json.dumps(
                    {
                        "doc": {
                            "content_vector": emb,
                            "vector_generated_at": datetime.utcnow().isoformat(),
                        }
                    }
                )
            )
        if not lines:
            log("ℹ️ 沒有可更新的向量（文本為空或全部失敗）")
            return
        payload = "\n".join(lines) + "\n"
        try:
            r = http_post(
                f"{ES_URL}/_bulk",
                data=payload,
                headers={"Content-Type": "application/x-ndjson"},
            )
            if r.ok:
                res = r.json()
                if not res.get("errors"):
                    log(f"✅ 成功更新 {len(documents)} 個文檔的向量")
                else:
                    fails = sum(
                        1
                        for it in res.get("items", [])
                        if any(v.get("error") for v in it.values())
                    )
                    log(f"⚠️ 部分更新失敗：{fails}/{len(documents)}")
            else:
                log(f"❌ 批量更新失敗 {r.status_code}: {r.text[:200]}")
        except Exception as e:
            log(f"❌ 批量更新例外：{e}")


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
            log(f"❌ 主循環錯誤: {e}")
        time.sleep(SLEEP_SEC)

    log("👋 向量服務結束。")


if __name__ == "__main__":
    main()
