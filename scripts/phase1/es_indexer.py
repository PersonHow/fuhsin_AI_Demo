#!/usr/bin/env python3
"""
Elasticsearch 索引管理器
處理所有 ES 相關操作
"""

import json, math
import time
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, date
from decimal import Decimal

try:
    import numpy as np
except Exception:
    np = None

def _json_default(o):
    # pandas / numpy 時間
    try:
        import pandas as pd
        if isinstance(o, (pd.Timestamp,)):
            return o.to_pydatetime().isoformat()
    except Exception:
        pass

    # 原生 datetime / date
    if isinstance(o, (datetime, date)):
        # 若沒有 tz，至少給 ISO 格式（ES 亦可吃）
        return o.isoformat()

    # Decimal -> float（或改成 str 依你需求）
    if isinstance(o, Decimal):
        return float(o)

    # numpy 數值/布林
    if np is not None:
        if isinstance(o, (np.integer,)):
            return int(o)
        if isinstance(o, (np.floating,)):
            # 注意 NaN/inf
            return None if (math.isnan(float(o)) or math.isinf(float(o))) else float(o)
        if isinstance(o, (np.bool_,)):
            return bool(o)

    # 其他不可序列化型別，最後退為字串
    return str(o)

def _clean_nan(obj):
    """把 NaN/NaT 統一轉 None，避免最後變成 'NaN' 字串。"""
    try:
        import pandas as pd
    except Exception:
        pd = None

    if isinstance(obj, dict):
        return {k: _clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_nan(v) for v in obj]
    # pandas NaT/NaN
    if pd is not None and getattr(pd, "isna", None) and pd.isna(obj):
        return None
    # numpy NaN/inf
    if np is not None:
        if isinstance(obj, (np.floating,)) and (math.isnan(float(obj)) or math.isinf(float(obj))):
            return None
    return obj

class ElasticsearchIndexer:
    """Elasticsearch 索引管理器"""

    def __init__(self, config: Dict[str, Any], logger):
        """
        初始化 ES 索引器

        Args:
            config: ES 設定
            logger: 日誌記錄器
        """
        self.config = config
        self.logger = logger
        self.base_url = f"http://{config['host']}:{config['port']}"

        # 建立 session
        self.session = requests.Session()
        self.session.auth = (config["user"], config["password"])
        self.session.headers.update({"Content-Type": "application/json"})

        self._wait_for_elasticsearch()
        self._check_connection()

    def _wait_for_elasticsearch(self, max_retries: int = 30):
        """等待 Elasticsearch 啟動"""
        for i in range(max_retries):
            try:
                response = self.session.get(
                    f"{self.base_url}/_cluster/health", timeout=5
                )
                if response.status_code == 200:
                    self.logger.info("✓ Elasticsearch 已就緒")
                    return
            except:
                pass

            self.logger.info(f"等待 Elasticsearch 啟動... ({i+1}/{max_retries})")
            time.sleep(2)

        raise Exception("Elasticsearch 啟動超時")

    def _check_connection(self):
        """檢查 ES 連接"""
        try:
            response = self.session.get(f"{self.base_url}/_cluster/health")
            response.raise_for_status()

            health = response.json()
            self.logger.info(f"✓ Elasticsearch 連接成功")
            self.logger.info(f"  叢集名稱：{health['cluster_name']}")
            self.logger.info(f"  叢集狀態：{health['status']}")
            self.logger.info(f"  節點數量：{health['number_of_nodes']}")

        except Exception as e:
            self.logger.error(f"Elasticsearch 連接失敗：{str(e)}")
            raise

    def create_index_template(self, template_name: str, template_config: Dict):
        """建立索引模板"""
        template = {
            "index_patterns": [f"{template_name}-*"],
            "template": {
                "settings": {
                    "number_of_shards": template_config["settings"].get(
                        "number_of_shards", 1
                    ),
                    "number_of_replicas": template_config["settings"].get(
                        "number_of_replicas", 0
                    ),
                    "refresh_interval": template_config["settings"].get(
                        "refresh_interval", "1s"
                    ),
                    "analysis": {
                        "analyzer": {
                            "ik_analyzer": {"type": "ik_max_word"},
                            "ik_search": {"type": "ik_smart"},
                        }
                    },
                }
            },
        }

        try:
            response = self.session.put(
                f"{self.base_url}/_index_template/{template_name}-template",
                json=template,
                timeout=30,
            )
            response.raise_for_status()
            self.logger.info(f"✓ 索引模板已建立：{template_name}-template")

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                self.logger.debug(f"索引模板已存在：{template_name}-template")
            else:
                self.logger.error(f"建立索引模板失敗：{str(e)}")
                raise

    def create_index_if_not_exists(
        self, index_name: str, table_config: Dict, template_config: Dict
    ):
        """建立索引（如果不存在）"""
        # 檢查索引是否存在
        if self.index_exists(index_name):
            self.logger.debug(f"索引已存在：{index_name}")
            return

        # 建立索引映射
        mappings = self._build_mappings(table_config, template_config)

        index_body = {
            "settings": {
                "number_of_shards": template_config["settings"].get(
                    "number_of_shards", 1
                ),
                "number_of_replicas": template_config["settings"].get(
                    "number_of_replicas", 0
                ),
                "refresh_interval": template_config["settings"].get(
                    "refresh_interval", "1s"
                ),
                "analysis": {
                    "analyzer": {
                        "ik_analyzer": {"type": "ik_max_word"},
                        "ik_search": {"type": "ik_smart"},
                    }
                },
            },
            "mappings": mappings,
        }

        def _ensure_jsonable_keys(obj, path="root"):
            if isinstance(obj, dict):
                fixed = {}
                for key, value in obj.items():
                    noKey = key if isinstance(key, (str, int, float, bool)) or key is None else str(key)
                    if noKey is not key:
                        self.logger.warning(f"非字串 Key 於 {path}: {key!r} -> {noKey!r}")
                    fixed[noKey] = _ensure_jsonable_keys(value, f"{path}.{noKey}")
                return fixed
            elif isinstance(obj, list):
                return [_ensure_jsonable_keys(i, f"{path}[]") for i in obj]
            return obj
        index_body = _ensure_jsonable_keys(index_body)

        try:
            response = self.session.put(
                f"{self.base_url}/{index_name}", json=index_body, timeout=30
            )
            response.raise_for_status()
            self.logger.info(f"✓ 索引已建立：{index_name}")

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"建立索引失敗：{str(e)}")
            self.logger.error(f"響應內容：{e.response.text}")
            raise

    def _build_mappings(self, table_config: Dict, template_config: Dict) -> Dict:
        """建立索引映射"""
        mappings = {
            "dynamic": True,
            "properties": {
                "_metadata": {
                    "properties": {
                        "source_table": {"type": "keyword"},
                        "indexed_at": {"type": "date"},
                        "sync_version": {"type": "integer"},
                        "search_weights": {"type": "object", "enabled": False},
                    }
                },
                "_search_content": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart",
                },
            },
        }

        # 添加搜尋欄位的特殊映射（不使用 boost 參數）
        if "search_fields" in table_config:
            sf = table_config["search_fields"]
            if isinstance(sf, dict):
                field_names = list(sf.keys())
            elif isinstance(sf, (list, tuple)):
                field_names = list(sf)
            else:
                field_names = []
            for field_name in field_names:
                mappings["properties"][str(field_name)] = {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                }

        return mappings

    def index_exists(self, index_name: str) -> bool:
        """檢查索引是否存在"""
        try:
            response = self.session.head(f"{self.base_url}/{index_name}")
            return response.status_code == 200
        except:
            return False

    def delete_index(self, index_name: str) -> bool:
        """刪除索引"""
        try:
            response = self.session.delete(f"{self.base_url}/{index_name}")
            response.raise_for_status()
            self.logger.info(f"✓ 索引已刪除：{index_name}")
            return True
        except Exception as e:
            self.logger.error(f"刪除索引失敗：{str(e)}")
            return False

    def bulk_index(self, index_name: str, documents: List[Dict]) -> int:
        """批量索引文檔"""
        if not documents:
            return 0

        # 建立批量請求
        bulk_data = []
        for doc in documents:
            # 索引動作
            action = {"index": {"_index": index_name, "_id": doc["_id"]}}
            # action 通常不用 default
            bulk_data.append(json.dumps(action, ensure_ascii=False))
            bulk_data.append(json.dumps(doc["_source"], ensure_ascii=False, default=_json_default))

        # 發送批量請求
        bulk_body = "\n".join(bulk_data) + "\n"

        try:
            response = self.session.post(
                f"{self.base_url}/_bulk",
                data=bulk_body.encode("utf-8"),
                headers={"Content-Type": "application/x-ndjson"},
                timeout=60,
            )
            response.raise_for_status()

            result = response.json()

            # 統計結果
            success_count = 0
            error_items = []

            # 查驗 item["index"]["status"] 與 有沒有 error
            for item in result.get("items", []):
                index = item.get("index", {})
                status = index.get("status")
                if 200 <= (status or 0) < 300 and not index.get("error"):
                    success_count += 1
                else:
                    error_items.append(index)

            if error_items:
                self.logger.warning(f"部分文檔索引失敗：{len(error_items)} 個")
                self.logger.debug(f"錯誤詳情：{error_items[:5]}")  # 只顯示前5個錯誤

            return success_count

        except Exception as e:
            self.logger.error(f"批量索引失敗：{str(e)}")
            raise

    def search(self, index_name: str, query: Dict, size: int = 10) -> Dict:
        """執行搜尋"""
        search_body = {"size": size, "query": query}

        try:
            response = self.session.post(
                f"{self.base_url}/{index_name}/_search",
                data=json.dumps(search_body),
                timeout=30,
            )
            response.raise_for_status()

            result = response.json()
            self.logger.debug(
                f"搜尋完成，找到 {result['hits']['total']['value']} 個結果"
            )
            return result

        except Exception as e:
            self.logger.error(f"搜尋失敗：{str(e)}")
            raise

    def count(self, index_name: str) -> int:
        """計算索引中的文檔數量"""
        try:
            response = self.session.get(f"{self.base_url}/{index_name}/_count")
            response.raise_for_status()

            result = response.json()
            return result["count"]

        except Exception as e:
            self.logger.error(f"計數失敗：{str(e)}")
            return 0

    def get_index_stats(self, index_name: str) -> Dict:
        """取得索引統計資訊"""
        try:
            response = self.session.get(f"{self.base_url}/{index_name}/_stats")
            response.raise_for_status()

            stats = response.json()
            index_stats = stats["indices"][index_name]

            return {
                "document_count": index_stats["primaries"]["docs"]["count"],
                "deleted_count": index_stats["primaries"]["docs"]["deleted"],
                "size_in_bytes": index_stats["primaries"]["store"]["size_in_bytes"],
                "size_in_mb": round(
                    index_stats["primaries"]["store"]["size_in_bytes"] / 1024 / 1024, 2
                ),
            }

        except Exception as e:
            self.logger.error(f"取得索引統計失敗：{str(e)}")
            return {}

    def update_document(self, index_name: str, doc_id: str, doc: Dict):
        """更新單一文檔"""
        try:
            response = self.session.post(
                f"{self.base_url}/{index_name}/_update/{doc_id}",
                data=json.dumps({"doc": doc}),
                timeout=30,
            )
            response.raise_for_status()
            self.logger.debug(f"文檔已更新：{index_name}/{doc_id}")

        except Exception as e:
            self.logger.error(f"更新文檔失敗：{str(e)}")
            raise

    def delete_document(self, index_name: str, doc_id: str):
        """刪除單一文檔"""
        try:
            response = self.session.delete(
                f"{self.base_url}/{index_name}/_doc/{doc_id}", timeout=30
            )
            response.raise_for_status()
            self.logger.debug(f"文檔已刪除：{index_name}/{doc_id}")

        except Exception as e:
            self.logger.error(f"刪除文檔失敗：{str(e)}")
            raise

    def refresh_index(self, index_name: str):
        """刷新索引"""
        try:
            response = self.session.post(
                f"{self.base_url}/{index_name}/_refresh", timeout=30
            )
            response.raise_for_status()
            self.logger.debug(f"索引已刷新：{index_name}")

        except Exception as e:
            self.logger.error(f"刷新索引失敗：{str(e)}")
            raise
