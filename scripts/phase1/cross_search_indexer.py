#!/usr/bin/env python3
"""
跨表搜索索引構建器
將多個表的數據整合到統一的搜索索引中
"""

import hashlib, re
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any
from opencc import OpenCC


def _safe_str(v: Any) -> str:
    return "" if v is None else str(v)


def _render_template(tpl: str, row: Dict[str, Any]) -> str:
    """
    安全的 {{ key }} 渲染：缺欄位 → 空字串，不丟 KeyError
    """
    if not tpl:
        return ""

    def repl(m):
        key = m.group(1).strip()
        return _safe_str(row.get(key, ""))

    return re.sub(r"\{\{\s*([^}]+)\s*\}\}", repl, tpl)


def _synthesize_content(row: Dict[str, Any], prefer_fields: List[str]) -> str:
    """
    當資料沒有 content 欄位時，自動把常見可檢索欄位接成一段文字
    """
    parts: List[str] = []
    used = set()

    # 先用 tables.yaml 的 search_fields
    for f in prefer_fields or []:
        if f in row and row[f] is not None:
            s = _safe_str(row[f]).strip()
            if s and (f, s) not in used:
                parts.append(s)
                used.add((f, s))

    # 還不夠，再用兜底欄位池補
    if len(parts) < 3:
        fallback = [
            "company_name",
            "company_short",
            "company_phone",
            "company_address",
            "customer_name",
            "customer_contact",
            "invoice_address",
            "industry_type",
            "order_id",
            "order_date",
            "order_status",
            "remark",
            "processing_type",
            "classification",
            "specification",
            "material_id",
            "item_group",
        ]
        for f in fallback:
            if f in row and row[f] is not None:
                s = _safe_str(row[f]).strip()
                if s and (f, s) not in used:
                    parts.append(s)
                    used.add((f, s))
            if len(parts) >= 12:
                break

    return " ".join(parts)


class CrossSearchIndexer:
    """跨表搜索索引構建器"""

    def __init__(self, es_indexer, transformer, logger):
        """初始化"""
        self.es = es_indexer
        self.transformer = transformer
        self.logger = logger

        # 簡繁體轉換器
        self.cc_s2t = OpenCC("s2t")
        self.cc_t2s = OpenCC("t2s")

        # 文檔類型映射
        self.doc_type_mapping = {
            "customers": "customer",
            "products": "product",
            "orders": "order",
            "order_details": "order_detail",
        }

    def build_cross_doc(
        self,
        table_name: str,
        table_cfg: Dict[str, Any],
        row: Dict[str, Any],
        templates: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        產生送進 erp-cross-search 的單筆文件：
        - 不硬性要求 row['content'] 存在
        - 模板缺欄位 → 自動補空字串
        - content 不存在時自動合成
        """
        pk = table_cfg.get("primary_key") or "id"
        source_id = row.get(pk)

        # 讀 cross_search_template.yaml 的該表樣板（沒有就當空）
        tpl = (templates or {}).get(table_name, {})
        title_tpl = tpl.get("title", "")
        summary_tpl = tpl.get("summary", "")

        title = _render_template(title_tpl, row)
        summary = _render_template(summary_tpl, row)

        # content：優先用資料裡的 content；沒有就用 search_fields 自動合成
        prefer_fields = table_cfg.get("search_fields") or []
        content = _safe_str(row.get("content") or "")
        if not content:
            content = _synthesize_content(row, prefer_fields)
        if not content:
            content = " ".join([title, summary]).strip()

        # 需要繁簡轉換就開（可選）
        try:
            if hasattr(self, "cc") and isinstance(self.cc, OpenCC):
                title = self.cc.convert(title)
                summary = self.cc.convert(summary)
                content = self.cc.convert(content)
        except Exception:
            pass

        # 統一 ID（或沿用你原本 unified_id）
        unified_id = f"{table_name}:{_safe_str(source_id)}"

        doc = {
            "_id": unified_id,
            "_source": {
                "source_table": table_name,
                "source_id": _safe_str(source_id),
                "title": title,
                "summary": summary,
                "content": content,
                "last_updated": datetime.now().isoformat(),
            },
        }

        # 常見可供 filter/聚合的欄位一併帶出（若存在）
        carry = [
            "company_id",
            "company_name",
            "company_short",
            "customer_name",
            "customer_contact",
            "order_id",
            "order_date",
            "order_status",
            "created_time",
            "updated_time",
            "classification",
            "processing_type",
            "specification",
            "material_id",
            "item_group",
            "currency",
            "total",
            "tax",
            "total_amount",
        ]
        for k in carry:
            if k in row and row[k] is not None:
                doc["_source"][k] = row[k]

        return doc

    def build_cross_search_document(
        self, table_name: str, source_data: Dict[str, Any], table_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        建立跨表搜索單筆文件：
        - 不硬性要求 source_data['content']
        - 模板缺欄位→空字串
        - content 缺→自動合成
        """
        # 取主鍵
        pk = table_config.get("primary_key") or "id"
        source_id = source_data.get(pk)

        # 取 cross_search 的模板（建議在 __init__ 預先載到 self.cross_templates）
        # 例如：self.cross_templates = self.cross_tpl["cross_search"]["templates"]
        templates = getattr(self, "cross_templates", {}) or {}
        tpl = templates.get(table_name, {})
        title_tpl = tpl.get("title", "")
        summary_tpl = tpl.get("summary", "")

        # 安全模板渲染
        title = _render_template(title_tpl, source_data)
        summary = _render_template(summary_tpl, source_data)

        # content：優先用資料本身的 content，否則依 search_fields 合成
        prefer_fields = table_config.get("search_fields") or []
        content = _safe_str(source_data.get("content") or "")
        if not content:
            content = _synthesize_content(source_data, prefer_fields)
        if not content:
            content = " ".join([title, summary]).strip()

        # 如有 OpenCC（繁簡轉換）則轉一下（可選）
        try:
            if hasattr(self, "cc") and self.cc:
                title = self.cc.convert(title)
                summary = self.cc.convert(summary)
                content = self.cc.convert(content)
        except Exception:
            pass

        unified_id = f"{table_name}:{_safe_str(source_id)}"

        # 組 _source；可加常用欄位，方便 filter/聚合
        payload = {
            "unified_id": unified_id,
            "source_table": table_name,
            "source_id": _safe_str(source_id),
            "title": title,
            "summary": summary,
            "content": content,
            "last_updated": datetime.now().isoformat(),
        }
        for k in [
            "company_id",
            "company_name",
            "company_short",
            "customer_name",
            "customer_contact",
            "order_id",
            "order_date",
            "order_status",
            "created_time",
            "updated_time",
            "classification",
            "processing_type",
            "specification",
            "material_id",
            "item_group",
            "currency",
            "total",
            "tax",
            "total_amount",
        ]:
            if k in source_data and source_data[k] is not None:
                payload[k] = source_data[k]

        return payload

    def sync_to_cross_search(
        self, table_name: str, documents: List[Dict], table_config: Dict
    ) -> int:
        """同步數據到跨表搜索索引"""

        cross_search_docs = []

        for doc in documents:
            try:
                cross_doc = self.build_cross_search_document(
                    table_name, doc.get("_source", {}), table_config
                )
                cross_search_docs.append(
                    {"_id": cross_doc["unified_id"], "_source": cross_doc}
                )
            except Exception as e:
                pk = table_config.get("primary_key") or "id"
                src = doc.get("_source", {})
                self.logger.error(
                    "構建跨表搜索文檔失敗（表=%s, 主鍵欄=%s, 主鍵值=%s, keys=%s）：%s",
                    table_name, pk, src.get(pk), list(src.keys())[:20], str(e)
                )
                continue

        if cross_search_docs:
            success_count = self.es.bulk_index("erp-cross-search", cross_search_docs)
            self.logger.info(f"同步 {success_count} 筆數據到跨表搜索索引")
            return success_count

        return 0
