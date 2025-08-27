#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
資料轉換器核心架構
統一處理多種來源的資料轉換和索引
"""
import os
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Protocol
from abc import ABC, abstractmethod
import requests
from opencc import OpenCC

# 設定日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataAdapter(Protocol):
    """資料適配器介面"""
    
    @abstractmethod
    def fetch_data(self) -> List[Dict[str, Any]]:
        """從來源取得資料"""
        pass
    
    @abstractmethod
    def get_metadata(self) -> Dict[str, str]:
        """取得來源元資料"""
        pass

class StandardDocument:
    """標準文檔格式定義"""
    
    REQUIRED_FIELDS = ['table_name', 'pk', 'updated_at']
    
    TEXT_FIELDS = [
        'supplier_name', 'customer_name', 'supplier_short', 'customer_short',
        'supplier_contact', 'customer_contact', 'supplier_address', 'customer_address',
        'employee_name', 'department_name', 'position', 'remark'
    ]
    
    NUMERIC_FIELDS = [
        'total_amount', 'discount', 'tax', 'quantity'
    ]
    
    DATE_FIELDS = [
        'planned_start_date', 'delivery_date', 'created_time', 'updated_time'
    ]
    
    KEYWORD_FIELDS = [
        'work_order_id', 'order_id', 'supplier_id', 'customer_id', 
        'employee_id', 'department_id', 'status', 'currency'
    ]

class DataTransformer:
    """資料轉換器"""
    
    def __init__(self):
        self.cc_s2t = OpenCC("s2t")  # 簡轉繁
        self.cc_t2s = OpenCC("t2s")  # 繁轉簡
    
    def transform(self, raw_data: Dict[str, Any], source_metadata: Dict[str, str]) -> Dict[str, Any]:
        """將原始資料轉換為標準格式"""
        
        # 基礎文檔結構
        doc = {
            '@timestamp': datetime.now(timezone.utc).isoformat(),
            'metadata': {
                'source_type': source_metadata.get('type', 'unknown'),
                'source_file': source_metadata.get('file', ''),
                'table_name': raw_data.get('table_name', ''),
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
        }
        
        # 處理必要欄位
        for field in StandardDocument.REQUIRED_FIELDS:
            if field in raw_data:
                doc[field] = raw_data[field]
        
        # 處理文字欄位（繁體為主 + IK 分詞）
        searchable_content = []
        for field in StandardDocument.TEXT_FIELDS:
            if field in raw_data and raw_data[field]:
                original = str(raw_data[field])
                # 轉換為繁體
                traditional = self.cc_s2t.convert(original)
                doc[field] = {
                    'raw': traditional,  # keyword 搜尋
                    'text': traditional  # 分詞搜尋
                }
                searchable_content.extend([original, traditional])
        
        # 處理數值欄位
        for field in StandardDocument.NUMERIC_FIELDS:
            if field in raw_data and raw_data[field] is not None:
                try:
                    doc[field] = float(raw_data[field])
                except (ValueError, TypeError):
                    logger.warning(f"無法轉換數值欄位 {field}: {raw_data[field]}")
        
        # 處理日期欄位
        for field in StandardDocument.DATE_FIELDS:
            if field in raw_data and raw_data[field]:
                doc[field] = self.normalize_date(raw_data[field])
        
        # 處理關鍵字欄位
        for field in StandardDocument.KEYWORD_FIELDS:
            if field in raw_data and raw_data[field]:
                doc[field] = str(raw_data[field])
        
        # 綜合搜尋內容
        doc['searchable_content'] = ' '.join(set(searchable_content))
        
        return doc
    
    def normalize_date(self, date_value: Any) -> Optional[str]:
        """標準化日期格式"""
        if not date_value:
            return None
        
        date_str = str(date_value)
        # 移除可能的時間戳格式
        if 'T' in date_str:
            return date_str
        
        # 處理常見日期格式
        try:
            if len(date_str) == 10 and '-' in date_str:  # YYYY-MM-DD
                return f"{date_str}T00:00:00Z"
            elif len(date_str) == 19:  # YYYY-MM-DD HH:MM:SS
                return f"{date_str}.000Z"
        except:
            pass
        
        return date_str

class ElasticsearchWriter:
    """Elasticsearch 寫入器"""
    
    def __init__(self, es_url: str, username: str, password: str):
        self.es_url = es_url
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update({"Content-Type": "application/json"})
    
    def ensure_index_template(self, template_name: str = "erp-template"):
        """確保索引模板存在"""
        template = {
            "index_patterns": ["erp_*_v*"],
            "template": {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "analysis": {
                        "analyzer": {
                            "ik_analyzer": {"type": "ik_max_word"},
                            "ik_search": {"type": "ik_smart"}
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date"},
                        "metadata": {
                            "properties": {
                                "source_type": {"type": "keyword"},
                                "source_file": {"type": "keyword"},
                                "table_name": {"type": "keyword"},
                                "last_updated": {"type": "date"}
                            }
                        },
                        "table_name": {"type": "keyword"},
                        "pk": {"type": "keyword"},
                        "updated_at": {"type": "date"},
                        "searchable_content": {
                            "type": "text",
                            "analyzer": "ik_analyzer",
                            "search_analyzer": "ik_search"
                        }
                    },
                    "dynamic_templates": [
                        {
                            "text_fields": {
                                "match": "*_name",
                                "mapping": {
                                    "type": "object",
                                    "properties": {
                                        "raw": {"type": "keyword"},
                                        "text": {
                                            "type": "text",
                                            "analyzer": "ik_analyzer",
                                            "search_analyzer": "ik_search"
                                        }
                                    }
                                }
                            }
                        },
                        {
                            "keyword_fields": {
                                "match": "*_id",
                                "mapping": {"type": "keyword"}
                            }
                        },
                        {
                            "date_fields": {
                                "match": "*_date",
                                "mapping": {"type": "date"}
                            }
                        },
                        {
                            "amount_fields": {
                                "match": "*_amount",
                                "mapping": {"type": "double"}
                            }
                        }
                    ]
                }
            }
        }
        
        try:
            response = self.session.put(
                f"{self.es_url}/_index_template/{template_name}",
                data=json.dumps(template)
            )
            response.raise_for_status()
            logger.info(f"索引模板 {template_name} 已建立")
        except Exception as e:
            logger.error(f"建立索引模板失敗: {e}")
            raise
    
    def create_index_with_alias(self, base_name: str, version: int = 1):
        """建立帶版本的索引和別名"""
        index_name = f"{base_name}_v{version}"
        alias_name = base_name
        
        try:
            # 建立索引
            response = self.session.put(f"{self.es_url}/{index_name}")
            response.raise_for_status()
            
            # 建立別名
            alias_actions = {
                "actions": [
                    {"add": {"index": index_name, "alias": alias_name}}
                ]
            }
            response = self.session.post(
                f"{self.es_url}/_aliases",
                data=json.dumps(alias_actions)
            )
            response.raise_for_status()
            
            logger.info(f"索引 {index_name} 已建立，別名 {alias_name} 已設定")
            return index_name
            
        except Exception as e:
            logger.error(f"建立索引失敗: {e}")
            raise
    
    def bulk_upsert(self, index_name: str, documents: List[Dict[str, Any]], batch_size: int = 500):
        """批量 upsert 文檔"""
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            bulk_data = []
            
            for doc in batch:
                # 使用 pk 作為 _id
                doc_id = doc.get('pk', hashlib.sha256(str(doc).encode()).hexdigest())
                
                bulk_data.append(json.dumps({
                    "index": {
                        "_index": index_name,
                        "_id": doc_id
                    }
                }, ensure_ascii=False))
                bulk_data.append(json.dumps(doc, ensure_ascii=False))
            
            if bulk_data:
                payload = '\n'.join(bulk_data) + '\n'
                response = self.session.post(
                    f"{self.es_url}/_bulk",
                    data=payload.encode('utf-8'),
                    headers={"Content-Type": "application/x-ndjson"}
                )
                
                result = response.json()
                if result.get("errors"):
                    errors = [item for item in result.get("items", []) 
                             if item.get("index", {}).get("error")]
                    logger.warning(f"批量寫入有錯誤: {len(errors)} 筆")
                    for error in errors[:3]:
                        logger.warning(f"錯誤範例: {error}")
                
                logger.info(f"批量寫入完成: {len(batch)} 筆文檔")

    def switch_alias(self, old_index: str, new_index: str, alias: str):
        """切換別名到新索引"""
        alias_actions = {
            "actions": [
                {"remove": {"index": old_index, "alias": alias}},
                {"add": {"index": new_index, "alias": alias}}
            ]
        }
        
        try:
            response = self.session.post(
                f"{self.es_url}/_aliases",
                data=json.dumps(alias_actions)
            )
            response.raise_for_status()
            logger.info(f"別名 {alias} 已從 {old_index} 切換到 {new_index}")
        except Exception as e:
            logger.error(f"切換別名失敗: {e}")
            raise

class DataPipeline:
    """資料管道主控制器"""
    
    def __init__(self, es_url: str, es_user: str, es_pass: str):
        self.transformer = DataTransformer()
        self.es_writer = ElasticsearchWriter(es_url, es_user, es_pass)
        
    def run(self, adapter: DataAdapter, target_index: str):
        """執行完整的資料轉換和索引流程"""
        logger.info(f"開始處理資料來源: {adapter.__class__.__name__}")
        
        # 確保索引模板存在
        self.es_writer.ensure_index_template()
        
        # 取得資料
        raw_data_list = adapter.fetch_data()
        metadata = adapter.get_metadata()
        
        logger.info(f"取得 {len(raw_data_list)} 筆原始資料")
        
        # 轉換資料
        transformed_docs = []
        for raw_data in raw_data_list:
            try:
                doc = self.transformer.transform(raw_data, metadata)
                transformed_docs.append(doc)
            except Exception as e:
                logger.error(f"轉換資料失敗: {e}, 資料: {raw_data}")
        
        logger.info(f"成功轉換 {len(transformed_docs)} 筆文檔")
        
        # 寫入 Elasticsearch
        if transformed_docs:
            self.es_writer.bulk_upsert(target_index, transformed_docs)
        
        logger.info("資料處理完成")
        
        return len(transformed_docs)
