#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
優化版資料庫同步腳本
專注於 MySQL 到 Elasticsearch 的資料同步
同步資料表：
- product_master_a: 產品主檔
- product_warehouse_b: 倉儲資料
- customer_complaint_c: 客訴記錄
- structured_documents: 結構化文件（技術文件、品質報告等）

向量生成由獨立的 vector_service.py 處理
"""

import os, sys, time, json, pymysql, requests
import signal, hashlib, threading, logging, math
from uuid import UUID
from pathlib import Path
from enum import Enum
from decimal import Decimal
from datetime import datetime, date
from typing import Dict, List, Any, Optional
from pymysql.cursors import DictCursor
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== 環境變數配置 ==========
# Elasticsearch 配置
ES_URL = os.environ.get('ES_URL', 'http://localhost:9200')
ES_USER = os.environ.get('ES_USER', 'elastic')
ES_PASS = os.environ.get('ES_PASS', 'admin@12345')

# MySQL 配置
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'mysql')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', '3306'))
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASS = os.environ.get('MYSQL_PASS', 'root')
MYSQL_DB = os.environ.get('MYSQL_DB', 'fuhsin_erp_demo')

# 同步配置
BATCH_SIZE = int(os.environ.get('DB_BATCH_SIZE', '1000'))
PAGE_SIZE = int(os.environ.get('DB_PAGE_SIZE', '5000'))
PARALLEL_THREADS = int(os.environ.get('PARALLEL_THREADS', '4'))
SYNC_INTERVAL = int(os.environ.get('DB_SYNC_INTERVAL', '60'))

# ========== 日誌配置 ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 全域控制變數
should_stop = False

# ========== Elasticsearch 客戶端 ==========
class ElasticsearchClient:
    def __init__(self):
        self.session = requests.Session()
        if ES_USER and ES_PASS:
            self.session.auth = HTTPBasicAuth(ES_USER, ES_PASS)
        self.session.headers.update({'Content-Type': 'application/json'})
        
    def check_connection(self):
        """檢查 Elasticsearch 連接"""
        try:
            response = self.session.get(f"{ES_URL}/_cluster/health")
            if response.status_code == 200:
                health = response.json()
                logger.info(f"✅ Elasticsearch 連接成功，狀態: {health['status']}")
                return True
            else:
                logger.error(f"❌ Elasticsearch 連接失敗: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"❌ 無法連接到 Elasticsearch: {e}")
            return False
    
    def create_index(self, index_name: str, doc_type: str = 'general'):
        """建立索引並設定 mapping"""
        try:
            # 檢查索引是否存在
            response = self.session.head(f"{ES_URL}/{index_name}")
            if response.status_code == 200:
                logger.debug(f"索引 {index_name} 已存在")
                return True
            
            # 建立新索引
            mapping = self._get_mapping_for_type(doc_type)
            response = self.session.put(
                f"{ES_URL}/{index_name}",
                json=mapping
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"✅ 成功建立索引: {index_name}")
                return True
            else:
                logger.error(f"❌ 建立索引失敗: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 建立索引時發生錯誤: {e}")
            return False
    
    def _get_mapping_for_type(self, doc_type: str) -> dict:
        """根據文檔類型獲取對應的 mapping"""
        base_mapping = {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 1,
                "refresh_interval": "30s",
                "analysis": {
                    "analyzer": {
                        "chinese_analyzer": {
                            "type": "standard",
                            "stopwords": "_chinese_"
                        }
                    }
                }
            },
            "mappings": {
                "properties": {}
            }
        }
        
        # 根據類型添加特定欄位
        if doc_type == 'product':
            base_mapping["mappings"]["properties"].update({
                "product_id": {"type": "keyword"},
                "product_name": {
                    "type": "text",
                    "analyzer": "chinese_analyzer",
                    "fields": {"keyword": {"type": "keyword"}}
                },
                "specification": {"type": "text", "analyzer": "chinese_analyzer"},
                "category": {"type": "keyword"},
                "brand": {"type": "keyword"},
                "unit": {"type": "keyword"},
                "price": {"type": "float"},
                "stock_quantity": {"type": "integer"},
                "warehouse_location": {"type": "keyword"},
                "description": {"type": "text", "analyzer": "chinese_analyzer"},
                "status": {"type": "keyword"},
                "created_date": {"type": "date"},
                "last_modified": {"type": "date"}
            })
        elif doc_type == 'complaint':
            base_mapping["mappings"]["properties"].update({
                "complaint_id": {"type": "keyword"},
                "customer_name": {
                    "type": "text",
                    "analyzer": "chinese_analyzer",
                    "fields": {"keyword": {"type": "keyword"}}
                },
                "customer_contact": {"type": "keyword"},
                "product_name": {"type": "text", "analyzer": "chinese_analyzer"},
                "issue_description": {"type": "text", "analyzer": "chinese_analyzer"},
                "issue_date": {"type": "date"},
                "handler": {"type": "keyword"},
                "status": {"type": "keyword"},
                "solution": {"type": "text", "analyzer": "chinese_analyzer"},
                "resolved_date": {"type": "date"},
                "last_modified": {"type": "date"}
            })
        elif doc_type == 'document':
            base_mapping["mappings"]["properties"].update({
                "original_doc_id": {"type": "keyword"},
                "doc_type": {"type": "keyword"},
                "doc_number": {"type": "keyword"},
                "doc_date": {"type": "date"},
                "file_name": {
                    "type": "text",
                    "analyzer": "chinese_analyzer",
                    "fields": {"keyword": {"type": "keyword"}}
                },
                "file_url": {"type": "keyword"},
                "file_path": {"type": "keyword"},
                "file_size": {"type": "long"},
                "file_hash": {"type": "keyword"},
                "product_category": {"type": "keyword"},
                "product_codes": {"type": "keyword"},
                "product_names": {
                    "type": "text",
                    "analyzer": "chinese_analyzer"
                },
                "applicant": {"type": "keyword"},
                "department": {"type": "keyword"},
                "responsible_units": {"type": "keyword"},
                "summary": {"type": "text", "analyzer": "chinese_analyzer"},
                "keywords": {"type": "keyword"},
                "status": {"type": "keyword"},
                "priority": {"type": "keyword"},
                "parsed_at": {"type": "date"},
                "last_modified": {"type": "date"}
            })
        
        return base_mapping
    
    def bulk_index(self, index_name: str, documents: List[Dict]) -> int:
        """批次索引文件"""
        if not documents:
            return 0
        
        EXCLUDE_FIELDS = {'original_extracted_content'}

        # 準備批次操作
        def sanitize_doc(doc):
            def _norm(v):
                # 基本可序列化型別（保持原樣）
                if v is None or isinstance(v, (str, int, float, bool)):
                    # 可選：避免 NaN/Infinity 進 ES
                    if isinstance(v, float) and not math.isfinite(v):
                        return None
                    return v

                # bytes 類：盡量用 utf-8，失敗則轉 hex
                if isinstance(v, (bytes, bytearray, memoryview)):
                    try:
                        return bytes(v).decode("utf-8")
                    except Exception:
                        return bytes(v).hex()

                # Decimal → float（確保 ES 走 numeric 映射）
                if isinstance(v, Decimal):
                    f = float(v)
                    return f if math.isfinite(f) else None

                # 日期時間 → ISO8601
                if isinstance(v, (datetime, date)):
                    return v.isoformat()

                # 其他常見可轉字串型別
                if isinstance(v, UUID):
                    return str(v)
                if isinstance(v, Path):
                    return str(v)
                if isinstance(v, Enum):
                    return v.value

                # 容器：遞迴處理
                if isinstance(v, dict):
                    # key 也做一次正規化並確保是字串
                    return {str(_norm(k)): _norm(val) for k, val in v.items() if k not in EXCLUDE_FIELDS}
                if isinstance(v, (list, tuple, set)):
                    return [_norm(x) for x in v]

                # 自訂物件：嘗試吃 __dict__，最後退回 str
                if hasattr(v, "__dict__"):
                    return _norm(vars(v))

                return str(v)  # 最後的保險
            # 入口通常是 dict，但讓它能接任何型別
            return _norm(doc)
        
        def ensure_doc_id(doc):
            # 按優先順序檢查可用的 ID 欄位
            for key in ("id", "product_id", "complaint_id", "original_doc_id", "doc_number"):
                if key in doc and doc[key] not in (None, ""):
                    return str(doc[key])
            # fallback：使用文檔內容的 hash
            blob = json.dumps(doc, sort_keys=True, default=str).encode("utf-8")
            return hashlib.md5(blob).hexdigest()
        
        actions = []
        for doc in documents:
            doc = sanitize_doc(doc)
            doc_id = ensure_doc_id(doc)
            actions.append({
                "_index": index_name,
                "_id": doc_id,
                "_source": doc
            })
        
        # 執行批次索引
        bulk_body = []
        for action in actions:
            bulk_body.append(json.dumps({"index": {
                "_index": action["_index"],
                "_id": action["_id"]
            }}))
            bulk_body.append(json.dumps(action["_source"], ensure_ascii=False))
        
        bulk_data = '\n'.join(bulk_body) + '\n'
        
        try:
            response = self.session.post(
                f"{ES_URL}/_bulk",
                data=bulk_data,
                headers={'Content-Type': 'application/x-ndjson'}
            )
            
            if response.status_code in [200, 201]:
                result = response.json()
                if not result.get('errors'):
                    indexed = len([item for item in result.get('items', []) 
                                 if item.get('index', {}).get('status') in [200, 201]])
                    return indexed
                else:
                    # 計算成功的數量
                    indexed = len([item for item in result.get('items', []) 
                                if item.get('index', {}).get('status') in [200, 201]])
                    failed = len(result.get('items', [])) - indexed
                    if failed > 0:
                        logger.warning(f"⚠️ 部分文檔索引失敗: {failed} 個")
                    return indexed
            else:
                logger.error(f"❌ 批次索引失敗: {response.text[:200]}")
                return 0
                
        except Exception as e:
            logger.error(f"❌ 批次索引時發生錯誤: {e}")
            return 0
    
    def get_doc_count(self, index_name: str) -> int:
        """獲取索引中的文檔數量"""
        try:
            response = self.session.get(f"{ES_URL}/{index_name}/_count")
            if response.status_code == 200:
                return response.json().get('count', 0)
            return 0
        except Exception:
            return 0
    
    def delete_index(self, index_name: str):
        """刪除索引"""
        try:
            response = self.session.delete(f"{ES_URL}/{index_name}")
            if response.status_code == 200:
                logger.info(f"✅ 成功刪除索引: {index_name}")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ 刪除索引時發生錯誤: {e}")
            return False

# ========== MySQL 同步器 ==========
class MySQLSyncer:
    def __init__(self, es_client: ElasticsearchClient):
        self.es_client = es_client
        self.connection = None
        self.last_sync_times = {}
        
    def connect(self):
        """連接到 MySQL"""
        try:
            self.connection = pymysql.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASS,
                database=MYSQL_DB,
                cursorclass=DictCursor,
                charset='utf8mb4'
            )
            logger.info("✅ MySQL 連接成功")
            return True
        except Exception as e:
            logger.error(f"❌ MySQL 連接失敗: {e}")
            return False
    
    def sync_table(self, table_name: str, index_name: str, doc_type: str = 'general'):
        """同步單個資料表"""
        if not self.connection or not self.connection.open:
            if not self.connect():
                return
        
        try:
            # 建立或更新索引
            self.es_client.create_index(index_name, doc_type)
            
            # 獲取總筆數
            with self.connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) as total FROM {table_name}")
                total = cursor.fetchone()['total']
                
            if total == 0:
                logger.info(f"資料表 {table_name} 沒有資料")
                return
            
            logger.info(f"📊 開始同步 {table_name}: 共 {total} 筆資料")
            
            # 使用多執行緒處理
            with ThreadPoolExecutor(max_workers=PARALLEL_THREADS) as executor:
                futures = []
                
                for offset in range(0, total, PAGE_SIZE):
                    future = executor.submit(
                        self._sync_batch, 
                        table_name, 
                        index_name, 
                        offset, 
                        min(PAGE_SIZE, total - offset)
                    )
                    futures.append(future)
                
                # 等待所有任務完成
                indexed_total = 0
                for future in as_completed(futures):
                    try:
                        indexed = future.result()
                        indexed_total += indexed
                    except Exception as e:
                        logger.error(f"批次處理失敗: {e}")
            
            # 記錄同步時間
            self.last_sync_times[table_name] = datetime.now()
            
            # 獲取最終文檔數
            final_count = self.es_client.get_doc_count(index_name)
            logger.info(f"✅ {table_name} 同步完成: 索引 {indexed_total} 筆，總計 {final_count} 筆文檔")
            
        except Exception as e:
            logger.error(f"❌ 同步 {table_name} 時發生錯誤: {e}")
    
    def _sync_batch(self, table_name: str, index_name: str, offset: int, limit: int) -> int:
        """同步一批資料"""
        conn = None
        try:
            # 為每個執行緒建立獨立連接
            conn = pymysql.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASS,
                database=MYSQL_DB,
                cursorclass=DictCursor,
                charset='utf8mb4'
            )
            
            indexed = 0
            with conn.cursor() as cursor:
                # 查詢資料
                query = f"SELECT * FROM {table_name} LIMIT %s OFFSET %s"
                cursor.execute(query, (limit, offset))
                
                # 批次處理
                batch = []
                for row in cursor:
                    # 處理日期時間欄位
                    for key, value in row.items():
                        if isinstance(value, (datetime, date)):
                            row[key] = value.isoformat()
                        elif isinstance(value, Decimal):
                            row[key] = float(value)
                        elif isinstance(value, (bytes, bytearray, memoryview)):
                            row[key] = bytes(value).decode("utf-8", errors="ignore")

                    
                    # 處理 structured_documents 的 JSON 欄位
                    if table_name == 'structured_documents':
                        json_fields = ['product_codes', 'product_names', 'responsible_units', 'keywords']
                        for field in json_fields:
                            if field in row and row[field]:
                                try:
                                    if isinstance(row[field], str):
                                        row[field] = json.loads(row[field])
                                except Exception as e:
                                    logger.warning(f"解析 JSON 欄位 {field} 失敗: {e}")
                                    row[field] = []
                        row.pop("original_extracted_content", None)
                    
                    batch.append(row)
                    
                    if len(batch) >= BATCH_SIZE:
                        indexed += self.es_client.bulk_index(index_name, batch)
                        batch = []
                
                # 處理剩餘的資料
                if batch:
                    indexed += self.es_client.bulk_index(index_name, batch)
            
            return indexed
            
        except Exception as e:
            logger.error(f"批次同步失敗 (offset={offset}): {e}")
            return 0
        finally:
            if conn:
                conn.close()
    
    def sync_all(self):
        """同步所有配置的資料表"""
        tables = [
            ('product_master_a', 'erp-products', 'product'),
            ('product_warehouse_b', 'erp-warehouse', 'product'),
            ('customer_complaint_c', 'erp-complaints', 'complaint'),
            ('structured_documents', 'erp-documents', 'document')
        ]
        
        for table_name, index_name, doc_type in tables:
            if should_stop:
                break
            self.sync_table(table_name, index_name, doc_type)
    
    def close(self):
        """關閉連接"""
        if self.connection:
            self.connection.close()
            logger.info("MySQL 連接已關閉")

# ========== 信號處理 ==========
def signal_handler(signum, frame):
    global should_stop
    logger.info("\n⚠️ 收到停止信號，正在優雅關閉...")
    should_stop = True

# ========== 主程式 ==========
def main():
    global should_stop
    
    # 註冊信號處理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 顯示配置資訊
    logger.info("=" * 60)
    logger.info("📋 資料庫同步服務啟動")
    logger.info(f"ES URL: {ES_URL}")
    logger.info(f"MySQL: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")
    logger.info(f"批次大小: {BATCH_SIZE}")
    logger.info(f"頁面大小: {PAGE_SIZE}")
    logger.info(f"並行執行緒: {PARALLEL_THREADS}")
    logger.info("同步資料表:")
    logger.info("  - product_master_a → erp-products")
    logger.info("  - product_warehouse_b → erp-warehouse")
    logger.info("  - customer_complaint_c → erp-complaints")
    logger.info("  - structured_documents → erp-documents")
    logger.info("向量生成由 vector_service.py 獨立處理")
    logger.info("=" * 60)
    
    # 建立客戶端
    es_client = ElasticsearchClient()
    
    # 檢查 Elasticsearch 連接
    while not should_stop:
        if es_client.check_connection():
            break
        logger.info("等待 Elasticsearch 啟動...")
        time.sleep(5)
    
    if should_stop:
        return
    
    # 建立同步器
    syncer = MySQLSyncer(es_client)
    
    try:
        # 首次全量同步
        logger.info("🚀 開始首次全量同步...")
        syncer.sync_all()
        
        # 定期增量同步
        while not should_stop:
            logger.info(f"⏰ 等待 {SYNC_INTERVAL} 秒後進行下次同步...")
            
            # 可中斷的等待
            for _ in range(SYNC_INTERVAL):
                if should_stop:
                    break
                time.sleep(1)
            
            if not should_stop:
                logger.info("🔄 開始增量同步...")
                syncer.sync_all()
                
    except Exception as e:
        logger.error(f"❌ 主程式發生錯誤: {e}")
    finally:
        syncer.close()
        logger.info("👋 資料庫同步服務已停止")

if __name__ == '__main__':
    main()
