#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
資料庫同步腳本 - 多表同步版
同步 PDF 相關表到 Elasticsearch
"""

import os, sys, time, json, pymysql, requests, signal
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, List, Any, Optional
from pymysql.cursors import DictCursor
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# ========== 環境變數配置 ==========
ES_URL = os.environ.get('ES_URL', 'http://localhost:9200')
ES_USER = os.environ.get('ES_USER', 'elastic')
ES_PASS = os.environ.get('ES_PASS', 'admin@12345')

MYSQL_HOST = os.environ.get('MYSQL_HOST', 'mysql')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', '3306'))
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASS = os.environ.get('MYSQL_PASS', 'root')
MYSQL_DB = os.environ.get('MYSQL_DB', 'fuhsin_erp_demo')

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

should_stop = False

def to_bool(v):
    if v is None: return None
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return int(v) != 0
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ('1','true','yes','y','on'): return True
        if s in ('0','false','no','n','off',''): return False
    return None

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
                "properties": {
                    "doc_id": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "last_modified": {"type": "date"}
                }
            }
        }
        
        # 根據類型添加特定欄位
        if doc_type == 'ecn_notice':
            base_mapping["mappings"]["properties"].update({
                "notice_number": {"type": "keyword"},
                "application_number": {"type": "keyword"},
                "product_code": {"type": "keyword"},
                "product_name": {
                    "type": "text",
                    "analyzer": "chinese_analyzer",
                    "fields": {"keyword": {"type": "keyword"}}
                },
                "change_description": {"type": "text", "analyzer": "chinese_analyzer"},
                "before_change": {"type": "text", "analyzer": "chinese_analyzer"},
                "after_change": {"type": "text", "analyzer": "chinese_analyzer"},
                "inventory_handling": {"type": "text", "analyzer": "chinese_analyzer"},
                "applicant": {"type": "keyword"},
                "ecn_date": {"type": "date"}
            })
        
        elif doc_type == 'ecn_application':
            base_mapping["mappings"]["properties"].update({
                "application_number": {"type": "keyword"},
                "product_code": {"type": "keyword"},
                "product_name": {
                    "type": "text",
                    "analyzer": "chinese_analyzer",
                    "fields": {"keyword": {"type": "keyword"}}
                },
                "reason": {"type": "text", "analyzer": "chinese_analyzer"},
                "change_items": {"type": "text", "analyzer": "chinese_analyzer"},
                "change_before": {"type": "text", "analyzer": "chinese_analyzer"},
                "change_after": {"type": "text", "analyzer": "chinese_analyzer"},
                "meeting_suggestions": {"type": "text", "analyzer": "chinese_analyzer"},
                "review_notes": {"type": "text", "analyzer": "chinese_analyzer"},
                "ecn_date": {"type": "date"}
            })
        
        elif doc_type == 'complaint':
            base_mapping["mappings"]["properties"].update({
                "complaint_number": {"type": "keyword"},
                "complaint_type": {"type": "keyword"},
                "customer_code": {"type": "keyword"},
                "customer_name": {
                    "type": "text",
                    "analyzer": "chinese_analyzer",
                    "fields": {"keyword": {"type": "keyword"}}
                },
                "product_code": {"type": "keyword"},
                "product_name": {
                    "type": "text",
                    "analyzer": "chinese_analyzer",
                    "fields": {"keyword": {"type": "keyword"}}
                },
                "complaint_description": {"type": "text", "analyzer": "chinese_analyzer"},
                "complaint_analysis": {"type": "text", "analyzer": "chinese_analyzer"},
                "responsible_sales": {"type": "keyword"}
            })
        
        elif doc_type == 'fmea':
            base_mapping["mappings"]["properties"].update({
                "case_number": {"type": "keyword"},
                "case_name": {
                    "type": "text",
                    "analyzer": "chinese_analyzer",
                    "fields": {"keyword": {"type": "keyword"}}
                },
                "analysis_type": {"type": "keyword"},
                "product_type": {"type": "keyword"},
                "responsible_person": {"type": "keyword"},
                "analyst": {"type": "text", "analyzer": "chinese_analyzer"},
                "analysis_item": {"type": "text", "analyzer": "chinese_analyzer"},
                "failure_mode": {"type": "text", "analyzer": "chinese_analyzer"},
                "failure_effect": {"type": "text", "analyzer": "chinese_analyzer"},
                "failure_cause": {"type": "text", "analyzer": "chinese_analyzer"},
                "severity_s": {"type": "integer"},
                "occurrence_o": {"type": "integer"},
                "detection_d": {"type": "integer"},
                "rpn": {"type": "integer"},
                "current_control": {"type": "text", "analyzer": "chinese_analyzer"},
                "corrective_action": {"type": "text", "analyzer": "chinese_analyzer"},
                "improvement_result": {"type": "text", "analyzer": "chinese_analyzer"},
                "is_customer_complaint": {"type": "boolean"},
                "department_head": {"type": "keyword"},
                "section_head": {"type": "keyword"},
                "form_date": {"type": "date"},
                "revision_date": {"type": "date"}
            })
        
        elif doc_type == 'document':
            base_mapping["mappings"]["properties"].update({
                "original_doc_id": {"type": "keyword"},
                "doc_type": {"type": "keyword"},
                "doc_number": {"type": "keyword"},
                "doc_date": {"type": "date"},
                "file_name": {"type": "keyword"},
                "file_url": {"type": "keyword"},
                "product_codes": {"type": "keyword"},
                "product_names": {
                    "type": "text",
                    "analyzer": "chinese_analyzer",
                    "fields": {"keyword": {"type": "keyword"}}
                },
                "applicant": {"type": "keyword"},
                "department": {"type": "keyword"},
                "summary": {"type": "text", "analyzer": "chinese_analyzer"},
                "keywords": {"type": "keyword"},
                "status": {"type": "keyword"},
                "priority": {"type": "keyword"}
            })
        
        return base_mapping
    
    def bulk_index(self, index_name: str, documents: List[Dict]) -> int:
        """ 批次索引文檔 """
        if not documents:
            return 0
        
        try:
            # 建立 bulk 請求
            lines = []
            for doc in documents:
                doc_id = doc.get('id') or doc.get('doc_id')
                # 索引命令
                lines.append(json.dumps({"index": {"_index": index_name, "_id": doc_id}}))
                # 文檔內容
                lines.append(json.dumps(doc, ensure_ascii=False, default=str))
            
            bulk_data = '\n'.join(lines) + '\n'
            
            # 發送 bulk 請求
            response = self.session.post(
                f"{ES_URL}/_bulk",
                data=bulk_data,
                headers={'Content-Type': 'application/x-ndjson'}
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('errors'):
                    # ⭐ 修改這裡：輸出詳細錯誤
                    error_items = [item for item in result['items'] if 'error' in item.get('index', {})]
                    for item in error_items[:5]:  # 只顯示前3個錯誤
                        error_detail = item.get('index', {}).get('error', {})
                        logger.error(f"索引錯誤詳情: {json.dumps(error_detail, ensure_ascii=False, indent=2)}")
                    
                    error_count = len(error_items)
                    logger.warning(f"批次索引部分失敗: {error_count}/{len(documents)} 錯誤")
                    return len(documents) - error_count
                return len(documents)
            else:
                logger.error(f"批次索引失敗: {response.status_code} - {response.text[:500]}")
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
                    
                    # 處理 JSON 欄位 (structured_documents)
                    if table_name == 'structured_documents':
                        json_fields = ['product_codes', 'product_names', 'related_doc_numbers', 
                                        'responsible_units', 'keywords']
                        for field in json_fields:
                            if field in row and row[field]:
                                try:
                                    if isinstance(row[field], str):
                                        row[field] = json.loads(row[field])
                                except Exception:
                                    row[field] = []
                    
                    if table_name == 'fmea_records':
                        if 'is_customer_complaint' in row:
                            row['is_customer_complaint'] = to_bool(row['is_customer_complaint'])

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
        # 方案A：每種表單同步到不同索引
        tables = [
            # PDF 文件相關表
            ('ecn_notices', 'erp-ecn-notices', 'ecn_notice'),
            ('ecn_applications', 'erp-ecn-applications', 'ecn_application'),
            ('complaint_records', 'erp-complaint-records', 'complaint'),
            ('fmea_records', 'erp-fmea', 'fmea'),
            ('structured_documents', 'erp-structure', 'document'),
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
    logger.info("  - ecn_notices → erp-ecn-notices")
    logger.info("  - ecn_applications → erp-ecn-applications")
    logger.info("  - complaint_records → erp-complaint-records")
    logger.info("  - fmea_records → erp-fmea")
    logger.info("  - structured_documents → erp-documents")
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
