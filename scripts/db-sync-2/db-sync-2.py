#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL to Elasticsearch 同步服務 - 修正版
修正 id 欄位問題、索引名稱、日期處理
"""

import os, sys, json, signal
import time, logging, pymysql, hashlib
from datetime import datetime, date, timezone
from typing import Dict, List, Optional, Any
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from elasticsearch import Elasticsearch, helpers

# 環境變數配置
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "fuhsin_erp_demo")

ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_PORT = int(os.getenv("ES_PORT", "9200"))
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASSWORD = os.getenv("ES_PASS", "admin@12345")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "5000"))
PARALLEL_THREADS = int(os.getenv("PARALLEL_THREADS", "4"))
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "30"))

# 狀態檔案路徑
STATE_FILE = Path("/state/sync_state.json")
STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ElasticsearchManager:
    """Elasticsearch 連線管理器"""
    
    def __init__(self):
        self.es = Elasticsearch(
            [f"http://{ES_HOST}:{ES_PORT}"],
            http_auth=(ES_USER, ES_PASSWORD),
            verify_certs=False,
            timeout=30,
            max_retries=3,
            retry_on_timeout=True
        )
        self.verify_connection()
        self.init_indices()
    
    def verify_connection(self):
        """驗證連線"""
        try:
            info = self.es.info()
            logger.info(f"連線到 Elasticsearch {info['version']['number']}")
        except Exception as e:
            logger.error(f"無法連線到 Elasticsearch: {e}")
            raise
    
    def init_indices(self):
        """初始化索引（加上 erp_ 前綴）"""
        indices = {
            'erp_product_master': self._get_product_mapping(),
            'erp_product_warehouse': self._get_warehouse_mapping(),
            'erp_customer_complaint': self._get_complaint_mapping(),
            'erp_structured_documents': self._get_documents_mapping()
        }
        
        for index_name, mapping in indices.items():
            try:
                if not self.es.indices.exists(index=index_name):
                    self.es.indices.create(index=index_name, body=mapping)
                    logger.info(f"建立索引: {index_name}")
                else:
                    logger.debug(f"索引已存在: {index_name}")
            except Exception as e:
                logger.error(f"建立索引 {index_name} 失敗: {e}")
    
    def _get_product_mapping(self):
        return {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "30s"
            },
            "mappings": {
                "properties": {
                    "product_id": {"type": "keyword"},
                    "product_name": {"type": "text"},
                    "category": {"type": "keyword"},
                    "price": {"type": "float"},
                    "stock_quantity": {"type": "integer"},
                    "last_modified": {"type": "date"}
                }
            }
        }
    
    def _get_warehouse_mapping(self):
        return {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "30s"
            },
            "mappings": {
                "properties": {
                    "warehouse_id": {"type": "keyword"},
                    "product_id": {"type": "keyword"},
                    "quantity": {"type": "integer"},
                    "location": {"type": "keyword"},
                    "last_modified": {"type": "date"}
                }
            }
        }
    
    def _get_complaint_mapping(self):
        return {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "30s"
            },
            "mappings": {
                "properties": {
                    "complaint_id": {"type": "keyword"},
                    "customer_name": {"type": "text"},
                    "product_id": {"type": "keyword"},
                    "description": {"type": "text"},
                    "status": {"type": "keyword"},
                    "created_date": {"type": "date"},
                    "last_modified": {"type": "date"}
                }
            }
        }
    
    def _get_documents_mapping(self):
        return {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "30s"
            },
            "mappings": {
                "properties": {
                    "original_doc_id": {"type": "keyword"},
                    "doc_type": {"type": "keyword"},
                    "doc_number": {"type": "keyword"},
                    "doc_date": {"type": "date"},
                    "file_name": {"type": "text"},
                    "file_url": {"type": "keyword"},
                    "product_category": {"type": "keyword"},
                    "product_codes": {"type": "keyword"},
                    "product_names": {"type": "text"},
                    "applicant": {"type": "keyword"},
                    "department": {"type": "keyword"},
                    "summary": {"type": "text"},
                    "keywords": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "priority": {"type": "keyword"},
                    "last_modified": {"type": "date"}
                }
            }
        }
    

    def bulk_index(self, index_name: str, documents: List[Dict]) -> int:
        """批次索引文件"""
        if not documents:
            return 0
        def sanitize_doc(doc):
            clean = {}
            for k, v in doc.items():
                if isinstance(v, (bytes, bytearray, memoryview)):
                    v = bytes(v).decode("utf-8", errors="ignore")
                clean[k] = v
            return clean

        def ensure_doc_id(doc):
            for key in ("id", "product_id", "complaint_id", "original_doc_id"):
                if key in doc and doc[key] not in (None, ""):
                    return str(doc[key])
            # fallback：穩定雜湊
            blob = json.dumps(doc, sort_keys=True, default=str).encode("utf-8")
            return hashlib.md5(blob).hexdigest()
        
        actions = []
        for doc in documents:
            # 使用適當的 ID 欄位
            doc = sanitize_doc(doc)
            doc_id = ensure_doc_id(doc)
            actions.append({
                "_index": index_name,
                "_id": doc_id,
                "_source": doc
            })
        
        try:
            success, errors = helpers.bulk(
                self.es,
                actions,
                chunk_size=500,
                raise_on_error=False
            )
            
            if errors:
                logger.warning(f"部分文件索引失敗")
            
            logger.info(f"成功索引 {success} 個文件到 {index_name}")
            return success
            
        except Exception as e:
            logger.error(f"批次索引失敗: {e}")
            return 0

class DatabaseManager:
    """MySQL 資料庫管理器"""
    
    def __init__(self):
        self.db_cfg = dict(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            charset='utf8mb4',
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30,
        )
        self.check_table_structure()

    def _new_connection(self):
        return pymysql.connect(**self.db_cfg)
    
    def get_connection(self):
        return self._new_connection()
    
    @contextmanager
    def _conn_ctx(self):
        """ 一次性連線 context manager : 開 -> 用 -> 關 """
        conn = self._new_connection()
        try:
            yield conn
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def _run_query_with_retry(self, sql, params=(), *, max_retries=3):
        """針對偶發封包/EOF錯誤做重試，每次都用新連線"""
        attempt = 0
        while True:
            attempt += 1
            with self._conn_ctx() as conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute(sql, params)
                        rows = cur.fetchall()
                        return list(rows or [])
                except (pymysql.err.OperationalError, pymysql.err.InternalError) as e:
                    # 常見於 Packet sequence/EOF/unpack 半包等
                    if attempt >= max_retries:
                        raise
                    time.sleep(min(0.2 * attempt, 1.0))

    def check_table_structure(self):
        """檢查表結構，確認主鍵欄位"""
        sql = "/* 你的檢查 SQL，例如 DESCRIBE 或 SELECT 1 */ SELECT 1"
        # 用 context manager，一次性開關
        with self._conn_ctx() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                cur.fetchall()
    
    def fetch_product_master(self, last_modified: str, limit: int) -> List[Dict]:
        """獲取產品主檔（根據實際欄位調整）"""
        # 使用實際存在的欄位排序
        sql = """
        SELECT * FROM product_master_a
        WHERE last_modified > %s
        ORDER BY last_modified, product_id
        LIMIT %s
        """
        
        try:
            results = self._run_query_with_retry(sql, (last_modified, limit))

            for row in results:
                # 處理日期
                for field in ['last_modified', 'created_date']:
                    if field in row and row[field]:
                        row[field] = self._format_datetime(row[field])
                
            return results
        except Exception as e:
            logger.error(f"查詢產品主檔失敗: {e}")
            return []
    
    def fetch_product_warehouse(self, last_modified: str, limit: int) -> List[Dict]:
        """獲取倉庫庫存"""
        sql = """
        SELECT * FROM product_warehouse_b
        WHERE last_modified > %s
        ORDER BY last_modified, product_id
        LIMIT %s
        """
        
        try:
            results = self._run_query_with_retry(sql, (last_modified, limit))

            for row in results:
                if 'last_modified' in row and row['last_modified']:
                    row['last_modified'] = self._format_datetime(row['last_modified'])
            
            return results
        except Exception as e:
            logger.error(f"查詢倉庫庫存失敗: {e}")
            return []
    
    def fetch_customer_complaint(self, last_modified: str, limit: int) -> List[Dict]:
        """獲取客訴資料"""
        sql = """
        SELECT * FROM customer_complaint_c
        WHERE last_modified > %s
        ORDER BY last_modified, complaint_id
        LIMIT %s
        """
        
        try:
            results = self._run_query_with_retry(sql, (last_modified, limit))
                
            for row in results:
                for field in ['last_modified', 'created_date', 'resolved_date']:
                    if field in row and row[field]:
                        row[field] = self._format_datetime(row[field])
                
            return results
        except Exception as e:
            logger.error(f"查詢客訴資料失敗: {e}")
            return []
    
    def fetch_structured_documents(self, last_modified: str, limit: int) -> List[Dict]:
        """獲取結構化文件"""
        # structured_documents 表有 id 欄位
        sql = """
        SELECT * FROM structured_documents
        WHERE last_modified > %s
        ORDER BY last_modified, id
        LIMIT %s
        """
        
        try:
            results = self._run_query_with_retry(sql, (last_modified, limit))
                
            for row in results:
                # 解析 JSON 欄位
                for json_field in ['product_codes', 'product_names', 'responsible_units', 'keywords']:
                    if json_field in row and row[json_field]:
                        try:
                            if isinstance(row[json_field], str):
                                row[json_field] = json.loads(row[json_field])
                        except:
                            row[json_field] = []
                    
                    # 處理日期（避免 tzinfo 問題）
                for date_field in ['doc_date', 'parsed_at', 'last_modified']:
                    if date_field in row and row[date_field]:
                        row[date_field] = self._format_datetime(row[date_field])    
            return results
        
        except Exception as e:
            logger.error(f"查詢結構化文件失敗: {e}")
            return []
    
    def _format_datetime(self, dt):
        """格式化日期時間（修正 tzinfo 問題）"""
        from datetime import time
        if dt is None:
            return None
        
        # 如果是字串，直接返回
        if isinstance(dt, str):
            dt_is_str = dt.strip()
            try:
                if "T" in dt_is_str:
                    dt_is_str = dt_is_str.replace("Z", "+00:00")
                    dt_is_str = datetime.fromisoformat(dt_is_str)
                else:
                    for fmt in ("%Y-%m-%d %H:%M:%S, %Y-%m-%d"):
                        try:
                            dt = datetime.strptime(dt_is_str, fmt)
                            break
                        except ValueError:
                            pass
                        if not isinstance(dt, datetime):
                            return dt_is_str
            except Exception:
                return dt_is_str
            
        if isinstance(dt, date) and not isinstance(dt, datetime):
            dt = datetime.combine(dt, time.min)
        
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        
        return dt.isoformat(timespec="seconds").replace("+00:00", "Z")

class StateManager:
    """同步狀態管理器"""
    
    def __init__(self):
        self.state_file = STATE_FILE
        self.states = self.load_states()
    
    def load_states(self) -> Dict:
        """載入狀態"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"載入狀態檔案失敗: {e}")
        
        # 預設狀態
        return {
            'product_master_a': {
                'last_sync': '2000-01-01 00:00:00',
                'count': 0
            },
            'product_warehouse_b': {
                'last_sync': '2000-01-01 00:00:00',
                'count': 0
            },
            'customer_complaint_c': {
                'last_sync': '2000-01-01 00:00:00',
                'count': 0
            },
            'structured_documents': {
                'last_sync': '2000-01-01 00:00:00',
                'count': 0
            }
        }
    
    def save_states(self):
        """儲存狀態"""
        try:
            # 確保目錄存在
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.state_file, 'w') as f:
                json.dump(self.states, f, indent=2, default=str)
            
            logger.debug(f"狀態已儲存到 {self.state_file}")
        except Exception as e:
            logger.error(f"儲存狀態失敗: {e}")
    
    def update_state(self, table: str, last_sync: str, count: int):
        """更新狀態"""
        if table not in self.states:
            self.states[table] = {'last_sync': '2000-01-01 00:00:00', 'count': 0}
        
        self.states[table]['last_sync'] = last_sync
        self.states[table]['count'] += count
        self.save_states()

class DataSynchronizer:
    """主同步類別"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.es = ElasticsearchManager()
        self.state_manager = StateManager()
        self.running = False
        
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        logger.info(f"收到終止訊號 {signum}")
        self.running = False
    
    def sync_table(self, table_name: str, fetch_method, index_name: str) -> int:
        """同步單一表格"""
        try:
            state = self.state_manager.states.get(table_name, {})
            last_sync = state.get('last_sync', '2000-01-01 00:00:00')
            
            logger.info(f"開始同步 {table_name}")
            logger.debug(f"上次同步時間: {last_sync}")
            
            total_synced = 0
            batch_count = 0
            
            while True:
                # 獲取更新資料
                updates = fetch_method(last_sync, PAGE_SIZE)
                
                if not updates:
                    break
                
                logger.debug(f"獲取 {len(updates)} 筆資料")
                
                # 批次索引
                for i in range(0, len(updates), BATCH_SIZE):
                    batch = updates[i:i + BATCH_SIZE]
                    synced = self.es.bulk_index(index_name, batch)
                    total_synced += synced
                    batch_count += 1
                    
                    # 更新最後同步時間
                    if batch:
                        last_record = batch[-1]
                        if 'last_modified' in last_record:
                            last_sync = last_record['last_modified']
                
                # 如果資料少於頁面大小，沒有更多資料
                if len(updates) < PAGE_SIZE:
                    break
                
                time.sleep(0.5)
            
            # 更新狀態
            if total_synced > 0:
                self.state_manager.update_state(table_name, last_sync, total_synced)
                logger.info(f"同步 {table_name} 完成: {total_synced} 筆")
            else:
                logger.debug(f"{table_name} 沒有新資料")
            
            return total_synced
            
        except Exception as e:
            logger.error(f"同步 {table_name} 失敗: {e}", exc_info=True)
            return 0
    
    def sync_all(self):
        """同步所有表格"""
        sync_configs = [
            ('product_master_a', self.db.fetch_product_master, 'erp_product_master'),
            ('product_warehouse_b', self.db.fetch_product_warehouse, 'erp_product_warehouse'),
            ('customer_complaint_c', self.db.fetch_customer_complaint, 'erp_customer_complaint'),
            ('structured_documents', self.db.fetch_structured_documents, 'erp_structured_documents')
        ]
        
        total = 0
        
        # 使用執行緒池並行同步
        with ThreadPoolExecutor(max_workers=PARALLEL_THREADS) as executor:
            futures = []
            for table, method, index in sync_configs:
                future = executor.submit(self.sync_table, table, method, index)
                futures.append((future, table))
            
            for future, table in futures:
                try:
                    result = future.result(timeout=300)
                    total += result
                except Exception as e:
                    logger.error(f"同步 {table} 執行緒異常: {e}")
        
        return total
    
    def run(self):
        """主執行迴圈"""
        self.running = True
        logger.info("MySQL to Elasticsearch 同步服務啟動")
        logger.info(f"設定: BATCH_SIZE={BATCH_SIZE}, PAGE_SIZE={PAGE_SIZE}, THREADS={PARALLEL_THREADS}")
        
        # 第一次執行立即同步
        first_run = True
        
        while self.running:
            try:
                start = time.time()
                total = self.sync_all()
                elapsed = time.time() - start
                
                if total > 0:
                    logger.info(f"本次同步完成: {total} 筆，耗時 {elapsed:.2f} 秒")
                elif first_run:
                    logger.info("初次檢查完成，沒有需要同步的資料")
                
                first_run = False
                
                # 顯示統計
                logger.info("=" * 50)
                logger.info("同步統計:")
                for table, state in self.state_manager.states.items():
                    logger.info(f"  {table}: 總計 {state['count']} 筆, 最後同步: {state['last_sync']}")
                logger.info("=" * 50)
                
                # 等待下一週期
                logger.debug(f"等待 {SLEEP_SECONDS} 秒後進行下一次同步")
                for _ in range(SLEEP_SECONDS):
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"同步服務錯誤: {e}", exc_info=True)
                time.sleep(10)
        
        logger.info("同步服務停止")

if __name__ == "__main__":
    try:
        synchronizer = DataSynchronizer()
        synchronizer.run()
    except KeyboardInterrupt:
        logger.info("收到中斷訊號")
    except Exception as e:
        logger.error(f"服務異常: {e}")
        sys.exit(1)
