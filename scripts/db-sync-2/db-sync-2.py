#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL to Elasticsearch 同步服務 - 支援技術文件
統一使用 last_modified 欄位和 ISO 8601 with timezone 格式
"""

import os, json, time, re, logging, pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Generator
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk

# 配置
DB_URL = os.getenv("DB_URL", "mysql+pymysql://root:root@mysql:3306/fuhsin_erp_demo")
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", "admin@12345")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "5000"))
PARALLEL_THREADS = int(os.getenv("PARALLEL_THREADS", "4"))
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "30"))
STATE_PATH = "/state/.sync_state.json"
LOG_PATH = "/logs/db-sync/db_sync.log"

# 日誌設定
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# 資料庫連線
engine = create_engine(
    DB_URL, pool_size=20, pool_pre_ping=True,
    connect_args={"charset": "utf8mb4", "connect_timeout": 10}
)

# ES客戶端
def get_es_client():
    """建立 Elasticsearch 客戶端連線"""
    return Elasticsearch(
        [ES_URL],
        basic_auth=(ES_USER, ES_PASS) if ES_USER else None,
        verify_certs=False, timeout=30, max_retries=3
    )

def format_date_for_es(date_value):
    """
    統一處理日期格式，輸出為 ISO 8601 with timezone
    目標格式: 2025-09-15T16:09:22+00:00
    """
    if pd.isna(date_value) or date_value is None:
        # 返回當前時間的 ISO 格式帶時區
        return datetime.now().strftime('%Y-%m-%dT%H:%M:%S+00:00')
    
    try:
        dt = None
        
        if isinstance(date_value, str):
            # 移除毫秒和時區資訊（如果有的話）
            date_value = date_value.split('.')[0].split('+')[0].split('Z')[0]
            
            # 嘗試各種格式解析
            for fmt in [
                '%Y-%m-%d %H:%M:%S',  # MySQL 標準格式
                '%Y-%m-%dT%H:%M:%S',   # ISO 格式（無時區）
                '%Y/%m/%d %H:%M:%S',
                '%Y-%m-%d',
                '%Y/%m/%d',
                '%Y%m%d'
            ]:
                try:
                    dt = datetime.strptime(date_value, fmt)
                    break
                except ValueError:
                    continue
                    
            if not dt:
                # 最後嘗試 pandas 解析
                try:
                    dt = pd.to_datetime(date_value)
                except:
                    logger.warning(f"無法解析日期: {date_value}")
                    return datetime.now().strftime('%Y-%m-%dT%H:%M:%S+00:00')
                    
        elif hasattr(date_value, 'to_pydatetime'):
            # pandas Timestamp 物件
            dt = date_value.to_pydatetime()
        elif isinstance(date_value, datetime):
            # datetime 物件
            dt = date_value
        else:
            # 其他類型，嘗試轉換
            try:
                dt = pd.to_datetime(date_value)
            except:
                return datetime.now().strftime('%Y-%m-%dT%H:%M:%S+00:00')
        
        # 統一輸出格式：ISO 8601 with timezone (+00:00)
        # 這個格式所有 ES 索引都能接受
        return dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
        
    except Exception as e:
        logger.warning(f"日期格式化失敗: {date_value}, 錯誤: {e}")
        return datetime.now().strftime('%Y-%m-%dT%H:%M:%S+00:00')

# 狀態管理
def load_state() -> Dict:
    """載入同步狀態"""
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, 'r') as f:
                state = json.load(f)
                logger.info(f"載入狀態: {list(state.keys())}")
                return state
        except: 
            pass
    return {}

def save_state(state: Dict):
    """儲存同步狀態"""
    try:
        with open(STATE_PATH, 'w') as f:
            json.dump(state, f, indent=2, default=str)
    except Exception as e:
        logger.error(f"儲存狀態失敗: {e}")

def ensure_index(es, index_name):
    """
    確保索引存在並有正確的映射
    統一使用 last_modified 欄位名稱
    """
    
    # 最寬鬆的日期格式組合，確保能接受各種格式
    date_formats = [
        "strict_date_optional_time",           # ISO 8601 標準格式
        "yyyy-MM-dd HH:mm:ss",                 # MySQL 格式
        "yyyy-MM-dd'T'HH:mm:ss",              # ISO 不帶時區
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",       # ISO 帶毫秒
        "yyyy-MM-dd'T'HH:mm:ssZ",             # ISO 帶時區
        "yyyy-MM-dd'T'HH:mm:ssZZ",            # ISO 帶時區（另一種）
        "yyyy-MM-dd",                          # 只有日期
        "epoch_millis"                         # Unix timestamp
    ]
    date_format = "||".join(date_formats)
    
    # 如果索引已存在
    if es.indices.exists(index=index_name):
        try:
            # 檢查當前映射
            mapping = es.indices.get_mapping(index=index_name)
            logger.info(f"索引 {index_name} 已存在")
            
            # 檢查日期欄位格式（檢查 last_modified 和 updated_at）
            props = mapping[index_name]['mappings'].get('properties', {})
            
            # 如果有 updated_at 欄位，警告需要重建
            if 'updated_at' in props:
                current_format = props['updated_at'].get('format', '')
                logger.warning(f"⚠️  {index_name} 使用 updated_at 欄位，建議重建以使用 last_modified")
                logger.info(f"當前 updated_at 格式: {current_format}")
                
            # 檢查 last_modified 欄位
            if 'last_modified' in props:
                current_format = props['last_modified'].get('format', '')
                logger.info(f"當前 last_modified 格式: {current_format}")
                    
        except Exception as e:
            logger.warning(f"檢查索引映射失敗: {e}")
    else:
        # 創建新索引，統一使用 last_modified 欄位
        mapping = {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 1,
                "index.mapping.ignore_malformed": True,  # 忽略格式錯誤
                "index.mapping.coerce": True             # 自動轉換類型
            },
            "mappings": {
                "properties": {
                    # 基本欄位
                    "doc_id": {"type": "keyword"},
                    "type": {"type": "keyword"},
                    "title": {"type": "text", "analyzer": "standard"},
                    "content": {"type": "text", "analyzer": "standard"},
                    "product_ids": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "metadata": {"type": "object", "enabled": True, "dynamic": True},
                    
                    # 統一使用 last_modified 作為時間欄位
                    "last_modified": {
                        "type": "date",
                        "format": date_format,
                        "ignore_malformed": True
                    },
                    
                    # 如果有創建時間，也使用相同格式
                    "created_at": {
                        "type": "date",
                        "format": date_format,
                        "ignore_malformed": True
                    }
                }
            }
        }
        
        es.indices.create(index=index_name, body=mapping)
        logger.info(f"✅ 創建索引: {index_name} (使用 last_modified 欄位)")

# 產品快取類別
class ProductCache:
    """產品資訊快取，用於快速查詢產品名稱"""
    
    def __init__(self):
        self.products = {}
        
    def refresh(self):
        """從資料庫載入所有產品資訊"""
        try:
            with engine.connect() as conn:
                df = pd.read_sql("SELECT product_id, product_name FROM product_master_a", conn)
                self.products = {
                    str(row['product_id']): str(row['product_name']) 
                    for _, row in df.iterrows() 
                    if pd.notna(row['product_id'])
                }
                logger.info(f"載入 {len(self.products)} 個產品到快取")
        except Exception as e:
            logger.error(f"載入產品快取失敗: {e}")
            self.products = {}
    
    def get_name(self, pid):
        """根據產品ID取得產品名稱"""
        if not pid or pd.isna(pid):
            return "未知產品"
        return self.products.get(str(pid), str(pid))

# 創建全域產品快取實例
product_cache = ProductCache()

def fetch_data_in_pages(table: str, since_time, page_size: int = PAGE_SIZE) -> Generator:
    """
    分頁查詢資料，避免記憶體溢出
    """
    offset = 0
    with engine.connect() as conn:
        while True:
            if since_time:
                # 確保 since_time 是字串格式
                if isinstance(since_time, dict):
                    logger.error(f"錯誤的時間格式: {since_time}")
                    since_time = None
                    query = f"SELECT * FROM {table} ORDER BY last_modified LIMIT {page_size} OFFSET {offset}"
                else:
                    since_str = since_time if isinstance(since_time, str) else str(since_time)
                    # 使用參數化查詢避免 SQL 注入
                    query = f"SELECT * FROM {table} WHERE last_modified > '{since_str}' ORDER BY last_modified LIMIT {page_size} OFFSET {offset}"
            else:
                query = f"SELECT * FROM {table} ORDER BY last_modified LIMIT {page_size} OFFSET {offset}"
            
            df = pd.read_sql(query, conn)
            if df.empty: 
                break
            
            logger.info(f"{table}: 取得 {len(df)} 筆資料 (offset={offset})")
            yield df
            
            offset += page_size
            if offset >= 1000000:  # 防止無限循環
                break

def process_products(df):
    """處理產品主檔資料"""
    if df.empty:
        return
        
    for _, row in df.iterrows():
        if pd.isna(row.get('product_id')):
            continue
            
        pid = str(row['product_id'])
            
        yield {
            "_id": f"product_{pid}",
            "_index": "erp-products",
            "doc_id": f"product_{pid}",
            "title": f"[{pid}] {row.get('product_name', '')}",
            "content": f"{row.get('description', '')} {row.get('specifications', '')}",
            "product_ids": [pid],
            "metadata": {
                "category": row.get('category'),
                "supplier": row.get('supplier'),
                "price": float(row['price']) if pd.notna(row.get('price')) else None
            },
            # 統一使用 last_modified 欄位名稱
            "last_modified": format_date_for_es(row.get('last_modified'))
        }

def process_warehouse(df):
    """處理倉儲資料"""
    if df.empty:
        return
        
    for idx, row in df.iterrows():
        # 使用 index 或其他唯一識別碼
        # 檢查可能的主鍵欄位名稱
        if 'warehouse_id' in row and pd.notna(row['warehouse_id']):
            wid = str(row['warehouse_id'])
        elif 'id' in row and pd.notna(row['id']):
            wid = str(row['id'])
        else:
            # 使用 product_id + warehouse_location 作為唯一識別
            wid = f"{row.get('product_id', idx)}_{row.get('warehouse_location', idx)}"
        
        pid = str(row.get('product_id', ''))
            
        yield {
            "_id": f"warehouse_{wid}",
            "_index": "erp-warehouse",
            "doc_id": f"warehouse_{wid}",
            "title": f"[{row.get('warehouse_location')}] {product_cache.get_name(pid)}",
            "content": str(row.get('special_notes', '')),
            "product_ids": [pid] if pid else [],
            "metadata": {
                "product_id": pid,
                "quantity": int(row['quantity']) if pd.notna(row.get('quantity')) else 0,
                "location": row.get('warehouse_location'),
                "manager": row.get('manager')
            },
            # 統一使用 last_modified 欄位名稱
            "last_modified": format_date_for_es(row.get('last_modified'))
        }

def process_complaints(df):
    """處理客訴資料"""
    if df.empty:
        return
        
    for _, row in df.iterrows():
        if pd.isna(row.get('complaint_id')):
            continue
            
        cid = str(row['complaint_id'])
            
        yield {
            "_id": f"complaint_{cid}",
            "_index": "erp-complaints",
            "doc_id": f"complaint_{cid}",
            "title": f"[{cid}] {row.get('customer_company', '')} - {row.get('complaint_type')}",
            "content": row.get('description', ''),
            "product_ids": [],
            "metadata": {
                "customer": row.get('customer_company'),
                "type": row.get('complaint_type'),
                "status": row.get('status'),
                "severity": row.get('severity')
            },
            # 統一使用 last_modified 欄位名稱
            "last_modified": format_date_for_es(row.get('last_modified'))
        }

def process_tech_docs(df):
    """處理技術文件資料"""
    if df.empty:
        return
        
    for _, row in df.iterrows():
        # 必須有 doc_id
        if pd.isna(row.get('doc_id')):
            continue
            
        doc_id = str(row['doc_id'])
        
        # 解析JSON欄位
        product_ids = []
        try:
            if pd.notna(row.get('product_ids')):
                pids = json.loads(row['product_ids']) if isinstance(row['product_ids'], str) else row['product_ids']
                product_ids = pids if isinstance(pids, list) else []
        except: 
            pass
        
        yield {
            "_id": f"tech_doc_{doc_id}",
            "_index": "erp-technical-docs",
            "doc_id": doc_id,
            "title": row.get('title') or row.get('doc_number') or row.get('file_name', ''),
            "content": row.get('content', ''),
            "product_ids": product_ids,
            "metadata": {
                "doc_type": row.get('doc_type'),
                "doc_number": row.get('doc_number'),
                "author": row.get('author'),
                "revision": row.get('revision'),
                "file_size": int(row['file_size']) if pd.notna(row.get('file_size')) else None,
                "page_count": int(row['page_count']) if pd.notna(row.get('page_count')) else None
            },
            # 統一使用 last_modified 欄位名稱
            "last_modified": format_date_for_es(row.get('last_modified'))
        }

def sync_table(table_name: str, processor, es_client, state: Dict) -> bool:
    """同步單一資料表"""
    
    # 從狀態中取得最後同步時間
    since = state.get(table_name)
    if isinstance(since, dict):
        # 如果是字典，嘗試取得時間戳
        since = since.get('last_sync')
    
    logger.info(f"同步 {table_name}, 起始: {since or '初始同步'}")
    
    # 設定索引名稱映射
    index_map = {
        "product_master_a": "erp-products",
        "product_warehouse_b": "erp-warehouse",
        "customer_complaint_c": "erp-complaints",
        "technical_documents": "erp-technical-docs"
    }
    
    # 確保索引存在
    if table_name in index_map:
        ensure_index(es_client, index_map[table_name])
    
    total_success = 0
    total_failed = 0
    max_time = since
    
    # 分頁處理資料
    try:
        for df in fetch_data_in_pages(table_name, since):
            if df.empty:
                continue
                
            # 先檢查資料結構（除錯用）
            if table_name == "product_warehouse_b" and "warehouse_id" not in df.columns:
                logger.warning(f"{table_name} 沒有 warehouse_id 欄位，可用欄位: {list(df.columns)}")
            
            # 處理資料
            docs = list(processor(df))
            if not docs: 
                continue
            
            # 批量寫入 Elasticsearch
            for success, info in parallel_bulk(
                es_client, docs,
                thread_count=PARALLEL_THREADS,
                chunk_size=500,
                raise_on_error=False,
                raise_on_exception=False
            ):
                if success: 
                    total_success += 1
                else:
                    total_failed += 1
                    if total_failed <= 5:  # 只記錄前5個錯誤避免日誌爆炸
                        logger.error(f"索引失敗: {info}")
            
            # 更新最大時間戳（用於下次同步）
            if 'last_modified' in df.columns:
                page_max = df['last_modified'].max()
                if pd.notna(page_max):
                    max_time = str(page_max) if not max_time or page_max > pd.Timestamp(max_time or '1970-01-01') else max_time
                    
    except Exception as e:
        logger.error(f"處理 {table_name} 時發生錯誤: {e}", exc_info=True)
        return False
    
    # 更新狀態 - 只有真正有同步資料才更新
    if total_success > 0:
        if max_time and max_time != since:
            state[table_name] = str(max_time)  # 確保儲存為字串
        logger.info(f"✅ {table_name}: 同步成功 {total_success} 筆, 失敗 {total_failed} 筆" + 
                   (f", 更新到 {max_time}" if max_time else ""))
        return True
    elif total_failed > 0:
        logger.error(f"❌ {table_name}: 全部失敗 {total_failed} 筆")
        return False
    else:
        logger.info(f"💤 {table_name}: 無新資料")
        return False

def main():
    """主程式"""
    logger.info("=" * 50)
    logger.info("🚀 MySQL to Elasticsearch 同步服務啟動")
    logger.info(f"📊 使用統一欄位名稱: last_modified")
    logger.info(f"📊 日期格式: ISO 8601 with timezone (+00:00)")
    
    # 初始化
    es = get_es_client()
    state = load_state()
    product_cache.refresh()
    
    # 檢查是否需要重建索引（透過環境變數控制）
    rebuild_indexes = os.getenv("REBUILD_INDEXES", "false").lower() == "true"
    if rebuild_indexes:
        logger.warning("⚠️  索引重建模式啟用")
        indexes_to_rebuild = [
            "erp-products", 
            "erp-warehouse", 
            "erp-complaints", 
            "erp-technical-docs"
        ]
        for idx in indexes_to_rebuild:
            if es.indices.exists(index=idx):
                logger.info(f"刪除索引: {idx}")
                es.indices.delete(index=idx)
        # 清空狀態，強制全量同步
        state = {}
        save_state(state)
    
    # 同步表配置
    tables = {
        "product_master_a": process_products,
        "product_warehouse_b": process_warehouse,
        "customer_complaint_c": process_complaints,
        "technical_documents": process_tech_docs
    }
    
    # 第一次執行時檢查資料表結構
    if not state or rebuild_indexes:
        logger.info("首次執行或重建模式，檢查資料表結構...")
        with engine.connect() as conn:
            for table_name in tables.keys():
                try:
                    # 檢查表是否存在並取得欄位資訊
                    check = f"SHOW TABLES LIKE '{table_name}'"
                    if not pd.read_sql(check, conn).empty:
                        # 取得前1筆資料檢查欄位
                        sample = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 1", conn)
                        if not sample.empty:
                            logger.info(f"{table_name} 欄位: {list(sample.columns)[:10]}...")
                    else:
                        logger.warning(f"{table_name} 資料表不存在")
                except Exception as e:
                    logger.error(f"檢查 {table_name} 失敗: {e}")
    
    # 主循環
    consecutive_no_updates = 0
    while True:
        try:
            has_updates = False
            
            # 同步各資料表
            for table_name, processor in tables.items():
                try:
                    # 檢查表是否存在
                    with engine.connect() as conn:
                        check = f"SHOW TABLES LIKE '{table_name}'"
                        if not pd.read_sql(check, conn).empty:
                            if sync_table(table_name, processor, es, state):
                                has_updates = True
                                save_state(state)
                        else:
                            logger.debug(f"跳過不存在的表: {table_name}")
                except Exception as e:
                    logger.error(f"同步 {table_name} 失敗: {e}")
            
            # 動態調整睡眠時間
            if has_updates:
                consecutive_no_updates = 0
                sleep_time = 5  # 有更新時短暫等待
            else:
                consecutive_no_updates += 1
                # 逐漸增加睡眠時間，最多5分鐘
                sleep_time = min(SLEEP_SECONDS * (1 + consecutive_no_updates // 5), 300)
            
            logger.info(f"💤 等待 {sleep_time} 秒...")
            time.sleep(sleep_time)
            
        except KeyboardInterrupt:
            logger.info("停止服務")
            break
        except Exception as e:
            logger.error(f"主循環錯誤: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()
