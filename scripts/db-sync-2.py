#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
優化版 MySQL to Elasticsearch 直接同步服務
- 支援大量資料處理
- 分頁查詢避免記憶體溢出
- 平行處理提升效能
- 智能產品關聯
"""

import os, json, time, re, logging, pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Generator
from sqlalchemy import create_engine, text
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, BulkIndexError

# ============== 配置 ==============
DB_URL = os.getenv("DB_URL", "mysql+pymysql://root:root@mysql:3306/fuhsin_erp_demo")
ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", "admin@12345")

# 效能相關配置
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))         # 每批處理數量
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "5000"))           # 分頁查詢大小
PARALLEL_THREADS = int(os.getenv("PARALLEL_THREADS", "4")) # 平行執行緒數
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "30"))     # 同步間隔

# 檔案路徑
STATE_PATH = "/state/.sync_state.json"
LOG_PATH = "/logs/db-sync/db_sync.log"

# ============== 日誌設定 ==============
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============== 資料庫連線池 ==============
engine = create_engine(
    DB_URL,
    pool_size=20,           # 連線池大小
    max_overflow=10,        # 最大溢出連線
    pool_pre_ping=True,     # 連線前檢查
    pool_recycle=3600,      # 連線回收時間
    echo=False,
    connect_args={
        "charset": "utf8mb4",
        "connect_timeout": 10,
        "init_command": (
            "SET SESSION sql_mode = "
            "REPLACE(REPLACE(@@sql_mode,'NO_ZERO_DATE',''),'NO_ZERO_IN_DATE','')"
        )
    }
)

# ============== Elasticsearch 客戶端 ==============
def get_es_client():
    """建立 Elasticsearch 客戶端"""
    return Elasticsearch(
        [ES_URL],
        basic_auth=(ES_USER, ES_PASS) if ES_USER and ES_PASS else None,
        verify_certs=False,
        timeout=30,
        max_retries=3,
        retry_on_timeout=True
    )

# ============== 索引管理 ==============
def ensure_index(es, index_name):
    """確保索引存在並設定正確的 mapping"""
    if not es.indices.exists(index=index_name):
        mapping = {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 1,
                "refresh_interval": "30s",  # 延遲刷新提升寫入效能
                "index": {
                    "max_result_window": 50000,  # 增加查詢窗口
                    "max_terms_count": 65536     # 增加 terms 查詢限制
                },
                "analysis": {
                    "analyzer": {
                        "ik_smart": {
                            "type": "custom",
                            "tokenizer": "ik_smart"
                        },
                        "ik_max_word": {
                            "type": "custom", 
                            "tokenizer": "ik_max_word"
                        }
                    },
                    "normalizer": {
                        "lowercase_normalizer": {
                            "type": "custom",
                            "filter": ["lowercase", "asciifolding"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    # 基本欄位
                    "type": {"type": "keyword"},
                    "id": {"type": "keyword"},
                    "doc_id": {"type": "keyword"},
                    
                    # 文字搜尋欄位
                    "title": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart",
                        "fields": {
                            "keyword": {"type": "keyword", "ignore_above": 256}
                        }
                    },
                    "content": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart"
                    },
                    "all_content": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart"
                    },
                    "searchable_content": {
                        "type": "text",
                        "analyzer": "ik_max_word",
                        "search_analyzer": "ik_smart"
                    },
                    
                    # 精確搜尋欄位
                    "product_ids": {"type": "keyword"},
                    "status": {"type": "keyword", "normalizer": "lowercase_normalizer"},
                    "warehouse_location": {"type": "keyword"},
                    "severity": {"type": "keyword"},
                    
                    # 時間欄位
                    "updated_at": {
                        "type": "date",
                        "format": "strict_date_time||epoch_millis||yyyy-MM-dd HH:mm:ss"
                    },
                    
                    # 元資料
                    "metadata": {
                        "type": "object",
                        "enabled": True,
                        "dynamic": True
                    },
                    
                    # 向量欄位（為未來預留）
                    "content_vector": {
                        "type": "dense_vector",
                        "dims": 1536,
                        "index": True,
                        "similarity": "cosine"
                    }
                }
            }
        }
        es.indices.create(index=index_name, body=mapping)
        logger.info(f"✅ 創建索引: {index_name}")
    else:
        # 更新現有索引的設定
        es.indices.put_settings(
            index=index_name,
            body={"index": {"refresh_interval": "30s"}}
        )

# ============== 產品快取 ==============
class ProductCache:
    """產品資訊快取，用於快速關聯查詢"""
    
    def __init__(self):
        self.products = {}
        self.last_refresh = None
        
    def refresh(self):
        """從資料庫載入所有產品資訊"""
        try:
            with engine.connect() as conn:
                query = text("""
                    SELECT product_id, product_name, category, status, supplier 
                    FROM product_master_a
                """)
                df = pd.read_sql(query, conn)
                
                self.products = {
                    row['product_id']: {
                        'name': row['product_name'],
                        'category': row['category'],
                        'status': row['status'],
                        'supplier': row['supplier']
                    }
                    for _, row in df.iterrows()
                }
                self.last_refresh = datetime.now()
                logger.info(f"📦 載入 {len(self.products)} 個產品到快取")
        except Exception as e:
            logger.error(f"載入產品快取失敗: {e}")
    
    def get(self, product_id: str) -> Optional[Dict]:
        """取得產品資訊"""
        # 每小時更新一次
        if not self.last_refresh or (datetime.now() - self.last_refresh).seconds > 3600:
            self.refresh()
        return self.products.get(product_id)
    
    def extract_product_ids(self, text: str) -> List[str]:
        """從文字中提取產品 ID"""
        # 支援 P 開頭的產品編號
        pattern = r'\b[P]\d{3}\b'
        found_ids = list(set(re.findall(pattern, str(text))))
        
        # 驗證 ID 是否存在
        valid_ids = [pid for pid in found_ids if pid in self.products]
        
        # 如果提到產品名稱，也找出對應 ID
        for pid, info in self.products.items():
            if info['name'] and info['name'] in str(text):
                if pid not in valid_ids:
                    valid_ids.append(pid)
        
        return valid_ids

# 全域產品快取
product_cache = ProductCache()

# ============== 狀態管理 ==============
def load_state():
    """載入同步狀態"""
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, "r") as f:
            return json.load(f)
    return {}

def save_state(state):
    """儲存同步狀態"""
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2, default=str)

# ============== 資料處理 ==============
def process_product_master(df):
    """處理產品主檔資料"""
    for _, row in df.iterrows():
        product_id = str(row['product_id'])
        
        # 組合各種搜尋欄位
        all_fields = [
            product_id,
            row.get('product_name', ''),
            row.get('product_model', ''),
            row.get('category', ''),
            row.get('supplier', ''),
            row.get('status', '')
        ]
        
        doc = {
            "_id": f"product_{product_id}",
            "_index": "erp-products",
            "type": "product_master",
            "id": product_id,
            "doc_id": f"product_{product_id}",
            "title": f"[{product_id}] {row.get('product_name', '')} ({row.get('product_model', '')})",
            "content": f"型號: {row.get('product_model')}; 分類: {row.get('category')}; 供應商: {row.get('supplier')}; 狀態: {row.get('status')}; 價格: {row.get('price')}; 庫存: {row.get('stock_qty')}",
            "all_content": " ".join(str(f) for f in all_fields if f),
            "searchable_content": f"產品編號 {product_id} 產品名稱 {row.get('product_name')} 型號 {row.get('product_model')} 分類 {row.get('category')}",
            "product_ids": [product_id],
            "status": row.get('status'),
            "metadata": {
                "product_name": row.get('product_name'),
                "product_model": row.get('product_model'),
                "price": float(row['price']) if pd.notna(row.get('price')) else None,
                "stock_qty": int(row['stock_qty']) if pd.notna(row.get('stock_qty')) else None,
                "category": row.get('category'),
                "supplier": row.get('supplier'),
                "manufacture_date": str(row.get('manufacture_date')) if row.get('manufacture_date') else None
            },
            "updated_at": row.get('last_modified', datetime.now())
        }
        
        # 動態添加產品特定欄位
        for col in df.columns:
            if col.startswith('field_') and col not in doc['metadata']:
                doc['metadata'][col] = row.get(col)
        
        yield doc

def process_warehouse(df):
    """處理倉庫資料"""
    for _, row in df.iterrows():
        product_id = str(row.get('product_id', ''))
        location = row.get('warehouse_location', '')
        
        # 從快取取得產品資訊
        product_info = product_cache.get(product_id)
        product_name = row.get('product_name') or (product_info['name'] if product_info else '')
        
        # 提取相關產品 ID
        related_ids = product_cache.extract_product_ids(row.get('special_notes', ''))
        all_product_ids = [product_id] if product_id else []
        all_product_ids.extend([pid for pid in related_ids if pid not in all_product_ids])
        
        doc = {
            "_id": f"warehouse_{product_id}_{location.replace(' ', '_')}",
            "_index": "erp-warehouse",
            "type": "warehouse",
            "id": f"{product_id}:{location}",
            "doc_id": f"warehouse_{product_id}_{location.replace(' ', '_')}",
            "title": f"[{product_id}] {product_name} @ {location}",
            "content": f"庫存數量: {row.get('quantity')}; 最低庫存: {row.get('min_stock_level')}; 管理人: {row.get('manager')}; 備註: {row.get('special_notes')}",
            "all_content": f"{product_id} {product_name} {location} {row.get('special_notes')}",
            "searchable_content": f"產品 {product_id} {product_name} 倉庫 {location} 數量 {row.get('quantity')}",
            "product_ids": all_product_ids,
            "warehouse_location": location,
            "metadata": {
                "product_id": product_id,
                "product_name": product_name,
                "warehouse_location": location,
                "quantity": int(row['quantity']) if pd.notna(row.get('quantity')) else 0,
                "min_stock_level": int(row['min_stock_level']) if pd.notna(row.get('min_stock_level')) else 0,
                "manager": row.get('manager'),
                "special_notes": row.get('special_notes'),
                "last_inventory_date": str(row.get('last_inventory_date')) if row.get('last_inventory_date') else None
            },
            "updated_at": row.get('last_modified', datetime.now())
        }
        
        yield doc

def process_complaints(df):
    """處理客訴資料"""
    for _, row in df.iterrows():
        complaint_id = str(row['complaint_id'])
        description = row.get('description', '')
        
        # 提取產品 ID
        product_ids = product_cache.extract_product_ids(description)
        
        # 取得產品名稱
        product_names = []
        for pid in product_ids:
            info = product_cache.get(pid)
            if info:
                product_names.append(f"{pid}({info['name']})")
        
        doc = {
            "_id": f"complaint_{complaint_id}",
            "_index": "erp-complaints",
            "type": "complaint",
            "id": complaint_id,
            "doc_id": f"complaint_{complaint_id}",
            "title": f"[{complaint_id}] {row.get('customer_company', '')} - {row.get('complaint_type')} ({row.get('status')})",
            "content": description,
            "all_content": f"{row.get('customer_name')} {row.get('customer_company')} {description} {row.get('complaint_type')}",
            "searchable_content": f"客訴編號 {complaint_id} 客戶 {row.get('customer_company')} 類型 {row.get('complaint_type')} {description}",
            "product_ids": product_ids,
            "status": row.get('status'),
            "severity": row.get('severity'),
            "metadata": {
                "complaint_date": str(row.get('complaint_date')) if row.get('complaint_date') else None,
                "customer_name": row.get('customer_name'),
                "customer_company": row.get('customer_company'),
                "complaint_type": row.get('complaint_type'),
                "severity": row.get('severity'),
                "handler": row.get('handler'),
                "resolution_date": str(row.get('resolution_date')) if row.get('resolution_date') else None,
                "related_products": ", ".join(product_names) if product_names else None
            },
            "updated_at": row.get('last_modified', datetime.now())
        }
        
        yield doc

# ============== 分頁查詢 ==============
def fetch_data_in_pages(table: str, since, page_size: int = PAGE_SIZE) -> Generator:
    """分頁查詢資料，避免記憶體溢出"""
    offset = 0
    total_fetched = 0
    
    with engine.connect() as conn:
        while True:
            # 建構查詢
            if since:
                query = text(f"""
                    SELECT * FROM {table}
                    WHERE last_modified > :since
                    ORDER BY last_modified ASC
                    LIMIT :limit OFFSET :offset
                """)
                params = {'since': since, 'limit': page_size, 'offset': offset}
            else:
                query = text(f"""
                    SELECT * FROM {table}
                    ORDER BY last_modified ASC
                    LIMIT :limit OFFSET :offset
                """)
                params = {'limit': page_size, 'offset': offset}
            
            # 執行查詢
            df = pd.read_sql(query, conn, params=params)
            
            if df.empty:
                break
            
            total_fetched += len(df)
            logger.info(f"📊 {table}: 取得第 {offset+1}-{offset+len(df)} 筆資料")
            
            yield df
            
            offset += page_size
            
            # 防止無限循環
            if total_fetched >= 1000000:  # 最多處理 100 萬筆
                logger.warning(f"⚠️ {table}: 已處理 100 萬筆，停止")
                break

# ============== 同步函數 ==============
def sync_table(table_name: str, processor, es_client, state: Dict) -> bool:
    """同步單一資料表"""
    since = state.get(table_name)
    
    logger.info(f"🔄 開始同步 {table_name}，起始時間: {since or '初始同步'}")
    
    # 確保索引存在
    if table_name == "product_master_a":
        ensure_index(es_client, "erp-products")
    elif table_name == "product_warehouse_b":
        ensure_index(es_client, "erp-warehouse")
    elif table_name == "customer_complaint_c":
        ensure_index(es_client, "erp-complaints")
    
    total_success = 0
    total_failed = 0
    max_timestamp = since
    
    # 分頁處理資料
    for page_df in fetch_data_in_pages(table_name, since):
        # 產生文檔
        docs = list(processor(page_df))
        
        if not docs:
            continue
        
        # 使用 parallel_bulk 提升效能
        try:
            for success, info in parallel_bulk(
                es_client,
                docs,
                thread_count=PARALLEL_THREADS,
                chunk_size=500,
                raise_on_error=False,
                raise_on_exception=False
            ):
                if success:
                    total_success += 1
                else:
                    total_failed += 1
                    logger.error(f"批量索引失敗: {info}")
        
        except BulkIndexError as e:
            logger.error(f"批量索引錯誤: {e}")
            for error in e.errors:
                logger.error(f"  詳細錯誤: {error}")
        
        # 更新最大時間戳
        if 'last_modified' in page_df.columns:
            page_max = page_df['last_modified'].max()
            if pd.notna(page_max):
                if max_timestamp is None or page_max > pd.Timestamp(max_timestamp):
                    max_timestamp = page_max
    
    # 更新狀態
    if max_timestamp and max_timestamp != since:
        state[table_name] = str(max_timestamp)
        logger.info(f"✅ {table_name}: 成功 {total_success} 筆, 失敗 {total_failed} 筆, 更新到 {max_timestamp}")
        return True
    elif total_success > 0:
        logger.info(f"✅ {table_name}: 成功 {total_success} 筆, 失敗 {total_failed} 筆")
        return True
    else:
        logger.info(f"💤 {table_name}: 無新資料")
        return False

# ============== 快速檢查 ==============
def check_recent_changes(minutes: int = 5) -> Dict[str, int]:
    """快速檢查最近的變更"""
    recent_threshold = datetime.now() - timedelta(minutes=minutes)
    changes = {}
    
    with engine.connect() as conn:
        for table in ['product_master_a', 'product_warehouse_b', 'customer_complaint_c']:
            query = text(f"""
                SELECT COUNT(*) as cnt 
                FROM {table} 
                WHERE last_modified > :threshold
            """)
            result = conn.execute(query, {'threshold': recent_threshold})
            count = result.scalar()
            if count > 0:
                changes[table] = count
    
    return changes

# ============== 主程式 ==============
def main():
    logger.info("=" * 60)
    logger.info("🚀 MySQL to Elasticsearch 直接同步服務啟動")
    logger.info(f"📊 配置: BATCH={BATCH_SIZE}, PAGE={PAGE_SIZE}, THREADS={PARALLEL_THREADS}")
    logger.info("=" * 60)
    
    # 初始化
    es = get_es_client()
    state = load_state()
    product_cache.refresh()
    
    # 定義同步表
    tables = {
        "product_master_a": process_product_master,
        "product_warehouse_b": process_warehouse,
        "customer_complaint_c": process_complaints
    }
    
    # 初始全量同步檢查
    first_run = not state
    if first_run:
        logger.info("📥 首次執行，開始全量同步...")
    
    # 主循環
    consecutive_no_updates = 0
    last_quick_check = time.time()
    
    while True:
        try:
            has_updates = False
            
            # 快速檢查（每分鐘）
            if time.time() - last_quick_check > 60:
                changes = check_recent_changes(5)
                if changes:
                    logger.info(f"⚡ 快速檢查發現變更: {changes}")
                last_quick_check = time.time()
            
            # 同步各表
            for table_name, processor in tables.items():
                try:
                    updated = sync_table(table_name, processor, es, state)
                    if updated:
                        has_updates = True
                        save_state(state)
                        consecutive_no_updates = 0
                except Exception as e:
                    logger.error(f"❌ 同步 {table_name} 失敗: {e}", exc_info=True)
            
            # 動態調整睡眠時間
            if not has_updates:
                consecutive_no_updates += 1
                # 如果連續多次無更新，延長睡眠時間
                sleep_time = min(SLEEP_SECONDS * (1 + consecutive_no_updates // 5), 300)
                logger.info(f"💤 無新資料，等待 {sleep_time} 秒...")
                time.sleep(sleep_time)
            else:
                # 有更新時短暫等待
                time.sleep(5)
            
        except KeyboardInterrupt:
            logger.info("⏹️ 收到中斷信號，正在關閉...")
            break
        except Exception as e:
            logger.error(f"❌ 主循環錯誤: {e}", exc_info=True)
            time.sleep(30)
    
    logger.info("👋 同步服務已停止")

if __name__ == "__main__":
    main()
