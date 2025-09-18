#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
優化版 MySQL to Elasticsearch 直接同步服務
- 支援大量資料處理
- 新增 technical_documents 表同步
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
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "5000"))
PARALLEL_THREADS = int(os.getenv("PARALLEL_THREADS", "4"))
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "30"))

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
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=3600,
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
    """確保索引存在並配置正確"""
    if es.indices.exists(index=index_name):
        logger.info(f"索引 {index_name} 已存在")
        es.indices.put_settings(
            index=index_name,
            body={"index": {"refresh_interval": "30s"}}
        )
        return
    
    # 建立新索引 - 根據不同索引類型使用不同映射
    if index_name == "erp-tech-docs":
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "index.refresh_interval": "30s",
                "analysis": {
                    "normalizer": {
                        "lowercase_normalizer": {
                            "type": "custom",
                            "filter": ["lowercase"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    # 基本欄位
                    "doc_id": {"type": "keyword"},
                    "doc_type": {"type": "keyword"},
                    "doc_number": {"type": "keyword"},
                    "title": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    
                    # 產品關聯
                    "product_ids": {"type": "keyword"},  # JSON 陣列會自動展開
                    
                    # 文檔資訊
                    "revision": {"type": "keyword"},
                    "department": {"type": "keyword"},
                    "author": {"type": "keyword"},
                    "issue_date": {"type": "date", "format": "yyyy-MM-dd||yyyy/MM/dd||strict_date_optional_time||epoch_millis"},
                    
                    # 內容欄位
                    "content": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "summary": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "keywords": {"type": "keyword"},  # JSON 陣列會自動展開
                    
                    # 檔案資訊
                    "file_path": {"type": "keyword"},
                    "file_hash": {"type": "keyword"},
                    "page_count": {"type": "integer"},
                    "file_size": {"type": "long"},
                    
                    # 時間戳記
                    "created_at": {"type": "date", "format": "strict_date_time||epoch_millis"},
                    "updated_at": {"type": "date", "format": "strict_date_time||epoch_millis"},
                    
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
    else:
        # 原有索引的映射（products, warehouse, complaints）
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "index.refresh_interval": "30s",
                "analysis": {
                    "normalizer": {
                        "lowercase_normalizer": {
                            "type": "custom",
                            "filter": ["lowercase"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "type": {"type": "keyword"},
                    "searchable_content": {"type": "text", "analyzer": "standard"},
                    "all_content": {"type": "text", "analyzer": "standard"},
                    "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "product_ids": {"type": "keyword"},
                    "status": {"type": "keyword", "normalizer": "lowercase_normalizer"},
                    "updated_at": {
                        "type": "date",
                        "format": "strict_date_time||epoch_millis||yyyy-MM-dd HH:mm:ss"
                    },
                    "metadata": {"type": "object", "enabled": True, "dynamic": True},
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
    logger.info(f"✅ 建立索引: {index_name}")

# ============== 產品快取 ==============
class ProductCache:
    """產品資訊快取"""
    
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

product_cache = ProductCache()

# ============== 表格配置 ==============
TABLE_CONFIGS = {
    'product_master_a': {
        'index_name': 'erp-products',
        'id_field': 'product_id',
        'type': 'product',
        'page_size': PAGE_SIZE
    },
    'product_warehouse_b': {
        'index_name': 'erp-warehouse',
        'id_field': 'id',
        'type': 'warehouse',
        'page_size': PAGE_SIZE
    },
    'customer_complaint_c': {
        'index_name': 'erp-complaints',
        'id_field': 'complaint_id',
        'type': 'complaint',
        'page_size': PAGE_SIZE
    },
    # 新增技術文檔表配置
    'technical_documents': {
        'index_name': 'erp-tech-docs',
        'id_field': 'doc_id',
        'type': 'tech_doc',
        'page_size': 1000  # 技術文檔較大，減少頁面大小
    }
}

# ============== 同步函數 ==============
def sync_technical_documents(es, state: Dict, force_full: bool = False) -> Dict:
    """
    同步技術文檔表到 Elasticsearch
    
    特別處理：
    1. JSON 欄位解析（product_ids, keywords, metadata）
    2. 日期格式轉換
    3. 大文本內容處理
    """
    table = 'technical_documents'
    config = TABLE_CONFIGS[table]
    index_name = config['index_name']
    
    # 確保索引存在
    ensure_index(es, index_name)
    
    # 檢查上次同步時間
    last_sync = state.get(table, {}).get('last_sync')
    
    if force_full or not last_sync:
        logger.info(f"🔄 執行全量同步: {table}")
        where_clause = "1=1"
    else:
        logger.info(f"🔄 執行增量同步: {table} (從 {last_sync} 開始)")
        where_clause = f"updated_at > '{last_sync}'"
    
    try:
        # 計算總數
        with engine.connect() as conn:
            count_query = text(f"SELECT COUNT(*) as cnt FROM {table} WHERE {where_clause}")
            total = conn.execute(count_query).scalar()
            
            if total == 0:
                logger.info(f"  沒有需要同步的記錄")
                return state
            
            logger.info(f"  需要同步 {total} 筆記錄")
            
            # 分頁查詢
            offset = 0
            synced = 0
            errors = 0
            
            while offset < total:
                # 查詢資料
                query = text(f"""
                    SELECT 
                        doc_id, doc_type, doc_number, title,
                        product_ids, revision, issue_date,
                        department, author, content, summary,
                        keywords, metadata, file_path, file_hash,
                        page_count, file_size, created_at, updated_at
                    FROM {table}
                    WHERE {where_clause}
                    ORDER BY updated_at
                    LIMIT :limit OFFSET :offset
                """)
                
                df = pd.read_sql(query, conn, params={'limit': config['page_size'], 'offset': offset})
                
                if df.empty:
                    break
                
                # 處理資料
                docs = []
                for _, row in df.iterrows():
                    try:
                        # 解析 JSON 欄位
                        product_ids = json.loads(row['product_ids']) if row['product_ids'] else []
                        keywords = json.loads(row['keywords']) if row['keywords'] else []
                        metadata = json.loads(row['metadata']) if row['metadata'] else {}
                        
                        # 準備文檔
                        doc = {
                            "_index": index_name,
                            "_id": row['doc_id'],
                            "_source": {
                                "doc_id": row['doc_id'],
                                "doc_type": row['doc_type'],
                                "doc_number": row['doc_number'],
                                "title": row['title'],
                                "product_ids": product_ids,  # ES 會自動處理陣列
                                "revision": row['revision'],
                                "issue_date": row['issue_date'].isoformat() if pd.notna(row['issue_date']) else None,
                                "department": row['department'],
                                "author": row['author'],
                                "content": row['content'][:100000] if row['content'] else "",  # 限制內容長度
                                "summary": row['summary'],
                                "keywords": keywords,  # ES 會自動處理陣列
                                "metadata": metadata,  # ES 會保持為物件
                                "file_path": row['file_path'],
                                "file_hash": row['file_hash'],
                                "page_count": int(row['page_count']) if pd.notna(row['page_count']) else 0,
                                "file_size": int(row['file_size']) if pd.notna(row['file_size']) else 0,
                                "created_at": row['created_at'].isoformat() if pd.notna(row['created_at']) else None,
                                "updated_at": row['updated_at'].isoformat() if pd.notna(row['updated_at']) else None,
                                
                                # 新增搜尋優化欄位
                                "searchable_content": f"{row['title']} {row['summary'] or ''} {' '.join(keywords)}",
                                "type": row['doc_type']  # 保持與其他索引一致
                            }
                        }
                        docs.append(doc)
                        
                    except Exception as e:
                        logger.error(f"處理文檔 {row['doc_id']} 失敗: {e}")
                        errors += 1
                        continue
                
                # 批次寫入 ES
                if docs:
                    success, failed = 0, 0
                    for ok, result in parallel_bulk(
                        es,
                        docs,
                        thread_count=PARALLEL_THREADS,
                        chunk_size=BATCH_SIZE,
                        raise_on_error=False
                    ):
                        if ok:
                            success += 1
                        else:
                            failed += 1
                            logger.error(f"寫入失敗: {result}")
                    
                    synced += success
                    errors += failed
                    logger.info(f"  進度: {synced}/{total} (成功: {success}, 失敗: {failed})")
                
                offset += config['page_size']
            
            # 更新狀態
            state[table] = {
                'last_sync': datetime.now().isoformat(),
                'total_synced': synced,
                'errors': errors
            }
            
            logger.info(f"✅ {table} 同步完成: {synced} 筆成功, {errors} 筆失敗")
            
    except Exception as e:
        logger.error(f"❌ 同步 {table} 失敗: {e}")
    
    return state

def sync_table(es, table: str, config: Dict, state: Dict, force_full: bool = False) -> Dict:
    """同步單一表格（原有邏輯）"""
    index_name = config['index_name']
    
    # 確保索引存在
    ensure_index(es, index_name)
    
    # 檢查上次同步時間
    last_sync = state.get(table, {}).get('last_sync')
    
    if force_full or not last_sync:
        logger.info(f"🔄 執行全量同步: {table}")
        where_clause = "1=1"
    else:
        logger.info(f"🔄 執行增量同步: {table} (從 {last_sync} 開始)")
        where_clause = f"updated_at > '{last_sync}'"
    
    # ... 原有同步邏輯保持不變 ...
    # （這裡省略原有程式碼，保持原樣）
    
    return state

# ============== 主程式 ==============
def main():
    """主程式"""
    logger.info("=" * 60)
    logger.info("🚀 資料同步服務啟動")
    logger.info(f"批次大小: {BATCH_SIZE}, 頁面大小: {PAGE_SIZE}")
    logger.info(f"執行緒數: {PARALLEL_THREADS}, 同步間隔: {SLEEP_SECONDS}秒")
    logger.info("=" * 60)
    
    # 初始化
    es = get_es_client()
    
    # 載入狀態
    state = {}
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, 'r') as f:
                state = json.load(f)
            logger.info(f"載入狀態: {state}")
        except:
            state = {}
    
    # 載入產品快取
    product_cache.refresh()
    
    # 主循環
    while True:
        try:
            logger.info("\n" + "="*40)
            logger.info(f"⏰ 開始同步週期 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 同步原有表格
            for table, config in TABLE_CONFIGS.items():
                if table == 'technical_documents':
                    # 技術文檔使用特殊同步函數
                    state = sync_technical_documents(es, state)
                else:
                    # 原有表格使用原同步函數
                    state = sync_table(es, table, config, state)
            
            # 儲存狀態
            with open(STATE_PATH, 'w') as f:
                json.dump(state, f, indent=2)
            
            # 每10次同步更新一次產品快取
            if datetime.now().timestamp() % 10 == 0:
                product_cache.refresh()
            
            logger.info(f"💤 休眠 {SLEEP_SECONDS} 秒...")
            time.sleep(SLEEP_SECONDS)
            
        except KeyboardInterrupt:
            logger.info("\n🛑 收到中斷信號，正在關閉...")
            break
        except Exception as e:
            logger.error(f"同步錯誤: {e}", exc_info=True)
            time.sleep(60)  # 錯誤後等待較長時間

if __name__ == "__main__":
    main()
