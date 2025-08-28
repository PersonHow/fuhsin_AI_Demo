#!/usr/bin/env python3
"""
多資料庫連接器 - 主動從各種資料庫拉取資料
取代原本的被動等待 .sql 檔案
"""

import os
import time
import yaml
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
import hashlib
import json
import requests

class DatabaseConnector:
    def __init__(self):
        self.engines = {}
        self.init_connections()
        
    def init_connections(self):
        """初始化所有資料庫連接"""
        
        # MySQL 連接
        if os.getenv('MYSQL_HOST'):
            mysql_url = (
                f"mysql+pymysql://{os.getenv('MYSQL_USER')}:"
                f"{os.getenv('MYSQL_PASSWORD')}@"
                f"{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}"
            )
            self.engines['mysql'] = create_engine(mysql_url)
            print(f"✓ 連接 MySQL: {os.getenv('MYSQL_HOST')}")
        
        # PostgreSQL 連接
        if os.getenv('PG_HOST'):
            pg_url = (
                f"postgresql://{os.getenv('PG_USER')}:"
                f"{os.getenv('PG_PASSWORD')}@"
                f"{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}"
            )
            self.engines['postgres'] = create_engine(pg_url)
            print(f"✓ 連接 PostgreSQL: {os.getenv('PG_HOST')}")
    
    def get_databases(self, engine_type):
        """取得所有資料庫清單"""
        if engine_type == 'mysql':
            query = "SHOW DATABASES"
        elif engine_type == 'postgres':
            query = "SELECT datname FROM pg_database WHERE datistemplate = false"
        
        with self.engines[engine_type].connect() as conn:
            result = conn.execute(text(query))
            return [row[0] for row in result]
    
    def get_tables(self, engine_type, database):
        """取得指定資料庫的所有表"""
        if engine_type == 'mysql':
            query = f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{database}'
            """
        elif engine_type == 'postgres':
            query = f"""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public'
            """
        
        # 建立包含資料庫名稱的連接
        engine_url = str(self.engines[engine_type].url)
        if engine_type == 'mysql':
            engine_url = f"{engine_url}/{database}"
        elif engine_type == 'postgres':
            engine_url = f"{engine_url}/{database}"
        
        temp_engine = create_engine(engine_url)
        with temp_engine.connect() as conn:
            result = conn.execute(text(query))
            return [row[0] for row in result]
    
    def sync_table(self, engine_type, database, table, last_sync=None):
        """同步單一表到 Elasticsearch"""
        print(f"📊 同步 {engine_type}.{database}.{table}")
        
        # 建立查詢
        if last_sync:
            # 增量同步（假設有 updated_at 欄位）
            query = f"""
                SELECT * FROM {table} 
                WHERE updated_at > '{last_sync}'
            """
        else:
            # 全量同步
            query = f"SELECT * FROM {table}"
        
        # 讀取資料
        engine_url = f"{str(self.engines[engine_type].url)}/{database}"
        temp_engine = create_engine(engine_url)
        
        df = pd.read_sql(query, temp_engine)
        
        # 轉換為 Elasticsearch 文檔
        documents = []
        for idx, row in df.iterrows():
            doc = {
                "@timestamp": datetime.utcnow().isoformat(),
                "source": {
                    "system": engine_type,
                    "database": database,
                    "table": table
                },
                "data": row.to_dict()
            }
            
            # 生成文檔 ID
            doc_id = hashlib.sha256(
                f"{engine_type}:{database}:{table}:{idx}".encode()
            ).hexdigest()
            
            documents.append((doc_id, doc))
        
        # 發送到 Elasticsearch
        self.index_to_elasticsearch(
            f"db-{engine_type}-{database}-{table}",
            documents
        )
        
        return len(documents)
    
    def index_to_elasticsearch(self, index_name, documents):
        """批量索引到 Elasticsearch"""
        # ... (使用原本的 bulk index 邏輯)
        pass

def main():
    connector = DatabaseConnector()
    
    # 讀取同步配置
    with open('/config/mapping.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    while True:
        for source in config['sources']:
            engine_type = source['type']
            databases = source.get('databases', connector.get_databases(engine_type))
            
            for db in databases:
                tables = source.get('tables', connector.get_tables(engine_type, db))
                
                for table in tables:
                    try:
                        count = connector.sync_table(engine_type, db, table)
                        print(f"✓ 同步 {count} 筆資料")
                    except Exception as e:
                        print(f"✗ 同步失敗 {db}.{table}: {e}")
        
        print(f"💤 等待 {os.getenv('SYNC_INTERVAL', 60)} 秒...")
        time.sleep(int(os.getenv('SYNC_INTERVAL', 60)))

if __name__ == "__main__":
    main()
