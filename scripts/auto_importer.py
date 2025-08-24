#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自動化檔案導入器 - 監控並處理 .sql 和 .db 檔案
支援關鍵字跨表檢索功能
"""

import os
import sys
import json
import time
import sqlite3
import logging
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

import requests
import pandas as pd
import opencc
from requests.auth import HTTPBasicAuth

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/auto_importer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseImporter:
    def __init__(self, 
                 es_url="http://elasticsearch:9200", 
                 username="elastic", 
                 password="admin@12345",
                 watch_dir="/data/import"):
        
        self.es_url = es_url
        self.auth = HTTPBasicAuth(username, password)
        self.watch_dir = Path(watch_dir)
        self.processed_files = self._load_processed_files()
        
        # OpenCC 簡繁轉換
        self.cc_s2t = opencc.OpenCC('s2tw')  # 簡轉繁
        self.cc_t2s = opencc.OpenCC('tw2s')  # 繁轉簡
        
        # 確保目錄存在
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        
    def _load_processed_files(self) -> Dict[str, str]:
        """載入已處理檔案的記錄"""
        record_file = Path('/tmp/processed_files.json')
        if record_file.exists():
            try:
                with open(record_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return {}
    
    def _save_processed_files(self):
        """保存已處理檔案的記錄"""
        record_file = Path('/tmp/processed_files.json')
        try:
            with open(record_file, 'w', encoding='utf-8') as f:
                json.dump(self.processed_files, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存處理記錄失敗: {e}")
    
    def _get_file_hash(self, file_path: Path) -> str:
        """計算檔案雜湊值，用於檢測檔案變化"""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
        except Exception:
            return str(file_path.stat().st_mtime)
        return hash_md5.hexdigest()
    
    def wait_for_elasticsearch(self, max_retries=30):
        """等待 Elasticsearch 服務就緒"""
        for i in range(max_retries):
            try:
                response = requests.get(f"{self.es_url}/_cluster/health", 
                                      auth=self.auth, timeout=5)
                if response.status_code == 200:
                    logger.info("Elasticsearch 服務已就緒")
                    return True
            except Exception:
                logger.info(f"等待 Elasticsearch 啟動... ({i+1}/{max_retries})")
                time.sleep(10)
        
        logger.error("Elasticsearch 服務啟動超時")
        return False
    
    def create_index_template(self, index_name: str) -> bool:
        """創建支援繁體中文檢索的索引模板"""
        template = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "traditional_chinese": {
                            "type": "ik_max_word"
                        },
                        "search_analyzer": {
                            "type": "ik_smart"
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    # 關鍵檢索欄位
                    "keyword_id": {
                        "type": "keyword"
                    },
                    "table_name": {
                        "type": "keyword"
                    },
                    "content": {
                        "type": "text",
                        "analyzer": "traditional_chinese",
                        "search_analyzer": "search_analyzer",
                        "fields": {
                            "raw": {"type": "keyword"},
                            "traditional": {
                                "type": "text",
                                "analyzer": "traditional_chinese"
                            },
                            "simplified": {
                                "type": "text",
                                "analyzer": "traditional_chinese"
                            }
                        }
                    },
                    "metadata": {
                        "type": "object",
                        "properties": {
                            "source_file": {"type": "keyword"},
                            "table_name": {"type": "keyword"},
                            "column_name": {"type": "keyword"},
                            "row_id": {"type": "keyword"},
                            "data_type": {"type": "keyword"}
                        }
                    },
                    "created_at": {
                        "type": "date"
                    }
                }
            }
        }
        
        try:
            response = requests.put(
                f"{self.es_url}/{index_name}",
                json=template,
                auth=self.auth,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            logger.info(f"索引 {index_name} 創建成功")
            return True
        except Exception as e:
            logger.error(f"創建索引失敗: {e}")
            return False
    
    def process_sql_file(self, sql_file: Path) -> bool:
        """處理 SQL 檔案"""
        try:
            logger.info(f"處理 SQL 檔案: {sql_file}")
            
            with open(sql_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # 創建索引
            index_name = f"sql-{sql_file.stem.lower()}"
            self.create_index_template(index_name)
            
            # 提取 SQL 中的關鍵資訊
            doc = {
                "content": content,
                "content.traditional": self.cc_s2t.convert(content),
                "content.simplified": self.cc_t2s.convert(content),
                "metadata": {
                    "source_file": sql_file.name,
                    "table_name": "sql_content",
                    "data_type": "sql_script"
                },
                "created_at": datetime.now().isoformat()
            }
            
            # 導入資料
            response = requests.post(
                f"{self.es_url}/{index_name}/_doc",
                json=doc,
                auth=self.auth,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            
            logger.info(f"SQL 檔案 {sql_file.name} 處理完成")
            return True
            
        except Exception as e:
            logger.error(f"處理 SQL 檔案 {sql_file} 失敗: {e}")
            return False
    
    def process_database_file(self, db_file: Path) -> bool:
        """處理資料庫檔案 - 支援關鍵字跨表檢索"""
        try:
            logger.info(f"處理資料庫檔案: {db_file}")
            
            # 連接資料庫
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
            # 獲取所有表名
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            index_name = f"db-{db_file.stem.lower()}"
            self.create_index_template(index_name)
            
            docs = []
            
            for table in tables:
                try:
                    # 獲取表結構
                    cursor.execute(f"PRAGMA table_info({table})")
                    columns = cursor.fetchall()
                    
                    # 識別文字欄位
                    text_columns = []
                    id_columns = []
                    
                    for col in columns:
                        col_name = col[1].lower()
                        col_type = col[2].lower()
                        
                        # 識別 ID 欄位
                        if 'id' in col_name or col[5] == 1:  # primary key
                            id_columns.append(col[1])
                        
                        # 識別文字欄位
                        if any(t in col_type for t in ['text', 'varchar', 'char']):
                            text_columns.append(col[1])
                    
                    # 讀取資料
                    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                    
                    for _, row in df.iterrows():
                        # 為每一行創建文檔
                        doc = {
                            "table_name": table,
                            "metadata": {
                                "source_file": db_file.name,
                                "table_name": table,
                                "data_type": "database_record"
                            },
                            "created_at": datetime.now().isoformat()
                        }
                        
                        # 處理 ID 欄位
                        primary_ids = []
                        for id_col in id_columns:
                            if id_col in row and pd.notna(row[id_col]):
                                primary_ids.append(str(row[id_col]))
                        doc["keyword_id"] = "|".join(primary_ids)
                        
                        # 處理文字內容
                        all_text_content = []
                        for col_name, value in row.items():
                            if pd.notna(value):
                                text_value = str(value)
                                
                                # 如果是文字欄位，加入主要內容
                                if col_name in text_columns:
                                    all_text_content.append(text_value)
                                
                                # 為每個欄位創建子文檔
                                doc[f"field_{col_name}"] = text_value
                                doc[f"field_{col_name}.traditional"] = self.cc_s2t.convert(text_value)
                                doc[f"field_{col_name}.simplified"] = self.cc_t2s.convert(text_value)
                        
                        # 合併所有文字內容
                        combined_content = " ".join(all_text_content)
                        if combined_content.strip():
                            doc["content"] = combined_content
                            doc["content.traditional"] = self.cc_s2t.convert(combined_content)
                            doc["content.simplified"] = self.cc_t2s.convert(combined_content)
                            
                            docs.append(doc)
                
                except Exception as e:
                    logger.warning(f"處理表 {table} 時發生錯誤: {e}")
                    continue
            
            conn.close()
            
            # 批量導入
            if docs:
                self.bulk_index(docs, index_name)
                logger.info(f"資料庫檔案 {db_file.name} 處理完成，共處理 {len(docs)} 筆記錄")
            
            return True
            
        except Exception as e:
            logger.error(f"處理資料庫檔案 {db_file} 失敗: {e}")
            return False
    
    def bulk_index(self, docs: List[Dict], index_name: str):
        """批量導入文檔"""
        bulk_data = []
        for doc in docs:
            bulk_data.append({"index": {"_index": index_name}})
            bulk_data.append(doc)
        
        bulk_body = '\n'.join(json.dumps(item, ensure_ascii=False) for item in bulk_data) + '\n'
        
        try:
            response = requests.post(
                f"{self.es_url}/_bulk",
                data=bulk_body,
                auth=self.auth,
                headers={'Content-Type': 'application/x-ndjson'}
            )
            response.raise_for_status()
            
            result = response.json()
            if result.get('errors'):
                logger.warning("批量導入過程中有部分錯誤")
                
        except Exception as e:
            logger.error(f"批量導入失敗: {e}")
    
    def search_across_tables(self, keyword: str, limit: int = 10) -> Dict:
        """跨表搜尋關鍵字"""
        search_body = {
            "size": limit,
            "query": {
                "multi_match": {
                    "query": keyword,
                    "fields": [
                        "content^3",
                        "content.traditional^3", 
                        "content.simplified^3",
                        "field_*^2",
                        "keyword_id^5"
                    ],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            },
            "highlight": {
                "fields": {
                    "content": {"fragment_size": 150},
                    "field_*": {"fragment_size": 100}
                }
            },
            "aggs": {
                "by_table": {
                    "terms": {
                        "field": "table_name",
                        "size": 20
                    }
                },
                "by_source": {
                    "terms": {
                        "field": "metadata.source_file",
                        "size": 10
                    }
                }
            }
        }
        
        try:
            response = requests.post(
                f"{self.es_url}/*/_search",  # 搜尋所有索引
                json=search_body,
                auth=self.auth,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"搜尋失敗: {e}")
            return {}
    
    def watch_and_process(self):
        """監控目錄並自動處理新檔案"""
        logger.info(f"開始監控目錄: {self.watch_dir}")
        
        while True:
            try:
                # 檢查 .sql 檔案
                for sql_file in self.watch_dir.glob("*.sql"):
                    file_hash = self._get_file_hash(sql_file)
                    if str(sql_file) not in self.processed_files or self.processed_files[str(sql_file)] != file_hash:
                        if self.process_sql_file(sql_file):
                            self.processed_files[str(sql_file)] = file_hash
                            self._save_processed_files()
                
                # 檢查 .db 檔案
                for db_file in self.watch_dir.glob("*.db"):
                    file_hash = self._get_file_hash(db_file)
                    if str(db_file) not in self.processed_files or self.processed_files[str(db_file)] != file_hash:
                        if self.process_database_file(db_file):
                            self.processed_files[str(db_file)] = file_hash
                            self._save_processed_files()
                
                # 檢查 SQLite 檔案
                for sqlite_file in self.watch_dir.glob("*.sqlite"):
                    file_hash = self._get_file_hash(sqlite_file)
                    if str(sqlite_file) not in self.processed_files or self.processed_files[str(sqlite_file)] != file_hash:
                        if self.process_database_file(sqlite_file):
                            self.processed_files[str(sqlite_file)] = file_hash
                            self._save_processed_files()
                
                # 等待一段時間再檢查
                time.sleep(30)
                
            except KeyboardInterrupt:
                logger.info("收到停止信號，正在退出...")
                break
            except Exception as e:
                logger.error(f"監控過程中發生錯誤: {e}")
                time.sleep(60)  # 發生錯誤時等待更久

def main():
    importer = DatabaseImporter()
    
    # 等待 Elasticsearch 啟動
    if not importer.wait_for_elasticsearch():
        sys.exit(1)
    
    # 開始監控和處理
    importer.watch_and_process()

if __name__ == "__main__":
    main()