#!/usr/bin/env python3
"""
MySQL 連接器
處理所有 MySQL 資料庫操作
"""

import pymysql
import pandas as pd
from sqlalchemy import create_engine, text
from contextlib import contextmanager
from typing import Dict, List, Any, Optional
import time

class MySQLConnector:
    """MySQL 資料庫連接器"""
    
    def __init__(self, config: Dict[str, Any], logger):
        """
        初始化 MySQL 連接器
        
        Args:
            config: 資料庫設定
            logger: 日誌記錄器
        """
        self.config = config
        self.logger = logger
        self.engine = None
        self.connection_string = None
        
        self._init_connection()
    
    def _init_connection(self):
        """初始化資料庫連接"""
        try:
            # 建立連接字串
            self.connection_string = (
                f"mysql+pymysql://{self.config['user']}:"
                f"{self.config['password']}@"
                f"{self.config['host']}:{self.config['port']}/"
                f"{self.config['database']}?charset={self.config['charset']}"
            )
            
            # 建立 SQLAlchemy 引擎
            pool_config = self.config.get('connection_pool', {})
            self.engine = create_engine(
                self.connection_string,
                pool_size=pool_config.get('size', 5),
                max_overflow=pool_config.get('max_overflow', 10),
                pool_timeout=pool_config.get('timeout', 30),
                pool_recycle=3600,  # 每小時回收連接
                echo=False  # 不輸出 SQL 語句
            )
            
            # 測試連接
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                self.logger.info(f"✓ MySQL 連接成功：{self.config['host']}:{self.config['port']}/{self.config['database']}")
                
        except Exception as e:
            self.logger.error(f"MySQL 連接失敗：{str(e)}")
            raise
    
    @contextmanager
    def get_connection(self):
        """取得資料庫連接的 context manager"""
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()
    
    def test_connection(self) -> bool:
        """測試資料庫連接"""
        try:
            with self.get_connection() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            self.logger.error(f"連接測試失敗：{str(e)}")
            return False
    
    def query_to_dataframe(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        執行查詢並返回 DataFrame
        
        Args:
            query: SQL 查詢語句
            params: 查詢參數
            
        Returns:
            查詢結果的 DataFrame
        """
        try:
            start_time = time.time()
            
            # 使用 pandas 讀取
            if params:
                df = pd.read_sql_query(query, self.engine, params=params)
            else:
                df = pd.read_sql_query(query, self.engine)
            
            elapsed_time = time.time() - start_time
            self.logger.debug(f"查詢執行完成，返回 {len(df)} 筆資料，耗時 {elapsed_time:.2f} 秒")
            
            return df
            
        except Exception as e:
            self.logger.error(f"查詢執行失敗：{str(e)}")
            self.logger.error(f"查詢語句：{query[:200]}...")
            raise
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> int:
        """
        執行非查詢語句（INSERT, UPDATE, DELETE）
        
        Args:
            query: SQL 語句
            params: 查詢參數
            
        Returns:
            影響的行數
        """
        try:
            with self.get_connection() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                conn.commit()
                
                affected_rows = result.rowcount
                self.logger.debug(f"語句執行完成，影響 {affected_rows} 行")
                return affected_rows
                
        except Exception as e:
            self.logger.error(f"語句執行失敗：{str(e)}")
            raise
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """
        取得表的所有欄位名稱
        
        Args:
            table_name: 表名
            
        Returns:
            欄位名稱列表
        """
        query = """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :database
            AND TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION
        """
        
        try:
            with self.get_connection() as conn:
                result = conn.execute(
                    text(query),
                    {'database': self.config['database'], 'table_name': table_name}
                )
                columns = [row[0] for row in result]
                self.logger.debug(f"表 {table_name} 有 {len(columns)} 個欄位")
                return columns
                
        except Exception as e:
            self.logger.error(f"取得表欄位失敗：{str(e)}")
            raise
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        取得表的詳細資訊
        
        Args:
            table_name: 表名
            
        Returns:
            表資訊字典
        """
        # 取得表結構
        schema_query = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                COLUMN_KEY,
                EXTRA,
                COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :database
            AND TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION
        """
        
        # 取得表統計
        stats_query = f"""
            SELECT COUNT(*) as row_count
            FROM {table_name}
        """
        
        try:
            # 表結構
            with self.get_connection() as conn:
                schema_result = conn.execute(
                    text(schema_query),
                    {'database': self.config['database'], 'table_name': table_name}
                )
                
                columns = []
                primary_keys = []
                
                for row in schema_result:
                    col_info = {
                        'name': row[0],
                        'type': row[1],
                        'max_length': row[2],
                        'nullable': row[3] == 'YES',
                        'default': row[4],
                        'key': row[5],
                        'extra': row[6],
                        'comment': row[7]
                    }
                    columns.append(col_info)
                    
                    if row[5] == 'PRI':
                        primary_keys.append(row[0])
                
                # 表統計
                stats_result = conn.execute(text(stats_query))
                row_count = stats_result.scalar()
                
                table_info = {
                    'name': table_name,
                    'database': self.config['database'],
                    'columns': columns,
                    'primary_keys': primary_keys,
                    'row_count': row_count
                }
                
                self.logger.info(f"表 {table_name}: {len(columns)} 欄位, {row_count} 筆資料")
                return table_info
                
        except Exception as e:
            self.logger.error(f"取得表資訊失敗：{str(e)}")
            raise
    
    def get_all_tables(self) -> List[str]:
        """
        取得資料庫中所有表名
        
        Returns:
            表名列表
        """
        query = """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = :database
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """
        
        try:
            with self.get_connection() as conn:
                result = conn.execute(
                    text(query),
                    {'database': self.config['database']}
                )
                tables = [row[0] for row in result]
                self.logger.info(f"資料庫 {self.config['database']} 共有 {len(tables)} 個表")
                return tables
                
        except Exception as e:
            self.logger.error(f"取得表清單失敗：{str(e)}")
            raise
    
    def check_table_exists(self, table_name: str) -> bool:
        """
        檢查表是否存在
        
        Args:
            table_name: 表名
            
        Returns:
            表是否存在
        """
        query = """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = :database
            AND TABLE_NAME = :table_name
        """
        
        try:
            with self.get_connection() as conn:
                result = conn.execute(
                    text(query),
                    {'database': self.config['database'], 'table_name': table_name}
                )
                exists = result.scalar() > 0
                return exists
                
        except Exception as e:
            self.logger.error(f"檢查表存在失敗：{str(e)}")
            raise
    
    def get_table_indexes(self, table_name: str) -> List[Dict]:
        """
        取得表的索引資訊
        
        Args:
            table_name: 表名
            
        Returns:
            索引資訊列表
        """
        query = """
            SELECT 
                INDEX_NAME,
                NON_UNIQUE,
                COLUMN_NAME,
                SEQ_IN_INDEX,
                INDEX_TYPE
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = :database
            AND TABLE_NAME = :table_name
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """
        
        try:
            with self.get_connection() as conn:
                result = conn.execute(
                    text(query),
                    {'database': self.config['database'], 'table_name': table_name}
                )
                
                indexes = {}
                for row in result:
                    index_name = row[0]
                    if index_name not in indexes:
                        indexes[index_name] = {
                            'name': index_name,
                            'unique': not row[1],
                            'type': row[4],
                            'columns': []
                        }
                    indexes[index_name]['columns'].append(row[2])
                
                return list(indexes.values())
                
        except Exception as e:
            self.logger.error(f"取得索引資訊失敗：{str(e)}")
            raise
    
    def close(self):
        """關閉資料庫連接"""
        if self.engine:
            self.engine.dispose()
            self.logger.info("MySQL 連接已關閉")
