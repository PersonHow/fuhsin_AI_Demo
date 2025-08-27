#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JDBCAdapter - 直接從資料庫拉取資料的適配器
支援增量同步和全量同步
"""
import os
import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging

# 根據環境安裝對應的驅動
try:
    import pymysql as mysql_driver
    mysql_available = True
except ImportError:
    mysql_available = False

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    postgres_available = True
except ImportError:
    postgres_available = False

try:
    import pyodbc
    sqlserver_available = True
except ImportError:
    sqlserver_available = False

logger = logging.getLogger(__name__)

class JDBCAdapter:
    """JDBC 資料庫適配器"""
    
    def __init__(self, 
                 db_config: Dict[str, str],
                 table_configs: List[Dict[str, Any]],
                 state_file: str = "jdbc_state.json"):
        """
        初始化 JDBC 適配器
        
        Args:
            db_config: 資料庫連接配置
            table_configs: 表格同步配置列表
            state_file: 狀態檔案路徑
        """
        self.db_config = db_config
        self.table_configs = table_configs
        self.state_file = state_file
        self.connection = None
        
        # 載入上次同步狀態
        self.last_sync_state = self._load_state()
    
    def fetch_data(self) -> List[Dict[str, Any]]:
        """從資料庫取得資料"""
        all_data = []
        
        try:
            self._connect()
            
            for table_config in self.table_configs:
                table_data = self._fetch_table_data(table_config)
                all_data.extend(table_data)
                
                # 更新同步狀態
                self._update_sync_state(table_config['table_name'])
            
            # 儲存狀態
            self._save_state()
            
        except Exception as e:
            logger.error(f"資料庫同步失敗: {e}")
            raise
        finally:
            self._disconnect()
        
        return all_data
    
    def get_metadata(self) -> Dict[str, str]:
        """取得資料庫來源的元資料"""
        return {
            'type': 'jdbc',
            'database': self.db_config.get('database', ''),
            'host': self.db_config.get('host', ''),
            'sync_time': datetime.now().isoformat()
        }
    
    def _connect(self):
        """建立資料庫連接"""
        db_type = self.db_config.get('type', 'mysql').lower()
        
        if db_type == 'mysql':
            if not mysql_available:
                raise ImportError("請安裝 pymysql: pip install pymysql")
            
            self.connection = mysql_driver.connect(
                host=self.db_config['host'],
                port=self.db_config.get('port', 3306),
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                charset='utf8mb4'
            )
            
        elif db_type == 'postgresql':
            if not postgres_available:
                raise ImportError("請安裝 psycopg2: pip install psycopg2-binary")
                
            self.connection = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config.get('port', 5432),
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database']
            )
            
        elif db_type == 'sqlserver':
            if not sqlserver_available:
                raise ImportError("請安裝 pyodbc: pip install pyodbc")
            
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.db_config['host']};"
                f"DATABASE={self.db_config['database']};"
                f"UID={self.db_config['user']};"
                f"PWD={self.db_config['password']}"
            )
            self.connection = pyodbc.connect(conn_str)
            
        else:
            raise ValueError(f"不支援的資料庫類型: {db_type}")
        
        logger.info(f"已連接到 {db_type} 資料庫")
    
    def _disconnect(self):
        """關閉資料庫連接"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def _fetch_table_data(self, table_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """從單個表格取得資料"""
        table_name = table_config['table_name']
        sync_mode = table_config.get('sync_mode', 'incremental')  # full, incremental
        timestamp_column = table_config.get('timestamp_column', 'updated_time')
        pk_columns = table_config.get('pk_columns', ['id'])
        
        logger.info(f"同步表格 {table_name} (模式: {sync_mode})")
        
        # 建構查詢
        base_query = f"SELECT * FROM {table_name}"
        
        if sync_mode == 'incremental':
            last_sync = self.last_sync_state.get(table_name)
            if last_sync:
                where_clause = f"WHERE {timestamp_column} >= '{last_sync}'"
                query = f"{base_query} {where_clause}"
            else:
                # 首次同步，取最近30天的資料
                cutoff_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
                query = f"{base_query} WHERE {timestamp_column} >= '{cutoff_date}'"
        else:
            query = base_query
        
        # 執行查詢
        cursor = self.connection.cursor()
        
        try:
            cursor.execute(query)
            
            # 取得欄位名稱
            if hasattr(cursor, 'description'):
                columns = [desc[0] for desc in cursor.description]
            else:
                columns = []
            
            records = []
            for row in cursor.fetchall():
                # 轉換為字典
                if hasattr(row, '_asdict'):  # namedtuple
                    record_dict = row._asdict()
                else:
                    record_dict = dict(zip(columns, row)) if columns else {}
                
                # 建立標準化記錄
                standard_record = self._normalize_record(record_dict, table_config)
                records.append(standard_record)
            
            logger.info(f"從 {table_name} 取得 {len(records)} 筆記錄")
            return records
            
        finally:
            cursor.close()
    
    def _normalize_record(self, record: Dict[str, Any], table_config: Dict[str, Any]) -> Dict[str, Any]:
        """標準化單筆記錄"""
        table_name = table_config['table_name']
        pk_columns = table_config.get('pk_columns', ['id'])
        
        # 建立主鍵
        pk_values = [str(record.get(col, '')) for col in pk_columns]
        pk = hashlib.sha256('::'.join([table_name] + pk_values).encode()).hexdigest()
        
        # 標準化記錄
        normalized = {
            'table_name': table_name,
            'pk': pk,
            'updated_at': datetime.now().isoformat(),
            'source_type': 'jdbc'
        }
        
        # 欄位映射
        field_mapping = self._get_field_mapping()
        
        for original_field, value in record.items():
            if value is not None:
                # 轉換欄位名稱
                standard_field = field_mapping.get(original_field.lower(), original_field.lower())
                
                # 處理日期時間
                if hasattr(value, 'isoformat'):  # datetime object
                    normalized[standard_field] = value.isoformat()
                else:
                    normalized[standard_field] = value
        
        return normalized
    
    def _get_field_mapping(self) -> Dict[str, str]:
        """資料庫欄位名稱標準化映射"""
        return {
            # 供應商相關
            'supplier_name': 'supplier_name',
            'supplier_short': 'supplier_short',
            'supplier_contact': 'supplier_contact',
            'supplier_address': 'supplier_address',
            'supplier_phone': 'supplier_phone',
            'supplier_id': 'supplier_id',
            
            # 客戶相關
            'customer_name': 'customer_name',
            'customer_short': 'customer_short',
            'customer_contact': 'customer_contact',
            'customer_address': 'customer_address',
            'customer_phone': 'customer_phone',
            'customer_id': 'customer_id',
            
            # 員工相關
            'name': 'employee_name',
            'employee_name': 'employee_name',
            'department_name': 'department_name',
            'position': 'position',
            'employee_id': 'employee_id',
            'department_id': 'department_id',
            
            # 訂單相關
            'work_order_id': 'work_order_id',
            'order_id': 'order_id',
            'total_amount': 'total_amount',
            'currency': 'currency',
            'work_order_status': 'status',
            'status': 'status',
            
            # 日期相關
            'planned_start_date': 'planned_start_date',
            'delivery_date': 'delivery_date',
            'created_time': 'created_time',
            'updated_time': 'updated_time',
            'created_at': 'created_time',
            'updated_at': 'updated_time',
            
            # 其他
            'remark': 'remark',
            'remarks': 'remark',
            'address': 'address',
            'phone': 'phone',
            'email': 'email'
        }
    
    def _load_state(self) -> Dict[str, str]:
        """載入同步狀態"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"載入狀態檔失敗: {e}")
        return {}
    
    def _save_state(self):
        """儲存同步狀態"""
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.last_sync_state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"儲存狀態檔失敗: {e}")
    
    def _update_sync_state(self, table_name: str):
        """更新表格同步狀態"""
        self.last_sync_state[table_name] = datetime.now().isoformat()
