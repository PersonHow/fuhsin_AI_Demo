#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FileSQLAdapter - 處理 SQL 檔案的資料適配器
"""
import re
import hashlib
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime

class FileSQLAdapter:
    """SQL 檔案適配器"""
    
    def __init__(self, import_dir: Path, processed_suffix: str = ".done"):
        self.import_dir = Path(import_dir)
        self.processed_suffix = processed_suffix
        self.import_dir.mkdir(parents=True, exist_ok=True)
    
    def fetch_data(self) -> List[Dict[str, Any]]:
        """從 SQL 檔案取得資料"""
        all_data = []
        
        # 找到待處理的 SQL 檔案
        sql_files = [f for f in self.import_dir.glob("*.sql") 
                    if not f.name.endswith(self.processed_suffix)]
        
        for sql_file in sql_files:
            try:
                file_data = self._process_sql_file(sql_file)
                all_data.extend(file_data)
                
                # 標記檔案為已處理
                done_file = sql_file.with_suffix(sql_file.suffix + self.processed_suffix)
                sql_file.rename(done_file)
                
            except Exception as e:
                print(f"處理檔案 {sql_file.name} 失敗: {e}")
        
        return all_data
    
    def get_metadata(self) -> Dict[str, str]:
        """取得檔案來源的元資料"""
        return {
            'type': 'file_sql',
            'source_dir': str(self.import_dir)
        }
    
    def _process_sql_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """處理單個 SQL 檔案"""
        content = self._robust_read_text(file_path)
        statements = self._split_sql_statements(content)
        
        all_records = []
        
        for statement in statements:
            if 'insert into' in statement.lower():
                parsed = self._parse_insert_statement(statement)
                if parsed:
                    table_name = parsed['table_name']
                    columns = parsed['columns'] 
                    records = parsed['records']
                    
                    # 將每一筆記錄轉為標準格式
                    for record_idx, record_values in enumerate(records):
                        record_data = {
                            'table_name': table_name,
                            'source_file': file_path.name,
                            'record_index': record_idx,
                            'updated_at': datetime.now().isoformat()
                        }
                        
                        # 建立主鍵
                        pk_components = [file_path.name, table_name, str(record_idx)]
                        record_data['pk'] = hashlib.sha256(
                            '::'.join(pk_components).encode()
                        ).hexdigest()
                        
                        # 映射欄位值
                        for col_idx, column_name in enumerate(columns):
                            if col_idx < len(record_values):
                                value = record_values[col_idx]
                                clean_col = column_name.strip().lower()
                                
                                # 標準化欄位名稱映射
                                field_mapping = self._get_field_mapping()
                                standard_field = field_mapping.get(clean_col, clean_col)
                                
                                if value is not None:
                                    record_data[standard_field] = value
                        
                        all_records.append(record_data)
        
        return all_records
    
    def _get_field_mapping(self) -> Dict[str, str]:
        """欄位名稱標準化映射"""
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
            
            # 其他
            'remark': 'remark',
            'address': 'address',
            'phone': 'phone',
            'email': 'email'
        }
    
    def _robust_read_text(self, path: Path) -> str:
        """強健的文字檔讀取"""
        b = path.read_bytes()
        encodings = ["utf-8", "utf-8-sig", "cp950", "big5", "utf-16le", "utf-16be"]
        
        for encoding in encodings:
            try:
                return b.decode(encoding).replace("\r", "")
            except:
                continue
        
        return b.decode("utf-8", "ignore").replace("\r", "")
    
    def _split_sql_statements(self, text: str) -> List[str]:
        """分割 SQL 語句"""
        # 移除註解
        no_comment = re.sub(r'/\*.*?\*/', '', text, flags=re.DOTALL)
        no_comment = re.sub(r'--[^\n]*', '', no_comment)
        
        # 按分號分割
        parts = [p.strip() for p in re.split(r';\s*(?:\n|$)', no_comment)]
        return [p for p in parts if p]
    
    def _parse_insert_statement(self, sql_text: str) -> Dict[str, Any]:
        """解析 INSERT INTO 語句"""
        clean_sql = re.sub(r'\s+', ' ', sql_text).strip()
        
        # 解析 INSERT INTO table_name(columns) VALUES
        insert_pattern = r'insert\s+into\s+([^\s(]+)\s*\(([^)]+)\)\s+values\s*(.+)'
        match = re.match(insert_pattern, clean_sql, re.IGNORECASE)
        
        if not match:
            return None
        
        table_name = match.group(1).strip()
        columns_str = match.group(2).strip()
        values_str = match.group(3).strip()
        
        # 解析欄位名
        columns = [col.strip() for col in columns_str.split(',')]
        
        # 解析 VALUES 部分
        records = self._parse_values_section(values_str)
        
        return {
            'table_name': table_name,
            'columns': columns,
            'records': records
        }
    
    def _parse_values_section(self, values_str: str) -> List[List[Any]]:
        """解析 VALUES 部分的多行記錄"""
        records = []
        
        # 找到所有的 (...) 記錄
        pattern = r'\(([^)]+(?:\([^)]*\)[^)]*)*)\)'
        matches = re.findall(pattern, values_str)
        
        for match in matches:
            values = self._parse_single_record(match)
            if values:
                records.append(values)
        
        return records
    
    def _parse_single_record(self, record_str: str) -> List[Any]:
        """解析單個記錄的值"""
        values = []
        current_value = ""
        in_quotes = False
        quote_char = None
        paren_depth = 0
        i = 0
        
        while i < len(record_str):
            char = record_str[i]
            
            if char in ('"', "'") and (i == 0 or record_str[i-1] != '\\'):
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                    quote_char = None
                current_value += char
            elif char == '(' and not in_quotes:
                paren_depth += 1
                current_value += char
            elif char == ')' and not in_quotes:
                paren_depth -= 1
                current_value += char
            elif char == ',' and not in_quotes and paren_depth == 0:
                values.append(self._clean_value(current_value.strip()))
                current_value = ""
            else:
                current_value += char
            
            i += 1
        
        # 添加最後一個值
        if current_value.strip():
            values.append(self._clean_value(current_value.strip()))
        
        return values
    
    def _clean_value(self, value: str) -> Any:
        """清理和轉換值"""
        value = value.strip()
        
        # NULL 值
        if value.lower() == 'null':
            return None
        
        # 去除 N' 前綴
        if value.startswith(("N'", "n'")):
            value = value[2:-1] if value.endswith("'") else value[2:]
            return value
        
        # 普通字符串
        if value.startswith("'") and value.endswith("'"):
            return value[1:-1]
        
        # 數字
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            pass
        
        # 日期/時間戳
        if value.upper().startswith(('DATE', 'TIMESTAMP')):
            date_match = re.search(r"'([^']+)'", value)
            if date_match:
                return date_match.group(1)
        
        return value
