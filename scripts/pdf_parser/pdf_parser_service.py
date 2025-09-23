#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件結構化解析服務 - 改進版
增強解析準確度，正確提取文件編號、部門、產品等資訊
"""

import os
import sys
import json
import re
import time
import logging
import hashlib
import pymysql
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import signal

# 環境變數配置
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "fuhsin_erp_demo")

FILE_SERVICE_BASE_URL = os.getenv("FILE_SERVICE_BASE_URL", "http://localhost:8088")
PDF_STORAGE_PATH = os.getenv("PDF_STORAGE_PATH", "/mnt/pdf/files")

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

# 狀態檔案
STATE_FILE = Path("/state/parser_state.json")
STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ImprovedContentParser:
    """改進的內容解析器"""
    
    # 部門對照表
    DEPARTMENT_MAPPING = {
        '品保': '品保課',
        '品質': '品保課',
        'QC': '品保課',
        'QA': '品保課',
        '製造': '製造課',
        '生產': '製造課',
        '工程': '工程部',
        '研發': '研發部',
        'RD': '研發部',
        'R&D': '研發部',
        '業務': '業務部',
        '採購': '採購部',
        '倉管': '倉管課',
        '倉庫': '倉管課',
        'IT': 'IT部',
        '資訊': 'IT部'
    }
    
    @classmethod
    def extract_doc_number(cls, content: str, file_name: str = '') -> str:
        """改進的文件編號提取"""
        # 優先順序：特定格式編號 > 一般編號 > 使用檔名（去除副檔名）
        
        # 1. 特定格式編號
        specific_patterns = [
            (r'CPR-K-Q-\d{2}-[A-Z]-B\d{3}', 'CPR'),      # 客訴單號
            (r'EC-K-\d{2}-[A-Z]-\d{3}(?:-\d)?', 'ECN'),  # 工程變更單號
            (r'ECN[-\s]?(\d{4,6})', lambda m: f'ECN-{m.group(1)}'),  # ECN編號
            (r'單號[：:\s]*([A-Z0-9\-]+)', None),         # 一般單號
            (r'編號[：:\s]*([A-Z0-9\-]+)', None),         # 一般編號
            (r'[GL]\d{2}-[A-Z0-9\-]+', 'PRODUCT'),       # 產品編號（不應作為文件編號）
        ]
        
        for pattern, prefix in specific_patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                # 跳過產品編號
                if prefix == 'PRODUCT':
                    continue
                    
                if callable(prefix):
                    return prefix(match)
                elif prefix:
                    return match.group(0)
                else:
                    return match.group(1) if match.groups() else match.group(0)
        
        # 2. 如果沒找到，從檔名提取（去除副檔名和路徑）
        if file_name:
            base_name = Path(file_name).stem  # 去除副檔名
            # 清理檔名中的日期等資訊
            clean_name = re.sub(r'[-_]\d{8}', '', base_name)  # 移除日期
            clean_name = re.sub(r'[-_]v\d+', '', clean_name)   # 移除版本號
            if clean_name and len(clean_name) > 3:
                return clean_name
        
        return ''
    
    @classmethod
    def extract_department(cls, content: str) -> str:
        """改進的部門提取"""
        # 先嘗試直接匹配
        dept_patterns = [
            r'部門[：:\s]*([^，,\n]+)',
            r'單位[：:\s]*([^，,\n]+)',
            r'申請部門[：:\s]*([^，,\n]+)',
            r'責任部門[：:\s]*([^，,\n]+)'
        ]
        
        for pattern in dept_patterns:
            match = re.search(pattern, content)
            if match:
                dept_text = match.group(1).strip()
                # 標準化部門名稱
                for key, value in cls.DEPARTMENT_MAPPING.items():
                    if key in dept_text:
                        return value
                # 如果是完整部門名稱，直接返回
                if '部' in dept_text or '課' in dept_text:
                    return dept_text
        
        # 從內容中尋找部門關鍵字
        for key, value in cls.DEPARTMENT_MAPPING.items():
            if key in content:
                return value
        
        return ''
    
    @classmethod
    def extract_applicant(cls, content: str) -> str:
        """改進的申請人提取"""
        patterns = [
            r'申請人[：:\s]*([^，,\n\s]+)',
            r'負責人[：:\s]*([^，,\n\s]+)',
            r'提交人[：:\s]*([^，,\n\s]+)',
            r'填表人[：:\s]*([^，,\n\s]+)',
            r'承辦人[：:\s]*([^，,\n\s]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, content)
            if match:
                name = match.group(1).strip()
                # 過濾掉明顯不是人名的內容
                if len(name) <= 10 and not any(char in name for char in '[](){}'):
                    return name
        
        return ''
    
    @classmethod
    def extract_product_info(cls, content: str) -> Tuple[List[str], List[str], str]:
        """改進的產品資訊提取"""
        product_codes = []
        product_names = []
        category = ''
        
        # 產品編號模式（更精確）
        code_patterns = [
            r'[GL]\d{2}-[A-Z0-9]+-[A-Z0-9]+',  # G12-ABC-123 格式
            r'[GL]\d{2}-[A-Z0-9]+',            # G12-ABC 格式
            r'L\d{6}[A-Z]?\d?',                # L123456 格式
            r'OB\d-[A-Z0-9\-]+',               # OB1-XXX 格式
        ]
        
        for pattern in code_patterns:
            matches = re.findall(pattern, content)
            product_codes.extend(matches)
        
        # 產品名稱提取（更精確）
        name_patterns = [
            r'產品名稱[：:\s]*([^，,。\n]+)',
            r'品名[：:\s]*([^，,。\n]+)',
            r'產品型號[：:\s]*([^，,。\n]+)',
            r'料號[：:\s]*([^，,。\n]+)'
        ]
        
        for pattern in name_patterns:
            matches = re.finditer(pattern, content)
            for match in matches:
                name = match.group(1).strip()
                # 過濾掉編號和無意義的內容
                if name and not re.match(r'^[A-Z0-9\-]+$', name) and len(name) > 2:
                    product_names.append(name)
        
        # 產品類別判斷（基於產品編號）
        unique_codes = list(set(product_codes))[:20]  # 限制數量
        
        if unique_codes:
            if any(code.startswith('G') for code in unique_codes):
                category = 'G鎖'
            elif any(code.startswith('L') for code in unique_codes):
                category = 'L鎖'
            elif any(code.startswith('T') for code in unique_codes):
                category = 'T鎖'
            elif any(code.startswith('OB') for code in unique_codes):
                category = 'OB系列'
        
        return unique_codes[:10], product_names[:5], category
    
    @classmethod
    def extract_responsible_units(cls, content: str) -> List[str]:
        """提取責任單位"""
        units = []
        unit_keywords = [
            '品保課', '製造課', '工程部', '研發部', 
            '業務部', '採購部', '倉管課', 'IT部'
        ]
        
        for unit in unit_keywords:
            if unit in content:
                units.append(unit)
        
        # 也檢查英文縮寫
        if 'QC' in content or 'QA' in content:
            if '品保課' not in units:
                units.append('品保課')
        if 'RD' in content or 'R&D' in content:
            if '研發部' not in units:
                units.append('研發部')
        
        return units[:5]
    
    @classmethod
    def extract_date(cls, content: str) -> Optional[str]:
        """提取日期（改進版）"""
        patterns = [
            (r'(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})', '%Y-%m-%d'),
            (r'(\d{4})年(\d{1,2})月(\d{1,2})日', '%Y-%m-%d'),
            (r'(\d{2})[/\-](\d{1,2})[/\-](\d{1,2})', '20%y-%m-%d'),
        ]
        
        for pattern, format_str in patterns:
            match = re.search(pattern, content)
            if match:
                try:
                    groups = match.groups()
                    if len(groups[0]) == 2:  # 兩位數年份
                        year = '20' + groups[0]
                    else:
                        year = groups[0]
                    month = groups[1].zfill(2)
                    day = groups[2].zfill(2)
                    
                    # 驗證日期合理性
                    date_str = f"{year}-{month}-{day}"
                    datetime.strptime(date_str, '%Y-%m-%d')
                    return date_str
                except:
                    continue
        
        return None
    
    @classmethod
    def detect_doc_type(cls, content: str) -> str:
        """改進的文件類型偵測"""
        type_indicators = {
            'ECN': ['工程變更', '設計變更', 'ECN', 'EC-K-'],
            'Complaint': ['客訴', '客戶投訴', 'CPR-K-Q-', '客戶抱怨'],
            'FMEA': ['FMEA', 'DFMEA', 'PFMEA', '失效模式', 'RPN'],
            'TestReport': ['測試報告', '檢驗報告', 'TEST REPORT'],
            'Specification': ['規格書', '產品規格', 'SPEC'],
            'WorkOrder': ['工單', '製令', '生產單'],
            'PurchaseOrder': ['採購單', '請購單', 'PO'],
        }
        
        # 計算每種類型的匹配分數
        scores = {}
        for doc_type, keywords in type_indicators.items():
            score = sum(1 for kw in keywords if kw in content.upper())
            if score > 0:
                scores[doc_type] = score
        
        # 返回最高分的類型
        if scores:
            return max(scores, key=scores.get)
        
        return 'General'
    
    @classmethod
    def generate_summary(cls, content: str, max_length: int = 500) -> str:
        """生成更好的摘要"""
        # 清理內容
        lines = content.split('\n')
        meaningful_lines = []
        
        for line in lines:
            line = line.strip()
            # 跳過無意義的行
            if len(line) < 5:
                continue
            if line.startswith('=') or line.startswith('-'):
                continue
            if re.match(r'^[\d\s\.\-/]+$', line):  # 純數字日期
                continue
            
            meaningful_lines.append(line)
            
            # 累積到足夠長度
            if sum(len(l) for l in meaningful_lines) > max_length:
                break
        
        # 組合摘要
        summary = ' '.join(meaningful_lines[:10])  # 最多10行
        
        if len(summary) > max_length:
            summary = summary[:max_length-3] + '...'
        
        return summary

class DatabaseManager:
    """資料庫管理器"""
    
    def __init__(self):
        self.connection = None
        self.init_database()
        self.state = self.load_state()
    
    def load_state(self) -> Dict:
        """載入處理狀態"""
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {'last_check': '2000-01-01 00:00:00', 'processed': []}
    
    def save_state(self):
        """儲存處理狀態"""
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump(self.state, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"儲存狀態失敗: {e}")
    
    def get_connection(self):
        """取得資料庫連線"""
        if not self.connection or not self.connection.open:
            self.connection = pymysql.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
        return self.connection
    
    def init_database(self):
        """初始化資料庫表"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # 建立 structured_documents 表
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS structured_documents (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    original_doc_id VARCHAR(50) NOT NULL UNIQUE,
                    doc_type VARCHAR(50),
                    doc_number VARCHAR(100),
                    doc_date DATE,
                    
                    file_name VARCHAR(255),
                    file_url TEXT,
                    file_path TEXT,
                    file_size BIGINT,
                    file_hash VARCHAR(64),
                    
                    product_category VARCHAR(50),
                    product_codes JSON,
                    product_names JSON,
                    
                    applicant VARCHAR(100),
                    department VARCHAR(100),
                    responsible_units JSON,
                    
                    summary TEXT,
                    keywords JSON,
                    status VARCHAR(50),
                    priority VARCHAR(20),
                    
                    parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    
                    INDEX idx_doc_number (doc_number),
                    INDEX idx_doc_type (doc_type),
                    INDEX idx_last_modified (last_modified)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                conn.commit()
                logger.info("資料庫表初始化完成")
        except Exception as e:
            logger.error(f"初始化資料庫失敗: {e}")
    
    def get_unprocessed_documents(self, limit: int = 10) -> List[Dict]:
        """取得未處理或需要更新的文件"""
        sql = """
        SELECT td.* 
        FROM technical_documents td
        LEFT JOIN structured_documents sd ON td.doc_id = sd.original_doc_id
        WHERE sd.id IS NULL 
           OR td.last_modified > sd.last_modified
        ORDER BY td.created_at DESC
        LIMIT %s
        """
        
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(sql, (limit,))
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"查詢未處理文件失敗: {e}")
            return []
    
    def save_structured_document(self, doc_data: Dict) -> bool:
        """儲存結構化文件"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # 處理 JSON 欄位
                json_fields = ['product_codes', 'product_names', 'responsible_units', 'keywords']
                for field in json_fields:
                    if field in doc_data and doc_data[field] is not None:
                        doc_data[field] = json.dumps(doc_data[field], ensure_ascii=False)
                
                # 建立 SQL
                fields = list(doc_data.keys())
                placeholders = ['%s'] * len(fields)
                update_fields = [f"{f}=VALUES({f})" for f in fields if f != 'original_doc_id']
                
                sql = f"""
                INSERT INTO structured_documents ({', '.join(fields)})
                VALUES ({', '.join(placeholders)})
                ON DUPLICATE KEY UPDATE {', '.join(update_fields)}
                """
                
                cursor.execute(sql, list(doc_data.values()))
                conn.commit()
                
                # 更新處理狀態
                if 'original_doc_id' in doc_data:
                    if doc_data['original_doc_id'] not in self.state['processed']:
                        self.state['processed'].append(doc_data['original_doc_id'])
                    self.state['last_check'] = datetime.now().isoformat()
                    self.save_state()
                
                return True
                
        except Exception as e:
            logger.error(f"儲存結構化文件失敗: {e}")
            if conn:
                conn.rollback()
            return False

class DocumentProcessor:
    """文件處理器"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.parser = ImprovedContentParser()
        self.running = False
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, signum, frame):
        logger.info(f"收到終止訊號 {signum}")
        self.running = False
    
    def process_document(self, doc: Dict) -> bool:
        """處理單一文件"""
        try:
            doc_id = doc.get('doc_id')
            content = doc.get('content', '')
            file_name = doc.get('file_name', '')
            
            if not content:
                logger.warning(f"文件 {doc_id} 沒有內容")
                return False
            
            logger.info(f"處理文件 {doc_id}, 檔案: {file_name}")
            
            # 建立結構化資料
            structured = {
                'original_doc_id': doc_id,
                'parsed_at': datetime.now().isoformat()
            }
            
            # 文件類型
            doc_type = self.parser.detect_doc_type(content)
            structured['doc_type'] = doc_type
            
            # 文件編號（傳入檔名協助判斷）
            doc_number = self.parser.extract_doc_number(content, file_name)
            structured['doc_number'] = doc_number if doc_number else f"DOC-{doc_id}"
            
            # 日期
            structured['doc_date'] = self.parser.extract_date(content)
            
            # 產品資訊
            product_codes, product_names, category = self.parser.extract_product_info(content)
            structured['product_codes'] = product_codes
            structured['product_names'] = product_names
            structured['product_category'] = category
            
            # 人員與部門
            structured['applicant'] = self.parser.extract_applicant(content)
            structured['department'] = self.parser.extract_department(content)
            structured['responsible_units'] = self.parser.extract_responsible_units(content)
            
            # 摘要
            structured['summary'] = self.parser.generate_summary(content)
            
            # 關鍵字
            keywords = []
            if doc_type:
                keywords.append(doc_type)
            if category:
                keywords.append(category)
            keywords.extend(product_codes[:3])  # 加入前3個產品編號作為關鍵字
            structured['keywords'] = list(set(keywords))[:10]
            
            # 檔案資訊
            if file_name:
                structured['file_name'] = file_name
                structured['file_url'] = f"{FILE_SERVICE_BASE_URL}/{doc_id}/{file_name}"
                
                file_path = f"{PDF_STORAGE_PATH}/{doc_id}/{file_name}"
                structured['file_path'] = file_path
                
                try:
                    if os.path.exists(file_path):
                        structured['file_size'] = os.path.getsize(file_path)
                        with open(file_path, 'rb') as f:
                            structured['file_hash'] = hashlib.sha256(f.read()).hexdigest()
                except:
                    pass
            
            # 優先級
            if any(word in content for word in ['嚴重', '緊急', '立即', '重大']):
                structured['priority'] = 'HIGH'
            elif any(word in content for word in ['中等', '一般']):
                structured['priority'] = 'NORMAL'
            else:
                structured['priority'] = 'LOW'
            
            structured['status'] = 'PARSED'
            
            # 儲存
            success = self.db.save_structured_document(structured)
            
            if success:
                logger.info(f"文件 {doc_id} 處理成功: 編號={doc_number}, 類型={doc_type}, 部門={structured['department']}")
            
            return success
            
        except Exception as e:
            logger.error(f"處理文件失敗: {e}", exc_info=True)
            return False
    
    def run(self):
        """主執行迴圈"""
        self.running = True
        logger.info("文件解析服務啟動")
        
        while self.running:
            try:
                docs = self.db.get_unprocessed_documents(limit=BATCH_SIZE)
                
                if docs:
                    logger.info(f"發現 {len(docs)} 個待處理文件")
                    for doc in docs:
                        if not self.running:
                            break
                        self.process_document(doc)
                        time.sleep(1)
                
                # 等待
                for _ in range(SCAN_INTERVAL):
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"主迴圈錯誤: {e}")
                time.sleep(10)
        
        logger.info("服務停止")

if __name__ == "__main__":
    try:
        processor = DocumentProcessor()
        processor.run()
    except KeyboardInterrupt:
        logger.info("收到中斷訊號")
    except Exception as e:
        logger.error(f"服務異常: {e}")
        sys.exit(1)
