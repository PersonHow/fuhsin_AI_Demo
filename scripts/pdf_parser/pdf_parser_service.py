#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF 解析器服務 - 多表解析版
從 technical_documents 讀取資料，解析到專屬表 + structured_documents
"""

import os
import sys
import time
import json
import re
import pymysql
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

# ========== 配置 ==========
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'mysql')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', '3306'))
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'root')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'fuhsin_erp_demo')

SCAN_INTERVAL = int(os.environ.get('SCAN_INTERVAL', '60'))
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '10'))

PDF_STORAGE_PATH = os.environ.get('PDF_STORAGE_PATH', '/mnt/pdf/done')
FILE_SERVICE_BASE_URL = os.environ.get('FILE_SERVICE_BASE_URL', 'http://pdf-file-server:80')

# ========== 日誌配置 ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========== 資料庫管理器 ==========
class DatabaseManager:
    """資料庫操作管理器"""
    
    def __init__(self):
        self.connection = None
        self.connect()
        self.ensure_tables_exist()

    def connect(self):
        """建立資料庫連線"""
        try:
            self.connection = pymysql.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=False
            )
            logger.info(f"✅ 資料庫連線成功")
        except Exception as e:
            logger.error(f"❌ 資料庫連線失敗: {e}")
            raise

    def get_connection(self):
        """取得資料庫連線"""
        try:
            if not self.connection or not self.connection.ping(reconnect=False):
                self.connect()
        except:
            self.connect()
        return self.connection
    
    def ensure_tables_exist(self):
        """確保必要的表存在，如果不存在則自動創建"""
        create_structured_documents_sql = """
        CREATE TABLE IF NOT EXISTS structured_documents (
            id INT AUTO_INCREMENT PRIMARY KEY,
            original_doc_id VARCHAR(50) NOT NULL UNIQUE COMMENT '關聯 technical_documents.doc_id',
            doc_type VARCHAR(50) COMMENT '文檔類型',
            doc_number VARCHAR(100) COMMENT '文檔編號',
            doc_date DATE COMMENT '文檔日期',
            
            -- 檔案資訊
            file_name VARCHAR(255),
            file_url TEXT,
            file_path TEXT,
            file_size BIGINT,
            file_hash VARCHAR(64),
            
            -- 產品資訊
            product_category VARCHAR(50),
            product_codes JSON,
            product_names JSON,
            related_doc_numbers JSON,
            
            -- 人員與部門
            applicant VARCHAR(100),
            department VARCHAR(100),
            responsible_units JSON,
            
            -- 摘要與關鍵字
            summary TEXT,
            keywords JSON,
            status VARCHAR(50),
            priority VARCHAR(20),
            
            -- 時間戳記
            parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            
            -- 索引
            INDEX idx_doc_number (doc_number),
            INDEX idx_doc_type (doc_type),
            INDEX idx_last_modified (last_modified),
            FOREIGN KEY (original_doc_id) REFERENCES technical_documents(doc_id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        COMMENT='結構化文件摘要表'
        """
        
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(create_structured_documents_sql)
                conn.commit()
                logger.info("✅ 已確保 structured_documents 表存在")
        except Exception as e:
            logger.warning(f"⚠️ 創建 structured_documents 表失敗 (可能已存在): {e}")

    
    def get_unprocessed_documents(self, limit: int = 10) -> List[Dict]:
        """取得未解析的文件"""
        sql = """
        SELECT td.doc_id, td.doc_type, td.file_name, td.content, td.page_count
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
    
    def save_to_ecn_notice(self, doc_id: str, data: Dict) -> bool:
        """儲存到設變通知單表"""
        sql = """
        INSERT INTO ecn_notices (
            doc_id, notice_number, application_number, applicable_scope,
            product_code, product_name, change_description, inventory_handling,
            inventory_doc_number, inventory_product_name, inventory_notes,
            ecn_date, applicant, before_change, after_change
        ) VALUES (
            %(doc_id)s, %(notice_number)s, %(application_number)s, %(applicable_scope)s,
            %(product_code)s, %(product_name)s, %(change_description)s, %(inventory_handling)s,
            %(inventory_doc_number)s, %(inventory_product_name)s, %(inventory_notes)s,
            %(ecn_date)s, %(applicant)s, %(before_change)s, %(after_change)s
        )
        ON DUPLICATE KEY UPDATE
            notice_number = VALUES(notice_number),
            application_number = VALUES(application_number),
            product_name = VALUES(product_name),
            change_description = VALUES(change_description),
            before_change = VALUES(before_change),
            after_change = VALUES(after_change)
        """
        
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                data['doc_id'] = doc_id
                cursor.execute(sql, data)
                conn.commit()
                logger.info(f"✅ 已儲存到 ecn_notices: {data.get('notice_number', 'N/A')}")
                return True
        except Exception as e:
            logger.error(f"儲存 ecn_notices 失敗: {e}")
            return False
    
    def save_to_ecn_application(self, doc_id: str, data: Dict) -> bool:
        """儲存到設變申請單表"""
        sql = """
        INSERT INTO ecn_applications (
            doc_id, application_number, ecn_date, product_name, product_code,
            reason, change_items, change_before, change_after, change_item_description,
            inventory_handling, inventory_product_code, inventory_quantity, inventory_notes,
            execution_plan, meeting_suggestions, review_notes, review_decision, review_meeting_date
        ) VALUES (
            %(doc_id)s, %(application_number)s, %(ecn_date)s, %(product_name)s, %(product_code)s,
            %(reason)s, %(change_items)s, %(change_before)s, %(change_after)s, %(change_item_description)s,
            %(inventory_handling)s, %(inventory_product_code)s, %(inventory_quantity)s, %(inventory_notes)s,
            %(execution_plan)s, %(meeting_suggestions)s, %(review_notes)s, %(review_decision)s, %(review_meeting_date)s
        )
        ON DUPLICATE KEY UPDATE
            application_number = VALUES(application_number),
            product_name = VALUES(product_name),
            reason = VALUES(reason),
            change_items = VALUES(change_items)
        """
        
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                data['doc_id'] = doc_id
                cursor.execute(sql, data)
                conn.commit()
                logger.info(f"✅ 已儲存到 ecn_applications: {data.get('application_number', 'N/A')}")
                return True
        except Exception as e:
            logger.error(f"儲存 ecn_applications 失敗: {e}")
            return False
    
    def save_to_fmea(self, doc_id: str, data: Dict) -> bool:
        """儲存到 FMEA 分析表"""
        sql = """
        INSERT INTO fmea_records (
            doc_id, case_number, case_name, analysis_type, product_type,
            responsible_person, analyst, department,
            analysis_item, failure_mode, failure_effect, failure_cause,
            severity_s, occurrence_o, detection_d, rpn,
            current_control, corrective_action, improvement_result,
            target_completion_date, execution_unit,
            improved_severity, improved_occurrence, improved_detection, improved_rpn,
            is_customer_complaint, department_head, section_head,
            form_date, revision_date, version
        ) VALUES (
            %(doc_id)s, %(case_number)s, %(case_name)s, %(analysis_type)s, %(product_type)s,
            %(responsible_person)s, %(analyst)s, %(department)s,
            %(analysis_item)s, %(failure_mode)s, %(failure_effect)s, %(failure_cause)s,
            %(severity_s)s, %(occurrence_o)s, %(detection_d)s, %(rpn)s,
            %(current_control)s, %(corrective_action)s, %(improvement_result)s,
            %(target_completion_date)s, %(execution_unit)s,
            %(improved_severity)s, %(improved_occurrence)s, %(improved_detection)s, %(improved_rpn)s,
            %(is_customer_complaint)s, %(department_head)s, %(section_head)s,
            %(form_date)s, %(revision_date)s, %(version)s
        )
        ON DUPLICATE KEY UPDATE
            case_number = VALUES(case_number),
            case_name = VALUES(case_name),
            product_type = VALUES(product_type),
            failure_mode = VALUES(failure_mode),
            corrective_action = VALUES(corrective_action)
        """
        
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                data['doc_id'] = doc_id
                cursor.execute(sql, data)
                conn.commit()
                logger.info(f"✅ 已儲存到 fmea_records: {data.get('case_number', 'N/A')}")
                return True
        except Exception as e:
            logger.error(f"儲存 fmea_records 失敗: {e}")
            return False
    
    def save_to_complaint(self, doc_id: str, data: Dict) -> bool:
        """儲存到客訴表"""
        sql = """
        INSERT INTO complaint_records (
            doc_id, complaint_number, complaint_type, source_number, production_notice_number,
            customer_code, customer_name, product_item, product_code, product_name,
            shipping_factory, complaint_description, responsible_sales, complaint_analysis
        ) VALUES (
            %(doc_id)s, %(complaint_number)s, %(complaint_type)s, %(source_number)s, %(production_notice_number)s,
            %(customer_code)s, %(customer_name)s, %(product_item)s, %(product_code)s, %(product_name)s,
            %(shipping_factory)s, %(complaint_description)s, %(responsible_sales)s, %(complaint_analysis)s
        )
        ON DUPLICATE KEY UPDATE
            complaint_number = VALUES(complaint_number),
            customer_name = VALUES(customer_name),
            product_name = VALUES(product_name),
            complaint_description = VALUES(complaint_description)
        """
        
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                data['doc_id'] = doc_id
                cursor.execute(sql, data)
                conn.commit()
                logger.info(f"✅ 已儲存到 complaint_records: {data.get('complaint_number', 'N/A')}")
                return True
        except Exception as e:
            logger.error(f"儲存 complaint_records 失敗: {e}")
            return False
    
    def save_to_structured_documents(self, data: Dict) -> bool:
        """儲存到 structured_documents 表"""
        sql = """
        INSERT INTO structured_documents (
            original_doc_id, doc_type, doc_number, doc_date,
            file_name, file_url, file_path, file_size, file_hash,
            product_category, product_codes, product_names, related_doc_numbers,
            applicant, department, responsible_units,
            summary, keywords, status, priority
        ) VALUES (
            %(original_doc_id)s, %(doc_type)s, %(doc_number)s, %(doc_date)s,
            %(file_name)s, %(file_url)s, %(file_path)s, %(file_size)s, %(file_hash)s,
            %(product_category)s, %(product_codes)s, %(product_names)s, %(related_doc_numbers)s,
            %(applicant)s, %(department)s, %(responsible_units)s,
            %(summary)s, %(keywords)s, %(status)s, %(priority)s
        )
        ON DUPLICATE KEY UPDATE
            doc_number = VALUES(doc_number),
            summary = VALUES(summary),
            keywords = VALUES(keywords)
        """
        
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # 轉換 JSON 欄位
                for field in ['product_codes', 'product_names', 'related_doc_numbers', 'responsible_units', 'keywords']:
                    if field in data and isinstance(data[field], list):
                        data[field] = json.dumps(data[field], ensure_ascii=False)
                
                cursor.execute(sql, data)
                conn.commit()
                logger.info(f"✅ 已儲存到 structured_documents: {data.get('doc_number', 'N/A')}")
                return True
        except Exception as e:
            logger.error(f"儲存 structured_documents 失敗: {e}")
            return False

    def close(self):
        """關閉資料庫連線"""
        if self.connection:
            self.connection.close()

# ========== 內容解析器 ==========
class ContentParser:
    """內容解析器 - 從文本中提取結構化資料"""
    
    @staticmethod
    def extract_ecn_notice(content: str) -> Dict:
        """解析設變通知單"""
        data = {}
        
        # 通知單單號
        match = re.search(r'(?:單號|編號)[:：\s]*([A-Z]{2,}-[A-Z]-\d{2}-[A-Z]-\d{3})', content)
        data['notice_number'] = match.group(1) if match else None
        
        # 設變申請單號
        match = re.search(r'設變申請單號[:：\s]*([A-Z]{2,}-[A-Z]-\d{2}-[A-Z]-\d{3})', content)
        data['application_number'] = match.group(1) if match else None
        
        # 適用範圍
        match = re.search(r'適用範圍[:：\s]*([^\n]+)', content)
        data['applicable_scope'] = match.group(1).strip() if match else None
        
        # 品號
        match = re.search(r'品號[:：\s]*([^\s\n]+)', content)
        data['product_code'] = match.group(1).strip() if match else None
        
        # 品名
        match = re.search(r'品名[:：\s]*([^\n]+)', content)
        data['product_name'] = match.group(1).strip() if match else None
        
        # 設變說明
        match = re.search(r'設變說明[:：\s]*([^\n]+(?:\n(?!設變前|設變後|庫存處理).+)*)', content, re.MULTILINE)
        data['change_description'] = match.group(1).strip() if match else None
        
        # 設變前
        match = re.search(r'設變前[:：\s]*([^\n]+)', content)
        data['before_change'] = match.group(1).strip() if match else None
        
        # 設變後
        match = re.search(r'設變後[:：\s]*([^\n]+)', content)
        data['after_change'] = match.group(1).strip() if match else None
        
        # 庫存處理
        match = re.search(r'庫存處理[:：\s]*([^\n]+)', content)
        data['inventory_handling'] = match.group(1).strip() if match else None
        
        # 申請人
        match = re.search(r'申請人[:：\s]*([^\n]+)', content)
        data['applicant'] = match.group(1).strip() if match else None
        
        # 日期
        match = re.search(r'(\d{4}[/-]\d{1,2}[/-]\d{1,2})', content)
        if match:
            try:
                date_str = match.group(1).replace('/', '-')
                data['ecn_date'] = date_str
            except:
                data['ecn_date'] = None
        else:
            data['ecn_date'] = None
        
        # 其他欄位設為 None
        data['inventory_doc_number'] = None
        data['inventory_product_name'] = None
        data['inventory_notes'] = None
        
        return data
    
    @staticmethod
    def extract_ecn_application(content: str) -> Dict:
        """解析設變申請單"""
        data = {}
        
        # 申請單號
        match = re.search(r'(?:申請單號|案號|開發案號)[:：\s]*([A-Z0-9-]+)', content)
        data['application_number'] = match.group(1) if match else None
        
        # 品名
        match = re.search(r'品名[:：\s]*([^\n]+)', content)
        data['product_name'] = match.group(1).strip() if match else None
        
        # 品號
        match = re.search(r'品號[:：\s]*([^\s\n]+)', content)
        data['product_code'] = match.group(1).strip() if match else None
        
        # 緣由
        match = re.search(r'緣由[:：\s]*([^\n]+(?:\n(?!設變項目).+)*)', content, re.MULTILINE)
        data['reason'] = match.group(1).strip() if match else None
        
        # 設變項目
        match = re.search(r'設變項目[:：\s]*([^\n]+)', content)
        data['change_items'] = match.group(1).strip() if match else None
        
        # 設變前
        match = re.search(r'設變(?:項目)?(?:前|原)[:：\s]*([^\n]+)', content)
        data['change_before'] = match.group(1).strip() if match else None
        
        # 設變後
        match = re.search(r'設變(?:項目)?後[:：\s]*([^\n]+)', content)
        data['change_after'] = match.group(1).strip() if match else None
        
        # 說明
        match = re.search(r'(?:設變項目)?說明[:：\s]*([^\n]+(?:\n(?!庫存處理).+)*)', content, re.MULTILINE)
        data['change_item_description'] = match.group(1).strip() if match else None
        
        # 會議建議
        match = re.search(r'會議建議[:：\s]*([^\n]+(?:\n(?!審查).+)*)', content, re.MULTILINE)
        data['meeting_suggestions'] = match.group(1).strip() if match else None
        
        # 審查說明
        match = re.search(r'(?:審查)?說明[:：\s]*([^\n]+(?:\n(?!審查核決).+)*)', content, re.MULTILINE)
        data['review_notes'] = match.group(1).strip() if match else None
        
        # 日期
        match = re.search(r'(\d{4}[/-]\d{1,2}[/-]\d{1,2})', content)
        if match:
            try:
                date_str = match.group(1).replace('/', '-')
                data['ecn_date'] = date_str
            except:
                data['ecn_date'] = None
        else:
            data['ecn_date'] = None
        
        # 其他欄位設為 None
        data['inventory_handling'] = None
        data['inventory_product_code'] = None
        data['inventory_quantity'] = None
        data['inventory_notes'] = None
        data['execution_plan'] = None
        data['review_decision'] = None
        data['review_meeting_date'] = None
        
        return data
    
    @staticmethod
    def extract_fmea(content: str) -> Dict:
        """解析 FMEA 分析表"""
        data = {}
        
        # 開發案號/案號
        match = re.search(r'(?:開發案號|案號|开发案号)[:：\s]*([A-Z0-9]+)', content)
        data['case_number'] = match.group(1) if match else None
        
        # 案件名稱
        match = re.search(r'案件名[稱称][:：\s]*([^\n]+)', content)
        data['case_name'] = match.group(1).strip() if match else None
        
        # 分析類型
        if 'DFMEA' in content or '■DFMEA' in content:
            data['analysis_type'] = 'DFMEA'
        elif 'PFMEA' in content or '■PFMEA' in content:
            data['analysis_type'] = 'PFMEA'
        else:
            data['analysis_type'] = 'FMEA'
        
        # 產品別
        match = re.search(r'產[\s]*品[\s]*別[:：\s]*([^\s\n]+)', content)
        data['product_type'] = match.group(1).strip() if match else None
        
        # 專案負責人/承辦人
        match = re.search(r'(?:專案負責人|承辦人)[:：\s]*([^\s\n]+)', content)
        data['responsible_person'] = match.group(1).strip() if match else None
        
        # 分析人員
        match = re.search(r'分析人員[:：\s]*([^\n]+)', content)
        if match:
            analyst_text = match.group(1).strip()
            # 移除部門資訊，只保留人名
            analyst_text = re.sub(r'產研部|模具部|生技部|開發課|品保部|____+', '', analyst_text)
            data['analyst'] = analyst_text.strip()
        else:
            data['analyst'] = None
        
        # 分析項目
        match = re.search(r'分析項目[:：\s]*([^\n]+)', content)
        data['analysis_item'] = match.group(1).strip() if match else None
        
        # 失效模式/問題描述
        match = re.search(r'(?:失效模式|問題描述)[:：\s]*([^\n]+)', content)
        data['failure_mode'] = match.group(1).strip() if match else None
        
        # 失效影響/效應分析
        match = re.search(r'(?:效應分析|失效影響)[:：\s]*([^\n]+(?:\n(?!失效成因).+)*)', content, re.MULTILINE)
        data['failure_effect'] = match.group(1).strip() if match else None
        
        # 失效成因分析
        match = re.search(r'失效成因分析[:：\s]*([^\n]+(?:\n(?!對策方案|現行管控).+)*)', content, re.MULTILINE)
        data['failure_cause'] = match.group(1).strip() if match else None
        
        # 嚴重度 S
        match = re.search(r'嚴重\s*度\s*[S\(S\)]?\s*[:：]?\s*(\d+)', content)
        if match:
            try:
                data['severity_s'] = int(match.group(1))
            except:
                data['severity_s'] = None
        else:
            data['severity_s'] = None
        
        # 發生度 O  
        match = re.search(r'發生\s*度\s*[O\(O\)]?\s*[:：]?\s*(\d+)', content)
        if match:
            try:
                data['occurrence_o'] = int(match.group(1))
            except:
                data['occurrence_o'] = None
        else:
            data['occurrence_o'] = None
        
        # 難檢度 D
        match = re.search(r'難檢\s*度\s*[D\(D\)]?\s*[:：]?\s*(\d+)', content)
        if match:
            try:
                data['detection_d'] = int(match.group(1))
            except:
                data['detection_d'] = None
        else:
            data['detection_d'] = None
        
        # 風險優先數 RPN
        match = re.search(r'RPN\s*[:：]?\s*(\d+)', content)
        if match:
            try:
                data['rpn'] = int(match.group(1))
            except:
                data['rpn'] = None
        else:
            # 如果找不到，嘗試計算
            if data.get('severity_s') and data.get('occurrence_o') and data.get('detection_d'):
                data['rpn'] = data['severity_s'] * data['occurrence_o'] * data['detection_d']
            else:
                data['rpn'] = None
        
        # 現行管控及檢測方式
        match = re.search(r'現行管控\s*及檢測方式[:：\s]*([^\n]+)', content)
        data['current_control'] = match.group(1).strip() if match else None
        
        # 對策方案/改善措施
        match = re.search(r'(?:對策方案|改善措施)[:：\s]*([^\n]+(?:\n(?!改善結果|預定完成).+)*)', content, re.MULTILINE)
        data['corrective_action'] = match.group(1).strip() if match else None
        
        # 改善結果
        match = re.search(r'改善結果[:：\s]*([^\n]+)', content)
        data['improvement_result'] = match.group(1).strip() if match else None
        
        # 預定完成日
        match = re.search(r'預定完成日[:：\s]*(\d{4}[/-]\d{1,2}[/-]\d{1,2})', content)
        if match:
            try:
                date_str = match.group(1).replace('/', '-')
                data['target_completion_date'] = date_str
            except:
                data['target_completion_date'] = None
        else:
            data['target_completion_date'] = None
        
        # 執行單位
        match = re.search(r'執行單位[:：\s]*([^\n]+)', content)
        data['execution_unit'] = match.group(1).strip() if match else None
        
        # 改善後評分（如果有的話）
        data['improved_severity'] = None
        data['improved_occurrence'] = None
        data['improved_detection'] = None
        data['improved_rpn'] = None
        
        # 是否為客訴案
        data['is_customer_complaint'] = '客訴案' in content or '客訴' in (data.get('case_name') or '')
        
        # 部主管
        match = re.search(r'部主管[:：\s]*([^\s\n]+)', content)
        data['department_head'] = match.group(1).strip() if match else None
        
        # 課主管
        match = re.search(r'課主管[:：\s]*([^\s\n]+)', content)
        data['section_head'] = match.group(1).strip() if match else None
        
        # 制訂日期
        match = re.search(r'制[訂订]日期[:：\s]*(\d{4}[/-]\d{1,2}[/-]\d{1,2})', content)
        if match:
            try:
                date_str = match.group(1).replace('/', '-')
                data['form_date'] = date_str
            except:
                data['form_date'] = None
        else:
            data['form_date'] = None
        
        # 修訂日期
        match = re.search(r'修[訂订]日期[:：\s]*(\d{4}[/-]\d{1,2}[/-]\d{1,2})', content)
        if match:
            try:
                date_str = match.group(1).replace('/', '-')
                data['revision_date'] = date_str
            except:
                data['revision_date'] = None
        else:
            data['revision_date'] = None
        
        # 版次
        match = re.search(r'版次[:：\s]*([^\s\n]+)', content)
        data['version'] = match.group(1).strip() if match else None
        
        # 部門（從分析人員中提取）
        data['department'] = None
        if '產研部' in content:
            data['department'] = '產研部'
        
        return data
    
    @staticmethod
    def extract_complaint(content: str) -> Dict:
        """解析客訴資料"""
        data = {}
        
        # 異常單號
        match = re.search(r'異常單號[:：\s]*([^\s\n]+)', content)
        data['complaint_number'] = match.group(1).strip() if match else None
        
        # 開單類別
        match = re.search(r'開單類別[:：\s]*([^\n]+)', content)
        data['complaint_type'] = match.group(1).strip() if match else None
        
        # 來源單號
        match = re.search(r'來源單號[:：\s]*([^\s\n]+)', content)
        data['source_number'] = match.group(1).strip() if match else None
        
        # 生產通知單號
        match = re.search(r'生產通知單號[:：\s]*([^\s\n]+)', content)
        data['production_notice_number'] = match.group(1).strip() if match else None
        
        # 客戶代號/名稱
        match = re.search(r'客戶(?:代號|名稱)[:：\s]*([^\n]+)', content)
        if match:
            customer_info = match.group(1).strip()
            # 嘗試分離代號和名稱
            parts = re.split(r'[/\s]+', customer_info)
            data['customer_code'] = parts[0] if len(parts) > 0 else None
            data['customer_name'] = parts[1] if len(parts) > 1 else customer_info
        else:
            data['customer_code'] = None
            data['customer_name'] = None
        
        # 產品項目
        match = re.search(r'產品項目[:：\s]*([^\n]+)', content)
        data['product_item'] = match.group(1).strip() if match else None
        
        # 品號
        match = re.search(r'品號[:：\s]*([^\s\n]+)', content)
        data['product_code'] = match.group(1).strip() if match else None
        
        # 品名
        match = re.search(r'品名[:：\s]*([^\n]+)', content)
        data['product_name'] = match.group(1).strip() if match else None
        
        # 出貨廠別
        match = re.search(r'出貨廠別[:：\s]*([^\n]+)', content)
        data['shipping_factory'] = match.group(1).strip() if match else None
        
        # 抱怨內容描述
        match = re.search(r'抱怨內容(?:描述)?[:：\s]*([^\n]+(?:\n(?!承辦|抱怨內容分析).+)*)', content, re.MULTILINE)
        data['complaint_description'] = match.group(1).strip() if match else None
        
        # 承辦業務
        match = re.search(r'承辦業務[:：\s]*([^\n]+)', content)
        data['responsible_sales'] = match.group(1).strip() if match else None
        
        # 抱怨內容分析
        match = re.search(r'抱怨內容分析[:：\s]*([^\n]+(?:\n.+)*)', content, re.MULTILINE)
        data['complaint_analysis'] = match.group(1).strip() if match else None
        
        return data
    
    @staticmethod
    def extract_common_fields(content: str, doc_type: str) -> Dict:
        """提取通用欄位 (用於 structured_documents)"""
        data = {}
        
        # 文檔編號 (根據類型提取)
        doc_number_patterns = [
            r'(?:單號|編號|申請單號|案號|異常單號)[:：\s]*([A-Z]{2,}-[A-Z]-\d{2}-[A-Z]-\d{3})',
            r'(?:開發案號|案號)[:：\s]*([A-Z0-9-]+)',
        ]
        for pattern in doc_number_patterns:
            match = re.search(pattern, content)
            if match:
                data['doc_number'] = match.group(1)
                break
        else:
            data['doc_number'] = None
        
        # 產品編號 (提取所有可能的產品編號)
        product_codes = re.findall(r'\b([0-9]{2,3}[*×xX-][A-Z0-9-]+)\b', content)
        data['product_codes'] = list(set(product_codes))[:5] if product_codes else []
        
        # 產品名稱
        product_names = []
        for match in re.finditer(r'品名[:：\s]*([^\n]+)', content):
            name = match.group(1).strip()
            if name and len(name) < 100:
                product_names.append(name)
        data['product_names'] = list(set(product_names))[:3] if product_names else []
        
        # 申請人
        match = re.search(r'申請人[:：\s]*([^\n]+)', content)
        data['applicant'] = match.group(1).strip() if match else None
        
        # 日期
        match = re.search(r'(\d{4}[/-]\d{1,2}[/-]\d{1,2})', content)
        if match:
            try:
                date_str = match.group(1).replace('/', '-')
                data['doc_date'] = date_str
            except:
                data['doc_date'] = None
        else:
            data['doc_date'] = None
        
        # 生成摘要 (前200字)
        clean_content = re.sub(r'\s+', ' ', content)
        data['summary'] = clean_content[:200] + '...' if len(clean_content) > 200 else clean_content
        
        # 關鍵字
        keywords = []
        if doc_type == 'ECN_NOTICE':
            keywords.extend(['設變通知單', '設變', '通知', '設變說明'])
        elif doc_type == 'ECN_APPLICATION':
            keywords.extend(['申請單', '申請單位', '緣由', '設變執行'])
        elif doc_type == 'COMPLAINT':
            keywords.extend(['客訴', '異常', '顧客抱怨處理資料'])
        elif doc_type == 'FMEA':
            keywords.extend(['FMEA', 'DFMEA表'])
        
        if data['product_codes']:
            keywords.extend(data['product_codes'][:3])
        
        data['keywords'] = keywords
        
        # 優先級
        if any(kw in content for kw in ['嚴重', '緊急', '立即', '重大']):
            data['priority'] = 'HIGH'
        else:
            data['priority'] = 'NORMAL'
        
        data['status'] = 'PARSED'
        data['product_category'] = None
        data['related_doc_numbers'] = []
        data['department'] = None
        data['responsible_units'] = []
        
        return data

# ========== 文檔處理器 ==========
class DocumentProcessor:
    """文檔處理器"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.parser = ContentParser()
        self.running = False
    
    def process_document(self, doc: Dict) -> bool:
        """處理單個文檔"""
        doc_id = doc['doc_id']
        doc_type = doc['doc_type']
        content = doc['content']
        file_name = doc['file_name']
        
        logger.info(f"處理文件 {doc_id}, 類型: {doc_type}, 檔案: {file_name}")
        
        try:
            # 1. 解析到專屬表
            if doc_type == 'ECN_NOTICE':
                parsed_data = self.parser.extract_ecn_notice(content)
                success = self.db.save_to_ecn_notice(doc_id, parsed_data)
            elif doc_type == 'ECN_APPLICATION':
                parsed_data = self.parser.extract_ecn_application(content)
                success = self.db.save_to_ecn_application(doc_id, parsed_data)
            elif doc_type == 'COMPLAINT':
                parsed_data = self.parser.extract_complaint(content)
                success = self.db.save_to_complaint(doc_id, parsed_data)
            elif doc_type == 'FMEA':
                parsed_data = self.parser.extract_fmea(content)
                success = self.db.save_to_fmea(doc_id, parsed_data)
            else:
                logger.warning(f"未知文檔類型: {doc_type}")
                success = False
            
            # 2. 解析到 structured_documents (通用欄位)
            common_data = self.parser.extract_common_fields(content, doc_type)
            common_data['original_doc_id'] = doc_id
            common_data['doc_type'] = doc_type
            common_data['file_name'] = file_name
            common_data['file_path'] = f"{PDF_STORAGE_PATH}/{file_name}"
            common_data['file_url'] = f"{FILE_SERVICE_BASE_URL}/download/{doc_id}/{file_name}"
            common_data['file_size'] = 0
            common_data['file_hash'] = doc_id
            
            success_common = self.db.save_to_structured_documents(common_data)
            
            if success and success_common:
                logger.info(f"✅ 文件 {doc_id} 處理成功")
                logger.info(f"   編號: {common_data.get('doc_number', 'N/A')}")
                logger.info(f"   摘要: {common_data.get('summary', '')[:50]}...")
                return True
            else:
                logger.error(f"❌ 文件 {doc_id} 儲存失敗")
                return False
            
        except Exception as e:
            logger.error(f"處理文件失敗 {doc_id}: {e}", exc_info=True)
            return False
    
    def run(self):
        """主執行迴圈"""
        self.running = True
        logger.info("=" * 60)
        logger.info("文件解析服務啟動")
        logger.info(f"掃描間隔: {SCAN_INTERVAL} 秒")
        logger.info(f"批次大小: {BATCH_SIZE}")
        logger.info("=" * 60)
        
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
                else:
                    logger.debug("沒有待處理文件")
                
                # 等待
                for _ in range(SCAN_INTERVAL):
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"主迴圈錯誤: {e}")
                time.sleep(10)
        
        logger.info("服務停止")

# ========== 主程式 ==========
if __name__ == "__main__":
    try:
        processor = DocumentProcessor()
        processor.run()
    except KeyboardInterrupt:
        logger.info("收到中斷訊號")
    except Exception as e:
        logger.error(f"服務異常: {e}")
        sys.exit(1)
