#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
文件結構化解析服務 - 福興工業技術文件自動化處理系統
=============================================================================

功能概述：
1. 從 technical_documents 表讀取未處理的 OCR 文字
2. 根據文件類型智慧解析結構化資料
3. 提取關鍵欄位（單號、日期、負責人、產品資訊等）
4. 將結構化資料存入 structured_documents 表
5. 支援多種文件類型（設變通知、客訴、FMEA等）

資料流程：
    technical_documents表 → 解析引擎 → structured_documents表

作者: [您的團隊]
版本: 1.0.0
更新日期: 2024
=============================================================================
"""

import os
import sys
import json
import re
import time
import logging
import hashlib
import pymysql
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict, field
from pathlib import Path
import signal

# ============================================================================
# 環境變數配置
# ============================================================================

# MySQL 連線設定
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "fuhsin_erp_demo")

# 處理參數設定
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))  # 掃描間隔（秒）
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))  # 每批處理文件數
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

# 日誌設定
LOG_LEVEL = logging.DEBUG if DEBUG_MODE else logging.INFO
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DIR = Path("/logs/parser")
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# 日誌配置
# ============================================================================

logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_DIR / "document_parser.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# 資料類型定義
# ============================================================================

@dataclass
class StructuredDocument:
    """結構化文件資料模型"""
    
    # 基本識別資訊
    original_doc_id: str  # 關聯到 technical_documents.doc_id
    doc_type: str  # 文件類型
    doc_number: str  # 文件編號
    doc_date: Optional[str] = None  # 文件日期
    
    # 產品與分類資訊
    product_category: Optional[str] = None  # 產品類別（G鎖/L鎖/T鎖）
    product_codes: List[str] = field(default_factory=list)  # 產品編號列表
    product_names: List[str] = field(default_factory=list)  # 產品名稱列表
    
    # 人員與部門資訊
    applicant: Optional[str] = None  # 申請人/負責人
    department: Optional[str] = None  # 部門
    responsible_units: List[str] = field(default_factory=list)  # 責任單位列表
    
    # 關聯文件
    related_doc_numbers: List[str] = field(default_factory=list)  # 關聯文件編號
    
    # 狀態與處理資訊
    status: Optional[str] = None  # 狀態
    priority: Optional[str] = None  # 優先級
    
    # 核心內容
    title: Optional[str] = None  # 標題
    description: Optional[str] = None  # 描述/說明
    root_cause: Optional[str] = None  # 根本原因（客訴用）
    corrective_action: Optional[str] = None  # 矯正措施
    
    # 詳細資料（JSON格式）
    form_data: Dict[str, Any] = field(default_factory=dict)  # 表單詳細資料
    metadata: Dict[str, Any] = field(default_factory=dict)  # 其他元資料
    
    # 搜尋優化欄位
    search_keywords: List[str] = field(default_factory=list)  # 搜尋關鍵字
    full_text: Optional[str] = None  # 完整文字（供全文檢索）

# ============================================================================
# 文件解析器基類
# ============================================================================

class DocumentParser:
    """文件解析器基類"""
    
    # 產品類別識別模式
    PRODUCT_CATEGORY_PATTERNS = {
        'G鎖': [r'G鎖', r'0B1-G', r'EC-K.*G', r'G\d{2,}'],
        'L鎖': [r'L鎖', r'LD\d{5,}', r'0B1-L', r'L\d{6}'],
        'T鎖': [r'T鎖', r'0B1-T', r'T\d{2,}']
    }
    
    # 通用產品編號模式
    PRODUCT_CODE_PATTERNS = [
        r'0B1-[GLT][0-9A-Z]+',  # 標準產品編號
        r'[GLT]\d{2}-[A-Z0-9]+',  # 簡化編號
        r'F\d{2}-L[0-9A-Z]+',  # F系列
        r'G\d{2}-L[0-9A-Z]+',  # G系列
    ]
    
    # 日期格式模式
    DATE_PATTERNS = [
        r'(\d{4})[/\-年](\d{1,2})[/\-月](\d{1,2})[日]?',  # 2024/12/25 或 2024年12月25日
        r'(\d{4})\.(\d{1,2})\.(\d{1,2})',  # 2024.12.25
    ]
    
    def __init__(self, content: str, file_name: str = "", doc_type: str = ""):
        """
        初始化解析器
        
        Args:
            content: OCR文字內容
            file_name: 原始檔案名稱
            doc_type: 文件類型（可選）
        """
        self.content = content
        self.file_name = file_name
        self.doc_type = doc_type
        self.lines = content.split('\n')
    
    def detect_product_category(self) -> Optional[str]:
        """
        偵測產品類別
        
        Returns:
            產品類別（G鎖/L鎖/T鎖）或 None
        """
        # 先從檔名判斷
        for category, patterns in self.PRODUCT_CATEGORY_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, self.file_name, re.IGNORECASE):
                    return category
        
        # 再從內容判斷
        for category, patterns in self.PRODUCT_CATEGORY_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, self.content, re.IGNORECASE):
                    return category
        
        return None
    
    def extract_date(self, text: str) -> Optional[str]:
        """
        提取日期
        
        Args:
            text: 要搜尋的文字
            
        Returns:
            格式化的日期字串 (YYYY-MM-DD) 或 None
        """
        for pattern in self.DATE_PATTERNS:
            match = re.search(pattern, text)
            if match:
                groups = match.groups()
                try:
                    year = int(groups[0])
                    month = int(groups[1])
                    day = int(groups[2])
                    return f"{year:04d}-{month:02d}-{day:02d}"
                except (ValueError, IndexError):
                    continue
        return None
    
    def extract_product_codes(self) -> List[str]:
        """
        提取所有產品編號
        
        Returns:
            產品編號列表
        """
        codes = set()
        for pattern in self.PRODUCT_CODE_PATTERNS:
            matches = re.findall(pattern, self.content)
            codes.update(matches)
        return list(codes)
    
    def extract_value_after_label(self, label: str, delimiter: str = "[:：]") -> Optional[str]:
        """
        提取標籤後的值
        
        Args:
            label: 標籤文字
            delimiter: 分隔符號模式
            
        Returns:
            提取的值或 None
        """
        pattern = f"{label}\\s*{delimiter}\\s*([^\\n\\s]+[^\\n]*)"
        match = re.search(pattern, self.content, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None
    
    def extract_section(self, start_label: str, end_labels: List[str] = None) -> Optional[str]:
        """
        提取區段文字
        
        Args:
            start_label: 開始標籤
            end_labels: 結束標籤列表
            
        Returns:
            區段文字或 None
        """
        start_idx = self.content.find(start_label)
        if start_idx == -1:
            return None
        
        start_idx += len(start_label)
        
        if end_labels:
            end_idx = len(self.content)
            for end_label in end_labels:
                idx = self.content.find(end_label, start_idx)
                if idx != -1 and idx < end_idx:
                    end_idx = idx
            
            if end_idx < len(self.content):
                return self.content[start_idx:end_idx].strip()
        
        # 如果沒有結束標籤，取到下一個空行或文件結束
        next_empty = self.content.find('\n\n', start_idx)
        if next_empty != -1:
            return self.content[start_idx:next_empty].strip()
        
        return self.content[start_idx:].strip()
    
    def parse(self) -> StructuredDocument:
        """
        解析文件（由子類實作）
        
        Returns:
            結構化文件物件
        """
        raise NotImplementedError("子類必須實作 parse 方法")

# ============================================================================
# 設變通知單解析器
# ============================================================================

class ECNParser(DocumentParser):
    """設變通知單解析器"""
    
    def parse(self) -> StructuredDocument:
        """解析設變通知單"""
        
        doc = StructuredDocument(
            original_doc_id="",  # 將由外部設定
            doc_type="設變通知單",
            doc_number="",
        )
        
        # 提取單號
        doc.doc_number = self.extract_value_after_label("單號") or ""
        if not doc.doc_number:
            # 嘗試從檔名或內容中找 LD 開頭的編號
            ld_match = re.search(r'LD\d{6,}', self.content)
            if ld_match:
                doc.doc_number = ld_match.group()
        
        # 提取日期
        date_text = self.extract_value_after_label("日期")
        if date_text:
            doc.doc_date = self.extract_date(date_text)
        
        # 提取申請人
        doc.applicant = self.extract_value_after_label("設變申請人")
        
        # 提取相關單號
        related_ec = self.extract_value_after_label("設變申請單號")
        if related_ec:
            doc.related_doc_numbers.append(related_ec)
        
        # 提取產品類別
        doc.product_category = self.detect_product_category()
        
        # 提取產品編號
        doc.product_codes = self.extract_product_codes()
        
        # 提取設變說明
        doc.description = self.extract_section(
            "設變說明", 
            ["設變申請人", "庫存處理", "設變前", "設變後"]
        )
        
        # 提取庫存處理資訊
        inventory_section = self.extract_section("庫存處理", ["設變前", "設變後", "實施日"])
        if inventory_section:
            doc.form_data['inventory_handling'] = inventory_section
        
        # 提取實施資訊
        implementation = self.extract_section("實施日", ["QP-K", "備註"])
        if implementation:
            doc.form_data['implementation_info'] = implementation
        
        # 建立搜尋關鍵字
        doc.search_keywords = [
            doc.doc_number,
            doc.doc_type,
            doc.product_category or "",
            doc.applicant or ""
        ] + doc.product_codes
        
        # 保留完整文字供全文檢索
        doc.full_text = self.content
        
        return doc

# ============================================================================
# 客訴單解析器
# ============================================================================

class ComplaintParser(DocumentParser):
    """客訴單解析器"""
    
    def parse(self) -> StructuredDocument:
        """解析客訴單"""
        
        doc = StructuredDocument(
            original_doc_id="",
            doc_type="客訴單",
            doc_number="",
        )
        
        # 提取異常單號
        doc.doc_number = self.extract_value_after_label("異常單號") or ""
        if not doc.doc_number:
            # 嘗試找 CPR 開頭的編號
            cpr_match = re.search(r'CPR-[A-Z0-9\-]+', self.content)
            if cpr_match:
                doc.doc_number = cpr_match.group()
        
        # 提取日期（可能有多個日期，取第一個）
        date_text = self.extract_value_after_label("日期")
        if date_text:
            doc.doc_date = self.extract_date(date_text)
        
        # 提取客戶資訊
        customer = self.extract_value_after_label("客戶")
        if customer:
            doc.form_data['customer'] = customer
        
        # 提取產品資訊
        doc.product_category = self.detect_product_category()
        doc.product_codes = self.extract_product_codes()
        
        # 提取產品項目
        product_item = self.extract_value_after_label("產品項目")
        if product_item:
            doc.product_names.append(product_item)
        
        # 提取客戶要求
        customer_request = self.extract_value_after_label("客戶要求")
        if customer_request:
            doc.form_data['customer_request'] = customer_request
        
        # 提取抱怨內容分析
        doc.description = self.extract_section(
            "抱怨內容分析",
            ["問題的根本原因", "暫定對策", "緊急處理", "矯正措施"]
        )
        
        # 提取根本原因
        doc.root_cause = self.extract_section(
            "問題的根本原因",
            ["矯正措施", "預防再發", "暫定對策"]
        )
        
        # 提取矯正措施
        doc.corrective_action = self.extract_section(
            "矯正措施",
            ["預防再發", "緊急處理", "簽核"]
        )
        
        # 提取責任單位
        units = re.findall(r'([^\\s]+課)', self.content)
        if units:
            doc.responsible_units = list(set(units))
        
        # 建立搜尋關鍵字
        doc.search_keywords = [
            doc.doc_number,
            doc.doc_type,
            doc.product_category or "",
            customer or ""
        ] + doc.product_codes + doc.product_names
        
        doc.full_text = self.content
        
        return doc

# ============================================================================
# FMEA 解析器
# ============================================================================

class FMEAParser(DocumentParser):
    """FMEA 表單解析器"""
    
    def parse(self) -> StructuredDocument:
        """解析 FMEA 表單"""
        
        doc = StructuredDocument(
            original_doc_id="",
            doc_type="FMEA",
            doc_number="",
        )
        
        # 提取開發案號
        doc.doc_number = self.extract_value_after_label("開發案號") or ""
        if not doc.doc_number:
            # 嘗試找 R 或 RD 開頭的編號
            rd_match = re.search(r'R[D]?\d{5,}', self.content)
            if rd_match:
                doc.doc_number = rd_match.group()
        
        # 提取案件名稱作為標題
        doc.title = self.extract_value_after_label("案件名稱")
        
        # 提取負責人
        doc.applicant = self.extract_value_after_label("專案負責人") or \
                        self.extract_value_after_label("承辦人")
        
        # 提取產品別
        product_type = self.extract_value_after_label("產品別")
        if product_type:
            doc.form_data['product_type'] = product_type
            # 從產品別判斷類別
            if 'G鎖' in product_type:
                doc.product_category = 'G鎖'
            elif 'L鎖' in product_type:
                doc.product_category = 'L鎖'
            elif 'T鎖' in product_type:
                doc.product_category = 'T鎖'
        
        if not doc.product_category:
            doc.product_category = self.detect_product_category()
        
        # 提取分析人員
        analysts = self.extract_value_after_label("分析人員")
        if analysts:
            doc.form_data['analysts'] = analysts
        
        # 提取失效模式相關內容
        failure_modes = []
        # 簡單提取包含"失效"的行作為關鍵資訊
        for line in self.lines:
            if '失效' in line and len(line) > 10:
                failure_modes.append(line.strip())
        
        if failure_modes:
            doc.form_data['failure_modes'] = failure_modes[:10]  # 限制數量
        
        # 將整個 FMEA 內容作為描述
        doc.description = self.content[:1000]  # 限制長度
        
        # 建立搜尋關鍵字
        doc.search_keywords = [
            doc.doc_number,
            doc.doc_type,
            doc.title or "",
            doc.product_category or "",
            product_type or ""
        ]
        
        doc.full_text = self.content
        
        return doc

# ============================================================================
# 通用文件解析器
# ============================================================================

class GenericParser(DocumentParser):
    """通用文件解析器（用於無法識別類型的文件）"""
    
    def parse(self) -> StructuredDocument:
        """解析通用文件"""
        
        doc = StructuredDocument(
            original_doc_id="",
            doc_type=self.doc_type or "其他",
            doc_number="",
        )
        
        # 嘗試提取各種可能的編號
        possible_numbers = []
        
        # 各種編號模式
        patterns = [
            r'單號\s*[:：]\s*([A-Z0-9\-]+)',
            r'編號\s*[:：]\s*([A-Z0-9\-]+)',
            r'案號\s*[:：]\s*([A-Z0-9\-]+)',
            r'([A-Z]{2,3}-[A-Z0-9\-]+)',  # 通用編號格式
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, self.content)
            possible_numbers.extend(matches)
        
        if possible_numbers:
            doc.doc_number = possible_numbers[0]
        
        # 提取日期
        all_dates = []
        for line in self.lines[:20]:  # 只看前20行
            date = self.extract_date(line)
            if date:
                all_dates.append(date)
        
        if all_dates:
            doc.doc_date = all_dates[0]
        
        # 提取產品類別和編號
        doc.product_category = self.detect_product_category()
        doc.product_codes = self.extract_product_codes()
        
        # 提取可能的人名（簡單規則）
        names = re.findall(r'([楊黃林陳李王張劉吳蔡許鄭謝鍾董周鐘何程葉姚朱曾][^\s]{1,2})', self.content)
        if names:
            doc.applicant = names[0]
        
        # 使用前500字作為描述
        doc.description = self.content[:500]
        
        # 建立基本關鍵字
        doc.search_keywords = [
            doc.doc_number,
            doc.doc_type,
            doc.product_category or ""
        ] + doc.product_codes
        
        doc.full_text = self.content
        
        return doc

# ============================================================================
# 文件類型識別器
# ============================================================================

class DocumentTypeDetector:
    """文件類型自動識別器"""
    
    # 文件類型識別規則
    TYPE_RULES = {
        '設變通知單': {
            'keywords': ['設變通知單','設變申請單號', 'LD\\d{6}', '設變說明'],
            'weight': 10
        },
        '客訴單': {
            'keywords': ['客戶抱怨', '客訴', '異常單號', 'CPR-', '抱怨內容分析'],
            'weight': 10
        },
        'FMEA': {
            'keywords': ['FMEA', 'DFMEA', 'PFMEA', '失效模式', '嚴重度', '發生度'],
            'weight': 10
        },
        '規格書': {
            'keywords': ['規格書', '產品規格', '技術規格', 'SPEC'],
            'weight': 8
        },
        '測試報告': {
            'keywords': ['測試報告', '檢驗報告', 'TEST REPORT'],
            'weight': 8
        },
        '設變申請單':{
            'keywords': ['設變申請', 'EC-K-\\d{2}-[A_Z]{2}-\\d{3}'],
            'weight': 10
        },
    }
    
    @classmethod
    def detect(cls, content: str, file_name: str = "") -> str:
        """
        識別文件類型
        
        Args:
            content: 文件內容
            file_name: 檔案名稱
            
        Returns:
            文件類型
        """
        scores = {}
        
        # 從內容判斷
        for doc_type, rules in cls.TYPE_RULES.items():
            score = 0
            for keyword in rules['keywords']:
                if re.search(keyword, content, re.IGNORECASE):
                    score += rules['weight']
            
            # 從檔名加分
            if file_name and re.search(doc_type, file_name, re.IGNORECASE):
                score += 5
            
            scores[doc_type] = score
        
        # 選擇分數最高的類型
        if scores:
            best_type = max(scores, key=scores.get)
            if scores[best_type] > 0:
                return best_type
        
        return "其他"

# ============================================================================
# 資料庫管理器
# ============================================================================

class DatabaseManager:
    """資料庫操作管理器"""
    
    def __init__(self):
        self.connection = None
        self.connect()
        self.init_tables()
    
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
            logger.info(f"✅ 資料庫連線成功: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
        except Exception as e:
            logger.error(f"❌ 資料庫連線失敗: {e}")
            raise
    
    def init_tables(self):
        """初始化結構化文件表"""
        try:
            with self.connection.cursor() as cursor:
                # 建立 structured_documents 表
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS structured_documents (
                    -- 主鍵
                    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主鍵',
                    
                    -- 關聯原始文件
                    original_doc_id VARCHAR(32) NOT NULL COMMENT '關聯 technical_documents.doc_id',
                    
                    -- 基本識別資訊
                    doc_type VARCHAR(50) NOT NULL COMMENT '文件類型',
                    doc_number VARCHAR(100) COMMENT '文件編號',
                    doc_date DATE COMMENT '文件日期',
                    
                    -- 產品資訊
                    product_category VARCHAR(20) COMMENT '產品類別(G鎖/L鎖/T鎖)',
                    product_codes JSON COMMENT '產品編號列表',
                    product_names JSON COMMENT '產品名稱列表',
                    
                    -- 人員與部門
                    applicant VARCHAR(100) COMMENT '申請人/負責人',
                    department VARCHAR(100) COMMENT '部門',
                    responsible_units JSON COMMENT '責任單位列表',
                    
                    -- 關聯與狀態
                    related_doc_numbers JSON COMMENT '關聯文件編號',
                    status VARCHAR(50) COMMENT '狀態',
                    priority VARCHAR(20) COMMENT '優先級',
                    
                    -- 內容欄位
                    title VARCHAR(500) COMMENT '標題',
                    description TEXT COMMENT '描述/說明',
                    root_cause TEXT COMMENT '根本原因',
                    corrective_action TEXT COMMENT '矯正措施',
                    
                    -- JSON 詳細資料
                    form_data JSON COMMENT '表單詳細資料',
                    metadata JSON COMMENT '其他元資料',
                    
                    -- 搜尋優化
                    search_keywords JSON COMMENT '搜尋關鍵字',
                    full_text LONGTEXT COMMENT '完整文字',
                    
                    -- 處理狀態
                    parse_status ENUM('pending', 'success', 'failed') DEFAULT 'success' 
                        COMMENT '解析狀態',
                    parse_error TEXT COMMENT '解析錯誤訊息',
                    
                    -- 時間戳記
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間',
                    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP 
                        COMMENT '更新時間',
                    
                    -- 索引
                    INDEX idx_original_doc (original_doc_id),
                    INDEX idx_doc_type (doc_type),
                    INDEX idx_doc_number (doc_number),
                    INDEX idx_doc_date (doc_date),
                    INDEX idx_product_category (product_category),
                    INDEX idx_applicant (applicant),
                    INDEX idx_status (status),
                    INDEX idx_created_at (created_at),
                    FULLTEXT idx_full_text (full_text),
                    
                    -- 外鍵約束（確保關聯到存在的原始文件）
                    CONSTRAINT fk_original_doc 
                        FOREIGN KEY (original_doc_id) 
                        REFERENCES technical_documents(doc_id)
                        ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                COMMENT='結構化文件表 - 存放解析後的結構化資料';
                """
                
                cursor.execute(create_table_sql)
                logger.info("✅ 確認表格存在: structured_documents")
                
                # 建立解析狀態追蹤表
                create_status_table_sql = """
                CREATE TABLE IF NOT EXISTS document_parse_status (
                    doc_id VARCHAR(32) PRIMARY KEY COMMENT '文件ID',
                    last_parsed TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '最後解析時間',
                    parse_count INT DEFAULT 1 COMMENT '解析次數',
                    INDEX idx_last_parsed (last_parsed)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                COMMENT='文件解析狀態追蹤表';
                """
                
                cursor.execute(create_status_table_sql)
                logger.info("✅ 確認表格存在: document_parse_status")
                
                self.connection.commit()
                
        except Exception as e:
            logger.error(f"初始化表格失敗: {e}")
            raise
    
    def get_unprocessed_documents(self, limit: int = 10) -> List[Dict]:
        """
        取得未處理的文件
        
        Args:
            limit: 最大取得數量
            
        Returns:
            未處理文件列表
        """
        sql = """
        SELECT 
            td.doc_id,
            td.doc_type,
            td.file_name,
            td.content,
            td.created_at
        FROM technical_documents td
        LEFT JOIN document_parse_status dps ON td.doc_id = dps.doc_id
        WHERE dps.doc_id IS NULL  -- 從未解析過
           OR TIMESTAMPDIFF(HOUR, dps.last_parsed, NOW()) > 24  -- 或超過24小時未更新
        ORDER BY td.created_at DESC
        LIMIT %s
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (limit,))
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"查詢未處理文件失敗: {e}")
            return []
    
    def save_structured_document(self, doc: StructuredDocument) -> bool:
        """
        儲存結構化文件
        
        Args:
            doc: 結構化文件物件
            
        Returns:
            是否成功
        """
        sql = """
        INSERT INTO structured_documents (
            original_doc_id, doc_type, doc_number, doc_date,
            product_category, product_codes, product_names,
            applicant, department, responsible_units,
            related_doc_numbers, status, priority,
            title, description, root_cause, corrective_action,
            form_data, metadata, search_keywords, full_text
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            doc_type = VALUES(doc_type),
            doc_number = VALUES(doc_number),
            doc_date = VALUES(doc_date),
            product_category = VALUES(product_category),
            product_codes = VALUES(product_codes),
            product_names = VALUES(product_names),
            applicant = VALUES(applicant),
            department = VALUES(department),
            responsible_units = VALUES(responsible_units),
            related_doc_numbers = VALUES(related_doc_numbers),
            status = VALUES(status),
            priority = VALUES(priority),
            title = VALUES(title),
            description = VALUES(description),
            root_cause = VALUES(root_cause),
            corrective_action = VALUES(corrective_action),
            form_data = VALUES(form_data),
            metadata = VALUES(metadata),
            search_keywords = VALUES(search_keywords),
            full_text = VALUES(full_text),
            last_modified = CURRENT_TIMESTAMP
        """
        
        try:
            # 轉換列表和字典為 JSON 字串
            values = (
                doc.original_doc_id,
                doc.doc_type,
                doc.doc_number,
                doc.doc_date,
                doc.product_category,
                json.dumps(doc.product_codes, ensure_ascii=False) if doc.product_codes else None,
                json.dumps(doc.product_names, ensure_ascii=False) if doc.product_names else None,
                doc.applicant,
                doc.department,
                json.dumps(doc.responsible_units, ensure_ascii=False) if doc.responsible_units else None,
                json.dumps(doc.related_doc_numbers, ensure_ascii=False) if doc.related_doc_numbers else None,
                doc.status,
                doc.priority,
                doc.title,
                doc.description,
                doc.root_cause,
                doc.corrective_action,
                json.dumps(doc.form_data, ensure_ascii=False) if doc.form_data else None,
                json.dumps(doc.metadata, ensure_ascii=False) if doc.metadata else None,
                json.dumps(doc.search_keywords, ensure_ascii=False) if doc.search_keywords else None,
                doc.full_text
            )
            
            with self.connection.cursor() as cursor:
                cursor.execute(sql, values)
                self.connection.commit()
                
                # 更新解析狀態
                self.update_parse_status(doc.original_doc_id)
                
                return True
                
        except Exception as e:
            logger.error(f"儲存結構化文件失敗: {e}")
            self.connection.rollback()
            return False
    
    def update_parse_status(self, doc_id: str):
        """更新解析狀態"""
        sql = """
        INSERT INTO document_parse_status (doc_id, last_parsed, parse_count)
        VALUES (%s, CURRENT_TIMESTAMP, 1)
        ON DUPLICATE KEY UPDATE
            last_parsed = CURRENT_TIMESTAMP,
            parse_count = parse_count + 1
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, (doc_id,))
                self.connection.commit()
        except Exception as e:
            logger.error(f"更新解析狀態失敗: {e}")

# ============================================================================
# 文件處理協調器
# ============================================================================

class DocumentProcessor:
    """文件處理協調器"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        
        # 解析器映射
        self.parsers = {
            '設變通知單': ECNParser,
            '客訴單': ComplaintParser,
            'FMEA': FMEAParser
        }
    
    def process_document(self, doc_data: Dict) -> bool:
        """
        處理單一文件
        
        Args:
            doc_data: 從資料庫取得的文件資料
            
        Returns:
            是否處理成功
        """
        try:
            doc_id = doc_data['doc_id']
            content = doc_data['content']
            file_name = doc_data.get('file_name', '')
            
            logger.info(f"📄 開始處理文件: {file_name} (ID: {doc_id})")
            
            # 識別文件類型
            doc_type = DocumentTypeDetector.detect(content, file_name)
            logger.info(f"  識別類型: {doc_type}")
            
            # 選擇對應的解析器
            parser_class = self.parsers.get(doc_type, GenericParser)
            parser = parser_class(content, file_name, doc_type)
            
            # 解析文件
            structured_doc = parser.parse()
            structured_doc.original_doc_id = doc_id
            
            # 補充額外資訊
            if not structured_doc.doc_number and doc_id:
                structured_doc.doc_number = f"AUTO_{doc_id[:8]}"
            
            # 儲存結果
            success = self.db_manager.save_structured_document(structured_doc)
            
            if success:
                logger.info(f"  ✅ 解析成功: {structured_doc.doc_number}")
            else:
                logger.error(f"  ❌ 儲存失敗")
            
            return success
            
        except Exception as e:
            logger.error(f"處理文件失敗: {e}", exc_info=True)
            return False
    
    def process_batch(self):
        """處理一批文件"""
        documents = self.db_manager.get_unprocessed_documents(BATCH_SIZE)
        
        if not documents:
            logger.info("沒有需要處理的文件")
            return
        
        logger.info(f"🔄 開始處理 {len(documents)} 份文件")
        
        success_count = 0
        for doc_data in documents:
            if self.process_document(doc_data):
                success_count += 1
        
        logger.info(f"✨ 批次處理完成: 成功 {success_count}/{len(documents)}")

# ============================================================================
# 主程式
# ============================================================================

def signal_handler(signum, frame):
    """處理中斷訊號"""
    logger.info("收到中斷訊號，正在結束...")
    sys.exit(0)

def main():
    """主函數"""
    logger.info("=" * 60)
    logger.info("🚀 文件結構化解析服務啟動")
    logger.info(f"📊 設定: 掃描間隔={SCAN_INTERVAL}秒, 批次大小={BATCH_SIZE}")
    logger.info("=" * 60)
    
    # 設定訊號處理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 初始化
    db_manager = DatabaseManager()
    processor = DocumentProcessor(db_manager)
    
    # 主迴圈
    while True:
        try:
            processor.process_batch()
            logger.info(f"💤 等待 {SCAN_INTERVAL} 秒...")
            time.sleep(SCAN_INTERVAL)
            
        except KeyboardInterrupt:
            logger.info("收到鍵盤中斷，正在結束...")
            break
        except Exception as e:
            logger.error(f"主迴圈錯誤: {e}", exc_info=True)
            time.sleep(10)  # 錯誤後短暫等待
    
    # 清理
    if db_manager.connection:
        db_manager.connection.close()
    
    logger.info("👋 服務已停止")

if __name__ == "__main__":
    main()
