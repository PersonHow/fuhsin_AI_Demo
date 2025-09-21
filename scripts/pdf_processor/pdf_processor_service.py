#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
PDF 文檔處理服務 - 福興工業技術文件自動化處理系統 
=============================================================================

功能概述：
1. 自動監控指定目錄中的 PDF 檔案
2. 解析福興工業的技術文件（設變通知、DFMEA、規格書、客訴單等）
3. 增強的表格處理能力，專門處理表單型 PDF
4. 提取結構化資料並存入 MySQL 資料庫
5. 支援狀態追蹤，避免重複處理
6. 可選的 OCR 功能處理掃描檔

系統架構：
    PDF檔案 → 監控目錄 → 解析處理 → MySQL → 同步到 ES → RAG檢索

作者: [您的團隊]
版本: 2.0.0
更新日期: 2024
=============================================================================
"""

import os, sys, time, json, signal, hashlib, logging, pymysql, pdfplumber, re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict

# 如果需要 OCR 功能，需要額外安裝這些套件
try:
    import pytesseract
    from pdf2image import convert_from_path
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    logging.warning("OCR 相關套件未安裝，OCR 功能將不可用")

# ============================================================================
# 環境變數配置
# ============================================================================
# MySQL 連線設定
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "fuhsin_erp_demo")

# PDF 檔案目錄結構
PDF_WATCH_DIR = Path(os.getenv("PDF_WATCH_DIR", "/mnt/pdf/incoming"))
PDF_DONE_DIR = PDF_WATCH_DIR / ".done"
PDF_ERROR_DIR = PDF_WATCH_DIR / ".error"
PDF_PROCESSING_DIR = PDF_WATCH_DIR / ".processing"

# 處理參數設定
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "30"))  # 掃描間隔（秒）
PROCESS_BATCH_SIZE = int(os.getenv("PROCESS_BATCH_SIZE", "5"))  # 每批處理檔案數
ENABLE_OCR = os.getenv("ENABLE_OCR", "false").lower() == "true"  # 是否啟用 OCR
OCR_LANG = os.getenv("OCR_LANG", "chi_tra+eng")  # OCR 語言：繁體中文+英文
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"  # 調試模式

# 狀態和日誌檔案路徑
STATE_FILE = Path("/state/.pdf_processor_state.json")
LOG_FILE = Path("/logs/pdf_processor.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ============================================================================
# 日誌設定
# ============================================================================
os.makedirs(LOG_FILE.parent, exist_ok=True)
os.makedirs(STATE_FILE.parent, exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ============================================================================
# 優雅關閉機制
# ============================================================================
should_stop = False

def signal_handler(signum, frame):
    """處理系統中斷信號"""
    global should_stop
    logger.info("🛑 收到中斷信號，準備優雅關閉...")
    should_stop = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ============================================================================
# 資料模型定義
# ============================================================================
@dataclass
class TechnicalDocument:
    """技術文檔資料模型"""
    doc_id: str  # 文檔唯一識別碼（MD5 hash）
    doc_type: str  # 文檔類型
    doc_number: str  # 文檔編號
    title: str  # 文檔標題
    product_ids: List[str]  # 相關產品編號列表
    revision: Optional[str]  # 版本號
    issue_date: Optional[str]  # 發行日期
    author: Optional[str]  # 作者
    content: str  # 文檔全文內容
    summary: str  # 摘要
    keywords: List[str]  # 關鍵字列表
    metadata: Dict  # 其他元資料
    file_name: str  # 原始檔案名稱
    file_size: int  # 檔案大小（bytes）
    page_count: int  # 頁數
    created_at: datetime  # 建立時間
    form_data: Optional[Dict]  # 表單資料（新增：用於存儲表格型PDF的結構化資料）

# ============================================================================
# 資料庫管理器
# ============================================================================
class DatabaseManager:
    """資料庫操作管理器"""
    
    def __init__(self):
        self.connection = None
        self.connect()
        self.init_database()  # 初始化資料庫表格

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
    
    def init_database(self):
        """
        初始化資料庫表格
        
        檢查必要的表格是否存在，如果不存在則建立
        這包含兩個主要表格：
        1. technical_documents - 儲存技術文件主要內容
        2. pdf_processing_log - 記錄處理歷程
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # 建立 technical_documents 表格
                create_documents_table_sql = """
                CREATE TABLE IF NOT EXISTS technical_documents (
                    -- 主鍵與基本識別
                    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主鍵',
                    doc_id VARCHAR(32) UNIQUE NOT NULL COMMENT '文檔唯一識別碼 (MD5 hash)',
                    
                    -- 文檔分類資訊
                    doc_type VARCHAR(50) COMMENT '文檔類型 (ECN/DFMEA/SPEC/DRAWING/COMPLAINT/REPORT/OTHER)',
                    doc_number VARCHAR(200) COMMENT '文檔編號',
                    title VARCHAR(500) COMMENT '文檔標題',
                    
                    -- 產品與版本資訊
                    product_ids JSON COMMENT '相關產品編號列表 (JSON陣列)',
                    revision VARCHAR(50) COMMENT '版本號',
                    issue_date VARCHAR(50) COMMENT '發行日期',
                    author VARCHAR(200) COMMENT '作者/申請人',
                    
                    -- 文檔內容
                    content LONGTEXT COMMENT '文檔全文內容',
                    summary TEXT COMMENT '摘要 (最多500字)',
                    keywords JSON COMMENT '關鍵字列表 (JSON陣列)',
                    metadata JSON COMMENT '其他元資料 (JSON物件)',
                    form_data JSON COMMENT '表單結構化資料 (針對表格型PDF)',
                    
                    -- 檔案資訊
                    file_name VARCHAR(255) NOT NULL COMMENT '原始檔案名稱',
                    file_size INT COMMENT '檔案大小 (bytes)',
                    page_count INT COMMENT '頁數',
                    
                    -- 時間戳記
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間',
                    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新時間',
                    
                    -- 索引定義
                    INDEX idx_doc_type (doc_type),
                    INDEX idx_doc_number (doc_number),
                    INDEX idx_issue_date (issue_date),
                    INDEX idx_created_at (created_at),
                    FULLTEXT idx_content (content),
                    FULLTEXT idx_summary (summary)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                COMMENT='技術文件主表 - 儲存福興工業的所有技術文件';
                """
                
                cursor.execute(create_documents_table_sql)
                logger.info("✅ 確認表格存在: technical_documents")
                
                # 建立 pdf_processing_log 表格
                create_log_table_sql = """
                CREATE TABLE IF NOT EXISTS pdf_processing_log (
                    -- 主鍵
                    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主鍵',
                    
                    -- 檔案資訊
                    file_name VARCHAR(255) NOT NULL COMMENT '檔案名稱',
                    file_hash VARCHAR(32) COMMENT '檔案雜湊值 (MD5)',
                    
                    -- 處理狀態
                    status ENUM('processing', 'success', 'error', 'skipped') NOT NULL COMMENT '處理狀態',
                    error_message TEXT COMMENT '錯誤訊息 (僅在狀態為error時)',
                    process_time_ms INT COMMENT '處理時間 (毫秒)',
                    
                    -- 時間戳記
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '記錄建立時間',
                    
                    -- 索引
                    INDEX idx_file_name (file_name),
                    INDEX idx_file_hash (file_hash),
                    INDEX idx_status (status),
                    INDEX idx_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                COMMENT='PDF處理日誌表 - 記錄所有PDF檔案的處理歷程';
                """
                
                cursor.execute(create_log_table_sql)
                logger.info("✅ 確認表格存在: pdf_processing_log")
                
                # 建立產品編號對照表（可選，用於快速查詢）
                create_product_mapping_table_sql = """
                CREATE TABLE IF NOT EXISTS product_document_mapping (
                    -- 主鍵
                    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主鍵',
                    
                    -- 關聯資訊
                    product_id VARCHAR(100) NOT NULL COMMENT '產品編號',
                    doc_id VARCHAR(32) NOT NULL COMMENT '文檔ID (關聯到technical_documents.doc_id)',
                    
                    -- 時間戳記
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間',
                    
                    -- 索引與約束
                    INDEX idx_product_id (product_id),
                    INDEX idx_doc_id (doc_id),
                    UNIQUE KEY unique_product_doc (product_id, doc_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                COMMENT='產品文件對照表 - 用於快速查詢特定產品的相關文件';
                """
                
                cursor.execute(create_product_mapping_table_sql)
                logger.info("✅ 確認表格存在: product_document_mapping")
                
                # 建立文件關鍵字索引表（可選，用於改善搜尋效能）
                create_keyword_index_table_sql = """
                CREATE TABLE IF NOT EXISTS document_keywords (
                    -- 主鍵
                    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主鍵',
                    
                    -- 關鍵字資訊
                    keyword VARCHAR(100) NOT NULL COMMENT '關鍵字',
                    doc_id VARCHAR(32) NOT NULL COMMENT '文檔ID',
                    frequency INT DEFAULT 1 COMMENT '關鍵字出現頻率',
                    
                    -- 時間戳記
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '建立時間',
                    
                    -- 索引與約束
                    INDEX idx_keyword (keyword),
                    INDEX idx_doc_id (doc_id),
                    UNIQUE KEY unique_keyword_doc (keyword, doc_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                COMMENT='文件關鍵字索引表 - 用於加速關鍵字搜尋';
                """
                
                cursor.execute(create_keyword_index_table_sql)
                logger.info("✅ 確認表格存在: document_keywords")
                
                conn.commit()
                
                # 顯示表格統計資訊
                self._show_table_stats(cursor)
                
        except Exception as e:
            logger.error(f"初始化資料庫表格失敗: {e}")
            raise
    
    def _show_table_stats(self, cursor):
        """
        顯示表格統計資訊
        
        在初始化後顯示各表格的記錄數量
        """
        try:
            tables = ['technical_documents', 'pdf_processing_log', 
                     'product_document_mapping', 'document_keywords']
            
            logger.info("="*60)
            logger.info("📊 資料庫表格統計")
            logger.info("="*60)
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                result = cursor.fetchone()
                count = result['count'] if result else 0
                logger.info(f"  {table}: {count} 筆記錄")
            
            logger.info("="*60)
            
        except Exception as e:
            logger.debug(f"無法取得表格統計: {e}")
    
    def drop_tables_if_needed(self, confirm=False):
        """
        刪除所有相關表格（危險操作，需要確認）
        
        Args:
            confirm: 必須設為 True 才會執行刪除
        
        使用方式：
            db = DatabaseManager()
            db.drop_tables_if_needed(confirm=True)
        """
        if not confirm:
            logger.warning("⚠️ 刪除表格需要明確確認 (confirm=True)")
            return
        
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                tables = [
                    'document_keywords',
                    'product_document_mapping', 
                    'pdf_processing_log',
                    'technical_documents'
                ]
                
                for table in tables:
                    sql = f"DROP TABLE IF EXISTS {table}"
                    cursor.execute(sql)
                    logger.warning(f"🗑️ 已刪除表格: {table}")
                
                conn.commit()
                logger.warning("⚠️ 所有表格已刪除，將在下次連線時重新建立")
                
        except Exception as e:
            logger.error(f"刪除表格失敗: {e}")

    def get_connection(self):
        """取得資料庫連線，必要時重新連線"""
        try:
            if not self.connection or not self.connection.ping(reconnect=False):
                logger.warning("資料庫連線已斷開，嘗試重新連線...")
                self.connect()
        except:
            self.connect()
        return self.connection

    def save_document(self, doc: TechnicalDocument) -> bool:
        """
        儲存文檔到資料庫
        
        同時更新相關的對照表
        """
        sql = """
        INSERT INTO technical_documents 
        (doc_id, doc_type, doc_number, title, product_ids, revision,
         issue_date, author, content, summary, keywords, metadata,
         file_name, file_size, page_count, form_data)
        VALUES (%(doc_id)s, %(doc_type)s, %(doc_number)s, %(title)s, 
                %(product_ids)s, %(revision)s, %(issue_date)s, %(author)s,
                %(content)s, %(summary)s, %(keywords)s, %(metadata)s,
                %(file_name)s, %(file_size)s, %(page_count)s, %(form_data)s)
        ON DUPLICATE KEY UPDATE
            doc_type = VALUES(doc_type),
            doc_number = VALUES(doc_number),
            title = VALUES(title),
            product_ids = VALUES(product_ids),
            revision = VALUES(revision),
            issue_date = VALUES(issue_date),
            author = VALUES(author),
            content = VALUES(content),
            summary = VALUES(summary),
            keywords = VALUES(keywords),
            metadata = VALUES(metadata),
            file_name = VALUES(file_name),
            file_size = VALUES(file_size),
            page_count = VALUES(page_count),
            form_data = VALUES(form_data)
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # 準備資料
                doc_dict = asdict(doc)
                
                # 保存原始的列表和字典，用於後續處理
                original_product_ids = doc.product_ids
                original_keywords = doc.keywords
                
                # 將列表和字典轉換為 JSON 字串
                doc_dict["product_ids"] = json.dumps(doc.product_ids, ensure_ascii=False)
                doc_dict["keywords"] = json.dumps(doc.keywords, ensure_ascii=False)
                doc_dict["metadata"] = json.dumps(doc.metadata, ensure_ascii=False)
                doc_dict["form_data"] = json.dumps(doc.form_data, ensure_ascii=False) if doc.form_data else None
                
                # 移除 created_at，讓資料庫使用預設值
                doc_dict.pop('created_at', None)
                
                # 執行主表插入
                cursor.execute(sql, doc_dict)
                
                # 更新產品文件對照表
                self._update_product_mapping(cursor, doc.doc_id, original_product_ids)
                
                # 更新關鍵字索引表
                self._update_keyword_index(cursor, doc.doc_id, original_keywords)
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"資料庫寫入錯誤: {e}")
            if conn:
                conn.rollback()
            return False
    
    def _update_product_mapping(self, cursor, doc_id: str, product_ids: List[str]):
        """
        更新產品文件對照表
        
        用於建立產品編號與文件的關聯
        """
        try:
            # 先刪除舊的對照
            cursor.execute(
                "DELETE FROM product_document_mapping WHERE doc_id = %s",
                (doc_id,)
            )
            
            # 插入新的對照
            if product_ids:
                values = [(product_id, doc_id) for product_id in product_ids]
                cursor.executemany(
                    "INSERT IGNORE INTO product_document_mapping (product_id, doc_id) VALUES (%s, %s)",
                    values
                )
                
        except Exception as e:
            logger.debug(f"更新產品對照表失敗: {e}")
    
    def _update_keyword_index(self, cursor, doc_id: str, keywords: List[str]):
        """
        更新關鍵字索引表
        
        用於加速關鍵字搜尋
        """
        try:
            # 先刪除舊的索引
            cursor.execute(
                "DELETE FROM document_keywords WHERE doc_id = %s",
                (doc_id,)
            )
            
            # 計算關鍵字頻率
            keyword_freq = {}
            for keyword in keywords:
                keyword_freq[keyword] = keyword_freq.get(keyword, 0) + 1
            
            # 插入新的索引
            if keyword_freq:
                values = [(keyword, doc_id, freq) 
                         for keyword, freq in keyword_freq.items()]
                cursor.executemany(
                    "INSERT IGNORE INTO document_keywords (keyword, doc_id, frequency) VALUES (%s, %s, %s)",
                    values
                )
                
        except Exception as e:
            logger.debug(f"更新關鍵字索引失敗: {e}")
    
    def check_document_exists(self, doc_id: str) -> bool:
        """
        檢查文檔是否已存在
        
        Args:
            doc_id: 文檔ID (MD5 hash)
            
        Returns:
            bool: 是否存在
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM technical_documents WHERE doc_id = %s",
                    (doc_id,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"檢查文檔存在性失敗: {e}")
            return False
    
    def get_statistics(self) -> Dict:
        """
        取得處理統計資訊
        
        Returns:
            包含各種統計數據的字典
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                stats = {}
                
                # 文檔總數
                cursor.execute("SELECT COUNT(*) as total FROM technical_documents")
                stats['total_documents'] = cursor.fetchone()['total']
                
                # 各類型文檔數量
                cursor.execute("""
                    SELECT doc_type, COUNT(*) as count 
                    FROM technical_documents 
                    GROUP BY doc_type
                """)
                stats['documents_by_type'] = {row['doc_type']: row['count'] 
                                            for row in cursor.fetchall()}
                
                # 今日處理數量
                cursor.execute("""
                    SELECT COUNT(*) as today_count 
                    FROM pdf_processing_log 
                    WHERE DATE(created_at) = CURDATE()
                """)
                stats['today_processed'] = cursor.fetchone()['today_count']
                
                # 錯誤統計
                cursor.execute("""
                    SELECT COUNT(*) as error_count 
                    FROM pdf_processing_log 
                    WHERE status = 'error' 
                    AND DATE(created_at) = CURDATE()
                """)
                stats['today_errors'] = cursor.fetchone()['error_count']
                
                return stats
                
        except Exception as e:
            logger.error(f"取得統計資訊失敗: {e}")
            return {}

    def log_processing(self, file_name: str, file_hash: str, status: str,
                        error_message: str = None, process_time_ms: int = 0):
        """記錄處理狀態到日誌表"""
        sql = """
        INSERT INTO pdf_processing_log 
        (file_name, file_hash, status, error_message, process_time_ms)
        VALUES (%s, %s, %s, %s, %s)
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(sql, (file_name, file_hash, status, error_message, process_time_ms))
                conn.commit()
        except Exception as e:
            logger.error(f"記錄處理日誌失敗: {e}")

    def close(self):
        """關閉資料庫連線"""
        if self.connection:
            self.connection.close()
            logger.info("資料庫連線已關閉")

# ============================================================================
# 增強版 PDF 解析器
# ============================================================================
class EnhancedPDFParser:
    """
    增強版 PDF 文件解析器
    
    主要改進：
    1. 改進的表格處理能力
    2. 多策略文字提取
    3. 表單資料結構化提取
    4. OCR 備援機制
    5. PDF 類型自動偵測
    """
    
    # 產品編號的正則表達式模式
    PRODUCT_PATTERNS = [
        r"OB\d-[A-Z0-9]+",
        r"[FG]\d{2}-[A-Z0-9]+",
        r"[PW]\d{3}",
        r"EC-K-\d{2}-[A-Z]-\d{3}",
        r"L\d{6}[A-Z]?\d?",
        r"[A-Z]{2,3}-\d{4,6}",
    ]
    
    # 文檔類型關鍵字
    DOC_TYPE_KEYWORDS = {
        "ECN": ["設變", "工程變更", "設計變更", "ECN", "設變通知", "設變單"],
        "DFMEA": ["DFMEA", "失效模式", "失效分析", "風險分析"],
        "SPEC": ["規格", "規格書", "specification", "spec"],
        "DRAWING": ["圖面", "圖紙", "工程圖", "drawing"],
        "COMPLAINT": ["客訴", "客戶投訴", "客戶抱怨", "complaint", "客訴單"],
        "REPORT": ["報告", "測試報告", "檢驗報告", "report"],
    }
    
    @classmethod
    def detect_pdf_type(cls, pdf_path: Path) -> str:
        """
        偵測 PDF 類型以採用不同處理策略
        
        返回值：
        - SCANNED: 掃描檔，需要 OCR
        - TABLE_FORM: 表單型，需要表格提取
        - MIXED: 混合型，有文字也有表格
        - TEXT: 純文字型
        """
        try:
            with pdfplumber.open(pdf_path) as pdf:
                if not pdf.pages:
                    return "EMPTY"
                
                first_page = pdf.pages[0]
                
                # 檢查是否有文字層
                text = first_page.extract_text()
                has_text = bool(text and len(text.strip()) > 50)
                
                # 檢查是否有表格
                tables = first_page.extract_tables()
                has_tables = bool(tables)
                
                # 檢查是否為掃描件（通常只有圖片）
                has_images = bool(first_page.images)
                
                # 檢查是否有大量的管道符號（表格殘留）
                has_pipe_chars = text.count('|') > 20 if text else False
                
                if not has_text and has_images:
                    logger.info(f"PDF 類型: SCANNED（掃描檔）")
                    return "SCANNED"
                elif has_tables or has_pipe_chars:
                    logger.info(f"PDF 類型: TABLE_FORM（表單型）")
                    return "TABLE_FORM"
                elif has_tables and has_text:
                    logger.info(f"PDF 類型: MIXED（混合型）")
                    return "MIXED"
                else:
                    logger.info(f"PDF 類型: TEXT（純文字型）")
                    return "TEXT"
                    
        except Exception as e:
            logger.error(f"偵測 PDF 類型失敗: {e}")
            return "UNKNOWN"
    
    @classmethod
    def extract_text_enhanced(cls, pdf_path: Path) -> Tuple[str, int]:
        """
        增強版文字提取方法
        
        使用多種策略提取文字：
        1. 表格優先策略
        2. 一般文字提取
        3. 字元級別提取
        4. OCR 備援
        """
        pdf_type = cls.detect_pdf_type(pdf_path)
        text_parts = []
        page_count = 0
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    page_count += 1
                    page_text = ""
                    
                    # 根據 PDF 類型選擇提取策略
                    if pdf_type in ["TABLE_FORM", "MIXED"]:
                        # 策略1: 優先提取表格內容
                        page_text = cls._extract_table_text(page)
                    
                    # 策略2: 提取一般文字
                    if not page_text:
                        page_text = cls._extract_regular_text(page)
                    
                    # 策略3: 字元級別提取（最後手段）
                    if not page_text and page.chars:
                        page_text = cls._extract_char_level_text(page)
                    
                    if page_text:
                        text_parts.append(f"[第 {page_num + 1} 頁]\n{page_text}")
                    
                    if DEBUG_MODE and page_num == 0:
                        logger.debug(f"第一頁文字預覽: {page_text[:200]}")
            
            combined_text = '\n'.join(text_parts)
            
            # 如果提取失敗且啟用了 OCR，使用 OCR 處理
            if (not combined_text.strip() or len(combined_text) < 50) and ENABLE_OCR:
                logger.warning(f"文字提取結果過少，嘗試 OCR: {pdf_path.name}")
                combined_text = cls._perform_ocr(pdf_path)
            
            # 清理文字
            combined_text = cls._clean_extracted_text(combined_text)
            
            return combined_text, page_count
            
        except Exception as e:
            logger.error(f"文字提取失敗: {e}")
            return "", 0
    
    @classmethod
    def _extract_table_text(cls, page) -> str:
        """
        提取表格內的文字
        
        專門處理表格型 PDF，如設變單、客訴單等
        """
        text_parts = []
        
        try:
            # 使用更細緻的表格提取設定
            table_settings = {
                "vertical_strategy": "lines",     # 使用線條偵測垂直邊界
                "horizontal_strategy": "lines",   # 使用線條偵測水平邊界
                "snap_tolerance": 3,              # 線條對齊容差
                "join_tolerance": 3,              # 線條連接容差
                "edge_min_length": 3,             # 最小邊緣長度
                "min_words_vertical": 1,          # 垂直最少字數
                "min_words_horizontal": 1,        # 水平最少字數
                "text_tolerance": 3,              # 文字對齊容差
                "intersection_tolerance": 3,      # 交叉點容差
            }
            
            tables = page.extract_tables(table_settings=table_settings)
            
            if tables:
                logger.debug(f"找到 {len(tables)} 個表格")
                
                for table_idx, table in enumerate(tables):
                    if not table:
                        continue
                    
                    # 處理每個表格
                    for row_idx, row in enumerate(table):
                        if not row:
                            continue
                        
                        # 過濾並清理每個儲存格
                        cleaned_cells = []
                        for cell in row:
                            if cell is not None:
                                # 移除純管道符號
                                cell_text = str(cell).strip()
                                if cell_text and cell_text != '|' and not all(c == '|' for c in cell_text):
                                    cleaned_cells.append(cell_text)
                        
                        # 組合非空儲存格
                        if cleaned_cells:
                            row_text = ' | '.join(cleaned_cells)
                            text_parts.append(row_text)
                            
                            if DEBUG_MODE and table_idx == 0 and row_idx < 5:
                                logger.debug(f"表格行 {row_idx}: {row_text}")
        
        except Exception as e:
            logger.error(f"表格提取失敗: {e}")
        
        return '\n'.join(text_parts)
    
    @classmethod
    def _extract_regular_text(cls, page) -> str:
        """提取一般文字"""
        try:
            text = page.extract_text()
            if text:
                # 移除多餘的管道符號
                text = re.sub(r'\|{2,}', ' ', text)
                text = re.sub(r'(?:^|\n)\|+(?:$|\n)', '\n', text)
                return text.strip()
        except Exception as e:
            logger.error(f"一般文字提取失敗: {e}")
        return ""
    
    @classmethod
    def _extract_char_level_text(cls, page) -> str:
        """字元級別文字提取"""
        try:
            chars = page.chars
            if chars:
                # 按位置排序字元
                sorted_chars = sorted(chars, key=lambda x: (x['top'], x['x0']))
                
                # 組合字元，考慮行間距
                lines = []
                current_line = []
                last_top = None
                line_threshold = 3  # 行間距閾值
                
                for char in sorted_chars:
                    if last_top is None or abs(char['top'] - last_top) < line_threshold:
                        current_line.append(char['text'])
                    else:
                        lines.append(''.join(current_line))
                        current_line = [char['text']]
                    last_top = char['top']
                
                if current_line:
                    lines.append(''.join(current_line))
                
                return '\n'.join(lines)
                
        except Exception as e:
            logger.error(f"字元級別提取失敗: {e}")
        return ""
    
    @classmethod
    def _perform_ocr(cls, pdf_path: Path) -> str:
        """
        使用 OCR 處理無法提取文字的 PDF
        
        需要安裝：
        - pytesseract
        - pdf2image
        - tesseract-ocr (系統層級)
        """
        if not OCR_AVAILABLE:
            logger.warning("OCR 套件未安裝，跳過 OCR 處理")
            return ""
        
        try:
            logger.info(f"開始 OCR 處理: {pdf_path.name}")
            
            # 將 PDF 轉換為圖片
            images = convert_from_path(pdf_path, dpi=300)
            
            text_parts = []
            for i, image in enumerate(images):
                # 執行 OCR
                text = pytesseract.image_to_string(
                    image,
                    lang=OCR_LANG,  # 使用設定的語言
                    config='--psm 6'  # 假設為統一的文字區塊
                )
                text_parts.append(f"[OCR 第 {i+1} 頁]\n{text}")
                logger.info(f"OCR 處理第 {i+1}/{len(images)} 頁完成")
            
            return '\n'.join(text_parts)
            
        except Exception as e:
            logger.error(f"OCR 處理失敗: {e}")
            return ""
    
    @classmethod
    def _clean_extracted_text(cls, text: str) -> str:
        """
        清理提取的文字
        
        移除多餘的符號、空白等
        """
        if not text:
            return ""
        
        # 移除連續的管道符號
        text = re.sub(r'\|{3,}', '', text)
        
        # 移除只有管道符號的行
        lines = text.split('\n')
        cleaned_lines = []
        for line in lines:
            # 移除只有管道和空白的行
            if line.strip() and not all(c in '| \t' for c in line):
                cleaned_lines.append(line)
        
        text = '\n'.join(cleaned_lines)
        
        # 移除多餘的空白
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {3,}', '  ', text)
        
        return text.strip()
    
    @classmethod
    def extract_form_data(cls, pdf_path: Path) -> Dict:
        """
        提取表單資料
        
        專門處理表單類 PDF（設變單、客訴單等）
        將表格轉換為結構化的鍵值對
        """
        form_data = {}
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    
                    for table in tables:
                        if not table:
                            continue
                        
                        for row in table:
                            if not row or len(row) < 2:
                                continue
                            
                            # 嘗試將表格解析為鍵值對
                            # 通常第一欄是標籤，第二欄是值
                            key = str(row[0]).strip() if row[0] else ""
                            value = str(row[1]).strip() if row[1] else ""
                            
                            # 清理鍵名
                            key = re.sub(r'[：:]', '', key)
                            key = key.replace(' ', '_')
                            
                            # 只保存有意義的資料
                            if key and value and key != '|' and value != '|':
                                # 如果鍵已存在，將值組合成列表
                                if key in form_data:
                                    if isinstance(form_data[key], list):
                                        form_data[key].append(value)
                                    else:
                                        form_data[key] = [form_data[key], value]
                                else:
                                    form_data[key] = value
                            
                            # 如果有更多欄位，也嘗試配對
                            if len(row) > 3:
                                for i in range(2, len(row) - 1, 2):
                                    if i + 1 < len(row):
                                        sub_key = str(row[i]).strip() if row[i] else ""
                                        sub_value = str(row[i + 1]).strip() if row[i + 1] else ""
                                        
                                        sub_key = re.sub(r'[：:]', '', sub_key).replace(' ', '_')
                                        
                                        if sub_key and sub_value and sub_key != '|' and sub_value != '|':
                                            form_data[sub_key] = sub_value
        
        except Exception as e:
            logger.error(f"表單資料提取失敗: {e}")
        
        if DEBUG_MODE and form_data:
            logger.debug(f"提取的表單資料: {list(form_data.keys())[:10]}")
        
        return form_data
    
    @classmethod
    def detect_doc_type(cls, text: str, file_name: str) -> str:
        """偵測文檔類型"""
        text_lower = text.lower()
        file_name_lower = file_name.lower()
        
        for doc_type, keywords in cls.DOC_TYPE_KEYWORDS.items():
            for keyword in keywords:
                if keyword.lower() in text_lower or keyword.lower() in file_name_lower:
                    logger.info(f"偵測到文檔類型: {doc_type}")
                    return doc_type
        
        return "OTHER"
    
    @classmethod
    def extract_product_ids(cls, text: str) -> List[str]:
        """提取產品編號"""
        product_ids = set()
        
        for pattern in cls.PRODUCT_PATTERNS:
            matches = re.findall(pattern, text)
            product_ids.update(matches)
        
        return list(product_ids)
    
    @classmethod
    def extract_metadata(cls, text: str, doc_type: str, form_data: Dict = None) -> Dict:
        """
        提取元資料
        
        結合文字內容和表單資料提取元資料
        """
        metadata = {}
        
        # 從表單資料提取
        if form_data:
            # 常見的表單欄位映射
            field_mappings = {
                '版本': 'revision',
                '版次': 'revision',
                'Rev': 'revision',
                '發行日期': 'issue_date',
                '日期': 'issue_date',
                'Date': 'issue_date',
                '作者': 'author',
                '簽核人': 'author',
                '申請人': 'applicant',
                '客戶': 'customer',
                '客戶名稱': 'customer',
                '部門': 'department',
                '單位': 'department',
            }
            
            for field_name, meta_key in field_mappings.items():
                if field_name in form_data:
                    metadata[meta_key] = form_data[field_name]
        
        # 從文字內容提取（如果表單資料沒有）
        if 'revision' not in metadata:
            revision_match = re.search(r'Rev[.:：\s]*([A-Z0-9]+)', text, re.IGNORECASE)
            if revision_match:
                metadata['revision'] = revision_match.group(1)
        
        if 'issue_date' not in metadata:
            date_match = re.search(r'(\d{4}[-/]\d{1,2}[-/]\d{1,2})', text)
            if date_match:
                metadata['issue_date'] = date_match.group(1)
        
        # 根據文檔類型添加特定元資料
        if doc_type == "ECN":
            metadata['change_type'] = cls._extract_change_type(text)
        elif doc_type == "COMPLAINT":
            metadata['severity'] = cls._extract_severity(text)
        
        return metadata
    
    @classmethod
    def _extract_change_type(cls, text: str) -> str:
        """提取設變類型"""
        if "緊急" in text:
            return "緊急"
        elif "一般" in text:
            return "一般"
        return "未分類"
    
    @classmethod
    def _extract_severity(cls, text: str) -> str:
        """提取客訴嚴重度"""
        if "嚴重" in text or "critical" in text.lower():
            return "嚴重"
        elif "中等" in text or "moderate" in text.lower():
            return "中等"
        return "一般"
    
    @classmethod
    def extract_keywords(cls, text: str, product_ids: List[str]) -> List[str]:
        """提取關鍵字"""
        keywords = set(product_ids)
        
        # 技術關鍵字
        tech_keywords = [
            "品質", "改善", "不良", "異常", "規格", "測試", "檢驗",
            "material", "process", "quality", "defect", "improvement"
        ]
        
        for keyword in tech_keywords:
            if keyword in text.lower():
                keywords.add(keyword)
        
        # 提取數字相關的關鍵字（可能是規格值）
        spec_values = re.findall(r'\d+\.?\d*\s*(?:mm|cm|kg|g|°C|℃)', text)
        keywords.update(spec_values[:5])  # 限制數量
        
        return list(keywords)[:20]  # 限制總數
    
    @classmethod
    def generate_summary(cls, text: str, form_data: Dict = None, max_length: int = 500) -> str:
        """
        生成摘要
        
        優先使用表單資料生成結構化摘要
        """
        summary_parts = []
        
        # 如果有表單資料，生成結構化摘要
        if form_data:
            important_fields = ['客戶', '客戶名稱', '問題描述', '不良現象', '改善對策', '原因分析']
            for field in important_fields:
                if field in form_data:
                    value = form_data[field]
                    if isinstance(value, list):
                        value = ', '.join(value)
                    summary_parts.append(f"{field}: {value[:100]}")
        
        # 如果摘要不夠，從文字提取
        if len(' '.join(summary_parts)) < 100:
            # 尋找重要句子
            important_patterns = [
                r"[^。！？\n]*(?:問題|不良|異常|原因)[^。！？\n]*",
                r"[^。！？\n]*(?:改善|對策|解決|處理)[^。！？\n]*",
                r"[^。！？\n]*(?:結果|效果|結論)[^。！？\n]*",
            ]
            
            for pattern in important_patterns:
                matches = re.findall(pattern, text)
                summary_parts.extend(matches[:2])
        
        # 組合摘要
        if summary_parts:
            summary = "。".join(summary_parts)[:max_length]
        else:
            # 沒有找到重要句子，取前段文字
            summary = text[:max_length]
        
        if len(summary) == max_length:
            summary += "..."
        
        return summary
    
    @classmethod
    def debug_pdf_structure(cls, pdf_path: Path):
        """
        調試工具：分析 PDF 結構
        
        用於診斷 PDF 解析問題
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"PDF 結構分析: {pdf_path.name}")
        logger.info(f"{'='*60}")
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                logger.info(f"總頁數: {len(pdf.pages)}")
                
                for i, page in enumerate(pdf.pages[:3]):  # 只分析前3頁
                    logger.info(f"\n--- 第 {i+1} 頁分析 ---")
                    
                    # 基本資訊
                    logger.info(f"頁面尺寸: {page.width} x {page.height}")
                    logger.info(f"字元數: {len(page.chars) if page.chars else 0}")
                    logger.info(f"圖片數: {len(page.images)}")
                    
                    # 表格分析
                    tables = page.extract_tables()
                    logger.info(f"表格數: {len(tables)}")
                    if tables:
                        for j, table in enumerate(tables[:2]):
                            if table:
                                logger.info(f"  表格 {j+1}: {len(table)} 行 x {len(table[0]) if table[0] else 0} 列")
                                # 顯示前3行
                                for row_idx, row in enumerate(table[:3]):
                                    logger.info(f"    行 {row_idx+1}: {row[:5] if row else 'Empty'}")
                    
                    # 文字預覽
                    text = page.extract_text()
                    if text:
                        # 顯示前200字
                        preview = text[:200].replace('\n', ' ')
                        logger.info(f"文字預覽: {preview}...")
                        
                        # 檢查特殊字符
                        pipe_count = text.count('|')
                        if pipe_count > 10:
                            logger.warning(f"發現大量管道符號 ({pipe_count} 個)，可能是表格解析問題")
                    else:
                        logger.warning("無法提取文字內容")
                    
                    # 檢查是否需要 OCR
                    if not text and page.images:
                        logger.warning("可能是掃描檔，建議啟用 OCR")
                        
        except Exception as e:
            logger.error(f"PDF 結構分析失敗: {e}")

# ============================================================================
# 主處理服務（整合增強功能）
# ============================================================================
class PDFProcessorService:
    """PDF 處理服務主類別"""
    
    def __init__(self):
        """初始化服務"""
        self.db = DatabaseManager()
        self.parser = EnhancedPDFParser()  # 使用增強版解析器
        self.state = self.load_state()
        self.setup_directories()
        logger.info("PDF 處理服務初始化完成（增強版）")
        
        # 在 DEBUG 模式下顯示配置
        if DEBUG_MODE:
            logger.debug(f"OCR 狀態: {'啟用' if ENABLE_OCR else '停用'}")
            logger.debug(f"OCR 可用: {'是' if OCR_AVAILABLE else '否'}")

    def setup_directories(self):
        """建立必要的目錄結構"""
        for dir_path in [PDF_WATCH_DIR, PDF_DONE_DIR, PDF_ERROR_DIR, PDF_PROCESSING_DIR]:
            dir_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"確認目錄存在: {dir_path}")

    def load_state(self) -> Dict:
        """載入處理狀態"""
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                logger.info(f"載入狀態檔案，已處理 {len(state)} 個檔案")
                return state
            except Exception as e:
                logger.error(f"載入狀態失敗: {e}")
        return {}

    def save_state(self):
        """儲存處理狀態"""
        try:
            STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
            logger.debug("狀態已儲存")
        except Exception as e:
            logger.error(f"儲存狀態失敗: {e}")

    def get_file_hash(self, file_path: Path) -> str:
        """計算檔案的 MD5 雜湊值"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def process_pdf(self, pdf_path: Path) -> bool:
        """
        處理單個 PDF 檔案（使用增強功能）
        """
        start_time = time.time()
        file_hash = self.get_file_hash(pdf_path)
        file_size = pdf_path.stat().st_size

        try:
            logger.info(f"📄 開始處理: {pdf_path.name} ({file_size/1024:.1f} KB)")
            
            # 記錄開始處理
            self.db.log_processing(pdf_path.name, file_hash, "processing")
            
            # 如果是 DEBUG 模式，先分析 PDF 結構
            if DEBUG_MODE:
                self.parser.debug_pdf_structure(pdf_path)
            
            # 移到處理中目錄
            processing_path = PDF_PROCESSING_DIR / pdf_path.name
            pdf_path.rename(processing_path)
            logger.debug(f"檔案移至處理中: {processing_path}")
            
            # 使用增強版文字提取
            text, page_count = self.parser.extract_text_enhanced(processing_path)
            
            if not text:
                raise ValueError("無法提取文字內容")
            
            logger.info(f"  提取文字: {len(text)} 字元, {page_count} 頁")
            
            # 提取表單資料（新增功能）
            form_data = None
            pdf_type = self.parser.detect_pdf_type(processing_path)
            if pdf_type in ["TABLE_FORM", "MIXED"]:
                form_data = self.parser.extract_form_data(processing_path)
                if form_data:
                    logger.info(f"  提取表單欄位: {len(form_data)} 個")
            
            # 解析文檔資訊
            doc_type = self.parser.detect_doc_type(text, processing_path.stem)
            product_ids = self.parser.extract_product_ids(text)
            metadata = self.parser.extract_metadata(text, doc_type, form_data)
            keywords = self.parser.extract_keywords(text, product_ids)
            summary = self.parser.generate_summary(text, form_data)
            
            # 從檔名提取文檔編號
            doc_number_match = re.search(
                r"[A-Z]{2,}-[A-Z]-\d{2}-[A-Z]-\d{3}|L\d{6}[A-Z]?\d?",
                processing_path.stem
            )
            doc_number = doc_number_match.group(0) if doc_number_match else processing_path.stem[:50]
            
            # 建立文檔物件
            doc = TechnicalDocument(
                doc_id=file_hash,
                doc_type=doc_type,
                doc_number=doc_number,
                title=processing_path.stem[:200],
                product_ids=product_ids,
                revision=metadata.get('revision'),
                issue_date=metadata.get('issue_date'),
                author=metadata.get('author'),
                content=text,
                summary=summary,
                keywords=keywords,
                metadata=metadata,
                file_name=processing_path.name,
                file_size=file_size,
                page_count=page_count,
                created_at=datetime.now(),
                form_data=form_data  # 新增：儲存表單資料
            )
            
            # 存入資料庫
            if self.db.save_document(doc):
                # 成功：移到完成目錄
                done_path = PDF_DONE_DIR / processing_path.name
                processing_path.rename(done_path)
                logger.debug(f"檔案移至完成目錄: {done_path}")
                
                process_time = int((time.time() - start_time) * 1000)
                self.db.log_processing(
                    pdf_path.name,
                    file_hash,
                    "success",
                    process_time_ms=process_time
                )
                
                # 更新狀態
                self.state[pdf_path.name] = {
                    "hash": file_hash,
                    "processed_at": datetime.now().isoformat(),
                    "doc_id": doc.doc_id,
                    "status": "success",
                    "page_count": page_count,
                    "doc_type": doc_type,
                    "product_count": len(product_ids),
                    "has_form_data": bool(form_data)  # 記錄是否有表單資料
                }
                self.save_state()
                
                logger.info(f"  ✅ 成功處理，耗時: {process_time}ms")
                return True
            else:
                raise Exception("資料庫寫入失敗")
                
        except Exception as e:
            # 失敗：移到錯誤目錄
            error_msg = str(e)
            logger.error(f"  ❌ 處理失敗: {error_msg}")
            
            # 確保檔案在處理中目錄
            if processing_path.exists():
                error_path = PDF_ERROR_DIR / pdf_path.name
                processing_path.rename(error_path)
                logger.debug(f"檔案移至錯誤目錄: {error_path}")
            
            process_time = int((time.time() - start_time) * 1000)
            self.db.log_processing(
                pdf_path.name,
                file_hash,
                "error",
                error_message=error_msg,
                process_time_ms=process_time
            )
            
            # 記錄錯誤狀態
            self.state[pdf_path.name] = {
                "hash": file_hash,
                "processed_at": datetime.now().isoformat(),
                "status": "error",
                "error": error_msg
            }
            self.save_state()
            return False

    def scan_and_process(self):
        """掃描監控目錄並處理 PDF 檔案"""
        pdf_files = sorted(PDF_WATCH_DIR.glob("*.pdf"))
        
        if not pdf_files:
            return 0
        
        logger.info(f"🔍 發現 {len(pdf_files)} 個待處理檔案")
        
        processed = 0
        for pdf_path in pdf_files[:PROCESS_BATCH_SIZE]:
            if should_stop:
                logger.info("收到停止信號，中斷處理")
                break
            
            # 檢查是否已處理過
            file_hash = self.get_file_hash(pdf_path)
            if pdf_path.name in self.state:
                if self.state[pdf_path.name].get("hash") == file_hash:
                    logger.info(f"⏭️ 跳過已處理: {pdf_path.name}")
                    done_path = PDF_DONE_DIR / pdf_path.name
                    pdf_path.rename(done_path)
                    continue
            
            # 處理檔案
            if self.process_pdf(pdf_path):
                processed += 1
            
            # 短暫休息
            time.sleep(1)
        
        return processed

    def run(self):
        """主執行循環"""
        logger.info("="*60)
        logger.info("🚀 PDF 處理服務啟動（增強版）")
        logger.info(f"📁 監控目錄: {PDF_WATCH_DIR}")
        logger.info(f"⏱ 掃描間隔: {SCAN_INTERVAL} 秒")
        logger.info(f"📦 批次大小: {PROCESS_BATCH_SIZE}")
        logger.info(f"🔍 OCR 狀態: {'啟用' if ENABLE_OCR else '關閉'}")
        logger.info(f"🐛 調試模式: {'開啟' if DEBUG_MODE else '關閉'}")
        logger.info("="*60)
        
        # 顯示資料庫統計
        stats = self.db.get_statistics()
        if stats:
            logger.info("📊 目前資料庫狀態：")
            logger.info(f"  總文檔數: {stats.get('total_documents', 0)}")
            if 'documents_by_type' in stats:
                for doc_type, count in stats['documents_by_type'].items():
                    logger.info(f"    - {doc_type}: {count} 份")
            logger.info(f"  今日處理: {stats.get('today_processed', 0)} 份")
            if stats.get('today_errors', 0) > 0:
                logger.warning(f"  今日錯誤: {stats.get('today_errors', 0)} 份")
        
        logger.info("="*60)
        
        no_file_count = 0
        
        try:
            while not should_stop:
                try:
                    processed = self.scan_and_process()
                    
                    if processed > 0:
                        no_file_count = 0
                        logger.info(f"✨ 本輪處理完成，共 {processed} 個檔案")
                        
                        # 每處理完一批就顯示統計
                        stats = self.db.get_statistics()
                        if stats:
                            logger.info(f"📊 累計處理 {stats.get('total_documents', 0)} 份文檔")
                    else:
                        no_file_count += 1
                        
                        # 動態調整掃描間隔
                        if no_file_count > 10:
                            sleep_time = min(300, SCAN_INTERVAL * 2)
                        else:
                            sleep_time = SCAN_INTERVAL
                        
                        # 每隔一段時間顯示系統仍在運作
                        if no_file_count % 5 == 0:
                            logger.debug(f"💤 系統運作中，等待新檔案... (已等待 {no_file_count} 次)")
                        
                        time.sleep(sleep_time)
                        
                except KeyboardInterrupt:
                    logger.info("收到鍵盤中斷")
                    break
                except Exception as e:
                    logger.error(f"處理循環錯誤: {e}")
                    time.sleep(30)
                    
        finally:
            self.cleanup()

    def cleanup(self):
        """清理資源"""
        logger.info("正在清理資源...")
        self.save_state()
        self.db.close()
        logger.info("👋 PDF 處理服務已關閉")

# ============================================================================
# 主程式進入點
# ============================================================================
if __name__ == "__main__":
    try:
        # 檢查是否需要重建資料庫表格
        # 可以透過環境變數控制
        REBUILD_TABLES = os.getenv("REBUILD_TABLES", "false").lower() == "true"
        
        if REBUILD_TABLES:
            logger.warning("⚠️ 準備重建資料庫表格...")
            logger.warning("這將刪除所有現有資料！")
            
            # 給使用者 5 秒鐘的時間中斷
            import time
            for i in range(5, 0, -1):
                logger.warning(f"將在 {i} 秒後開始...（按 Ctrl+C 中斷）")
                time.sleep(1)
            
            # 執行重建
            temp_db = DatabaseManager()
            temp_db.drop_tables_if_needed(confirm=True)
            temp_db.close()
            logger.info("表格已刪除，將重新建立...")
        
        # 正常啟動服務
        service = PDFProcessorService()
        service.run()
        
    except KeyboardInterrupt:
        logger.info("使用者中斷執行")
        sys.exit(0)
    except Exception as e:
        logger.error(f"服務啟動失敗: {e}")
        sys.exit(1)
