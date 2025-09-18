#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
PDF 文檔處理服務 - 福興工業技術文件自動化處理系統
=============================================================================

功能概述：
1. 自動監控指定目錄中的 PDF 檔案
2. 解析福興工業的技術文件（設變通知、DFMEA、規格書等）
3. 提取結構化資料並存入 MySQL 資料庫
4. 支援狀態追蹤，避免重複處理
5. 可選的 OCR 功能處理掃描檔

系統架構：
    PDF檔案 → 監控目錄 → 解析處理 → MySQL → 同步到 ES → RAG檢索

作者: [您的團隊]
版本: 1.0.0
更新日期: 2024
=============================================================================
"""

import os, sys, time, json, signal, hashlib, logging, pymysql, pdfplumber, re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict

# ============================================================================
# 環境變數配置
# ============================================================================
# 這些環境變數在 docker-compose.yml 中設定，提供靈活的配置選項

# MySQL 連線設定 - 連接到您的資料庫容器
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")  # Docker 網路中的主機名
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "fuhsin_erp_demo")

# PDF 檔案目錄結構
# /mnt/pdf/
#   ├── incoming/      # 新檔案放這裡
#   ├── .done/         # 處理完成的檔案
#   ├── .error/        # 處理失敗的檔案
#   └── .processing/   # 正在處理的檔案（避免重複處理）
PDF_WATCH_DIR = Path(os.getenv("PDF_WATCH_DIR", "/mnt/pdf/incoming"))
PDF_DONE_DIR = PDF_WATCH_DIR / ".done"
PDF_ERROR_DIR = PDF_WATCH_DIR / ".error"
PDF_PROCESSING_DIR = PDF_WATCH_DIR / ".processing"

# 處理參數設定
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "30"))  # 掃描間隔（秒）
PROCESS_BATCH_SIZE = int(os.getenv("PROCESS_BATCH_SIZE", "5"))  # 每批處理檔案數
ENABLE_OCR = os.getenv("ENABLE_OCR", "false").lower() == "true"  # 是否啟用 OCR
OCR_LANG = os.getenv("OCR_LANG", "chi_tra+eng")  # OCR 語言：繁體中文+英文

# 狀態和日誌檔案路徑
STATE_FILE = Path("/state/.pdf_processor_state.json")  # 處理狀態記錄
LOG_FILE = Path("/logs/pdf_processor.log")  # 日誌檔案
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ============================================================================
# 日誌設定
# ============================================================================
# 確保目錄存在
os.makedirs(LOG_FILE.parent, exist_ok=True)
os.makedirs(STATE_FILE.parent, exist_ok=True)

# 配置日誌格式和輸出
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),  # 寫入檔案
        logging.StreamHandler(),  # 輸出到控制台
    ],
)
logger = logging.getLogger(__name__)

# ============================================================================
# 優雅關閉機制
# ============================================================================
# 全域變數：用於接收中斷信號時優雅關閉
should_stop = False


def signal_handler(signum, frame):
    """
    處理系統中斷信號（SIGINT, SIGTERM）
    當收到 docker stop 或 Ctrl+C 時觸發
    """
    global should_stop
    logger.info("🛑 收到中斷信號，準備優雅關閉...")
    should_stop = True


# 註冊信號處理器
signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # docker stop


# ============================================================================
# 資料模型定義
# ============================================================================
@dataclass
class TechnicalDocument:
    """
    技術文檔資料模型

    使用 @dataclass 裝飾器自動生成 __init__、__repr__ 等方法
    這個模型對應到 MySQL 的 technical_documents 表
    """

    doc_id: str  # 文檔唯一識別碼（MD5 hash）
    doc_type: str  # 文檔類型（ECN/DFMEA/SPEC/DRAWING/COMPLAINT）
    doc_number: str  # 文檔編號（如 EC-K-28-C-083）
    title: str  # 文檔標題
    product_ids: List[str]  # 相關產品編號列表（如 ['OB1-G04313AU', 'OB1-G04313A']）
    revision: Optional[str]  # 版本號
    issue_date: Optional[str]  # 發行日期
    department: Optional[str]  # 部門
    author: Optional[str]  # 作者/負責人
    content: str  # 完整文本內容（用於全文搜尋）
    summary: Optional[str]  # 自動生成的摘要
    keywords: List[str]  # 關鍵字列表（用於標籤和快速檢索）
    metadata: Dict  # 其他元資料（JSON 格式存儲）
    file_path: str  # 原始檔案路徑
    file_hash: str  # 檔案 SHA256 雜湊值（用於去重）
    created_at: str  # 建立時間
    updated_at: str  # 更新時間
    page_count: int  # PDF 頁數
    file_size: int  # 檔案大小（bytes）


# ============================================================================
# 資料庫管理器
# ============================================================================
class DatabaseManager:
    """
    資料庫連線和操作管理

    負責：
    1. 管理 MySQL 連線
    2. 建立資料表
    3. 插入/更新文檔資料
    4. 記錄處理日誌
    """

    def __init__(self):
        """初始化資料庫管理器"""
        self.connection = None
        self.create_tables()  # 確保資料表存在

    def get_connection(self):
        """
        取得資料庫連線（連線池概念的簡單實現）
        如果連線不存在或已斷開，建立新連線
        """
        if not self.connection or not self.connection.open:
            logger.info("建立新的資料庫連線...")
            self.connection = pymysql.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE,
                charset="utf8mb4",  # 支援完整的 Unicode（包括 emoji）
                autocommit=True,  # 自動提交事務
            )
        return self.connection

    def create_tables(self):
        """
        建立必要的資料表
        如果表已存在則跳過（CREATE TABLE IF NOT EXISTS）
        """
        create_sql = """
        -- 主要文檔表：存儲所有解析出的技術文件
        CREATE TABLE IF NOT EXISTS technical_documents (
            id INT AUTO_INCREMENT PRIMARY KEY,
            doc_id VARCHAR(64) UNIQUE NOT NULL COMMENT '文檔唯一ID',
            doc_type VARCHAR(20) NOT NULL COMMENT '文檔類型',
            doc_number VARCHAR(50) COMMENT '文檔編號',
            title VARCHAR(255) NOT NULL COMMENT '標題',
            product_ids JSON COMMENT '相關產品編號（JSON陣列）',
            revision VARCHAR(20) COMMENT '版本號',
            issue_date DATE COMMENT '發行日期',
            department VARCHAR(50) COMMENT '部門',
            author VARCHAR(50) COMMENT '作者',
            content LONGTEXT NOT NULL COMMENT '完整內容（用於全文搜尋）',
            summary TEXT COMMENT '摘要',
            keywords JSON COMMENT '關鍵字（JSON陣列）',
            metadata JSON COMMENT '元資料（JSON物件）',
            file_path VARCHAR(500) COMMENT '檔案路徑',
            file_hash VARCHAR(64) COMMENT '檔案雜湊',
            page_count INT DEFAULT 0 COMMENT '頁數',
            file_size INT DEFAULT 0 COMMENT '檔案大小(bytes)',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            
            -- 索引優化查詢效能
            INDEX idx_doc_type (doc_type),        -- 按類型查詢
            INDEX idx_doc_number (doc_number),    -- 按編號查詢
            INDEX idx_issue_date (issue_date),    -- 按日期查詢
            INDEX idx_created_at (created_at),    -- 按建立時間排序
            FULLTEXT idx_content (content),       -- 全文搜尋內容
            FULLTEXT idx_title_summary (title, summary)  -- 全文搜尋標題和摘要
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        
        -- 處理記錄表：追蹤每個檔案的處理狀態
        CREATE TABLE IF NOT EXISTS pdf_processing_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            file_name VARCHAR(255) NOT NULL COMMENT '檔案名稱',
            file_hash VARCHAR(64) COMMENT '檔案雜湊',
            status ENUM('processing', 'success', 'error') NOT NULL COMMENT '處理狀態',
            error_message TEXT COMMENT '錯誤訊息',
            process_time_ms INT COMMENT '處理時間（毫秒）',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            INDEX idx_status (status),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """

        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # 分割 SQL 語句並逐一執行
                for statement in create_sql.split(";"):
                    if statement.strip():
                        cursor.execute(statement)
                conn.commit()
                logger.info("✅ 資料表建立/確認成功")
        except Exception as e:
            logger.error(f"❌ 建立資料表失敗: {e}")
            raise

    def upsert_document(self, doc: TechnicalDocument) -> bool:
        """
        插入或更新文檔（UPSERT 操作）

        使用 INSERT ... ON DUPLICATE KEY UPDATE 語法
        如果 doc_id 已存在則更新，否則插入新記錄

        Args:
            doc: TechnicalDocument 物件

        Returns:
            bool: 操作是否成功
        """
        sql = """
        INSERT INTO technical_documents (
            doc_id, doc_type, doc_number, title, product_ids, revision,
            issue_date, department, author, content, summary, keywords,
            metadata, file_path, file_hash, page_count, file_size
        ) VALUES (
            %(doc_id)s, %(doc_type)s, %(doc_number)s, %(title)s, 
            %(product_ids)s, %(revision)s, %(issue_date)s, %(department)s,
            %(author)s, %(content)s, %(summary)s, %(keywords)s,
            %(metadata)s, %(file_path)s, %(file_hash)s, %(page_count)s, %(file_size)s
        )
        ON DUPLICATE KEY UPDATE
            doc_type = VALUES(doc_type),
            title = VALUES(title),
            product_ids = VALUES(product_ids),
            content = VALUES(content),
            summary = VALUES(summary),
            keywords = VALUES(keywords),
            metadata = VALUES(metadata),
            page_count = VALUES(page_count),
            file_size = VALUES(file_size),
            updated_at = CURRENT_TIMESTAMP
        """

        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # 將 dataclass 轉換為字典
                doc_dict = asdict(doc)

                # 將 Python 列表/字典轉換為 JSON 字串
                doc_dict["product_ids"] = json.dumps(
                    doc.product_ids, ensure_ascii=False
                )
                doc_dict["keywords"] = json.dumps(doc.keywords, ensure_ascii=False)
                doc_dict["metadata"] = json.dumps(doc.metadata, ensure_ascii=False)

                cursor.execute(sql, doc_dict)
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"資料庫寫入錯誤: {e}")
            return False

    def log_processing(
        self,
        file_name: str,
        file_hash: str,
        status: str,
        error_message: str = None,
        process_time_ms: int = 0,
    ):
        """
        記錄處理狀態到日誌表
        用於追蹤和診斷問題
        """
        sql = """
        INSERT INTO pdf_processing_log 
        (file_name, file_hash, status, error_message, process_time_ms)
        VALUES (%s, %s, %s, %s, %s)
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    sql, (file_name, file_hash, status, error_message, process_time_ms)
                )
                conn.commit()
        except Exception as e:
            logger.error(f"記錄處理日誌失敗: {e}")

    def close(self):
        """關閉資料庫連線"""
        if self.connection:
            self.connection.close()
            logger.info("資料庫連線已關閉")


# ============================================================================
# PDF 解析器
# ============================================================================
class PDFParser:
    """
    PDF 文件解析器 - 專門針對福興工業文件優化

    主要功能：
    1. 提取 PDF 文字內容（原生文字層）
    2. 識別文檔類型（設變通知、DFMEA 等）
    3. 提取產品編號（使用正則表達式）
    4. 生成摘要和關鍵字
    5. 可選的 OCR 處理（掃描檔）
    """

    # 福興產品編號的正則表達式模式
    # 這些模式基於您提供的 PDF 文件中的實際產品編號格式
    PRODUCT_PATTERNS = [
        r"OB\d-[A-Z0-9]+",  # 例：OB1-G04313AU, OB1-G04313A
        r"[FG]\d{2}-[A-Z0-9]+",  # 例：F05-L0Y513, G05-L05513
        r"[PW]\d{3}",  # 例：P001, W002（產品/倉庫編號）
        r"EC-K-\d{2}-[A-Z]-\d{3}",  # 例：EC-K-28-C-083（文件編號）
        r"L\d{6}[A-Z]?\d?",  # 例：L113055R2, L112078（變更單號）
    ]

    # 文檔類型識別關鍵字
    # 用於自動分類文檔
    DOC_TYPE_KEYWORDS = {
        "ECN": ["設變通知", "設計變更", "ECN", "Engineering Change"],
        "DFMEA": ["DFMEA", "失效模式", "風險分析"],
        "SPEC": ["規格", "規範", "Specification", "技術要求"],
        "DRAWING": ["圖面", "圖紙", "Drawing", "工程圖"],
        "COMPLAINT": ["客訴", "客戶抱怨", "顧客抱怨", "Complaint"],
        "REPORT": ["報告", "測試", "Report", "Test"],
    }

    @classmethod
    def extract_text(cls, pdf_path: Path) -> Tuple[str, int]:
        """
        提取 PDF 全文和頁數

        Args:
            pdf_path: PDF 檔案路徑

        Returns:
            tuple: (提取的文字, 頁數)
        """
        text = ""
        page_count = 0

        try:
            # 使用 pdfplumber 開啟 PDF
            with pdfplumber.open(pdf_path) as pdf:
                page_count = len(pdf.pages)
                logger.debug(f"PDF 共 {page_count} 頁")

                for i, page in enumerate(pdf.pages, 1):
                    # 提取頁面文字
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
                        logger.debug(f"  第 {i} 頁提取 {len(page_text)} 字元")

                    # 提取表格（福興文件常包含規格表）
                    tables = page.extract_tables()
                    for table_idx, table in enumerate(tables):
                        logger.debug(f"  第 {i} 頁發現表格 {table_idx + 1}")
                        for row in table:
                            if row:
                                # 將表格行轉換為文字（用 | 分隔）
                                text += (
                                    " | ".join(
                                        str(cell) if cell else "" for cell in row
                                    )
                                    + "\n"
                                )

        except Exception as e:
            logger.error(f"PDF文字提取錯誤 {pdf_path}: {e}")

            # 如果啟用 OCR 且無法提取文字，嘗試 OCR
            if ENABLE_OCR and not text:
                logger.info(f"嘗試使用 OCR 處理 {pdf_path}")
                text = cls.extract_text_with_ocr(pdf_path)

        return text, page_count

    @classmethod
    def extract_text_with_ocr(cls, pdf_path: Path) -> str:
        """
        使用 OCR 提取文字（處理掃描檔）
        需要安裝 tesseract-ocr 和相關 Python 套件

        Args:
            pdf_path: PDF 檔案路徑

        Returns:
            str: OCR 識別的文字
        """
        try:
            import pytesseract
            from pdf2image import convert_from_path

            logger.info(f"開始 OCR 處理...")

            # 將 PDF 轉換為圖片（DPI 200 適合大部分文件）
            images = convert_from_path(str(pdf_path), dpi=200)

            text = ""
            for i, image in enumerate(images):
                logger.info(f"  OCR 處理第 {i+1}/{len(images)} 頁")
                # 使用 Tesseract 進行 OCR
                # lang 參數：chi_tra = 繁體中文, eng = 英文
                page_text = pytesseract.image_to_string(image, lang=OCR_LANG)
                text += page_text + "\n"

            logger.info(f"OCR 完成，共識別 {len(text)} 字元")
            return text

        except ImportError:
            logger.error("OCR 相關套件未安裝（pytesseract, pdf2image）")
            return ""
        except Exception as e:
            logger.error(f"OCR 處理失敗: {e}")
            return ""

    @classmethod
    def detect_doc_type(cls, text: str, filename: str) -> str:
        """
        識別文檔類型

        策略：
        1. 先檢查檔名
        2. 再檢查內容關鍵字
        3. 無法識別則返回 'GENERAL'

        Args:
            text: 文檔內容
            filename: 檔案名稱

        Returns:
            str: 文檔類型
        """
        text_lower = text.lower()
        filename_lower = filename.lower()

        # 檢查每種文檔類型的關鍵字
        for doc_type, keywords in cls.DOC_TYPE_KEYWORDS.items():
            for keyword in keywords:
                if keyword.lower() in text_lower or keyword.lower() in filename_lower:
                    logger.debug(f"識別為 {doc_type} 類型（關鍵字：{keyword}）")
                    return doc_type

        # 無法識別，返回通用類型
        return "GENERAL"

    @classmethod
    def extract_product_ids(cls, text: str) -> List[str]:
        """
        提取產品編號

        使用正則表達式匹配福興的產品編號格式
        去重並排序後返回

        Args:
            text: 文檔內容

        Returns:
            list: 產品編號列表
        """
        product_ids = set()

        for pattern in cls.PRODUCT_PATTERNS:
            # 使用正則表達式查找所有匹配
            matches = re.findall(pattern, text, re.IGNORECASE)
            # 轉換為大寫並加入集合（自動去重）
            product_ids.update(m.upper() for m in matches)

        result = sorted(list(product_ids))[:20]  # 最多保留20個
        logger.debug(f"找到 {len(result)} 個產品編號: {result[:5]}...")

        return result

    @classmethod
    def extract_metadata(cls, text: str, doc_type: str) -> Dict:
        """
        提取元資料（日期、版本號等）

        Args:
            text: 文檔內容
            doc_type: 文檔類型

        Returns:
            dict: 元資料字典
        """
        metadata = {}

        # 提取日期（支援多種格式）
        date_patterns = [
            r"\d{4}/\d{1,2}/\d{1,2}",  # 2024/4/25
            r"\d{4}-\d{1,2}-\d{1,2}",  # 2024-04-25
        ]
        for pattern in date_patterns:
            dates = re.findall(pattern, text)
            if dates:
                metadata["dates"] = dates[:5]  # 最多保留5個日期
                logger.debug(f"找到日期: {dates[:3]}")
                break

        # 提取版本號（Rev A, R2 等格式）
        revision_match = re.search(r"[Rr]ev(?:ision)?[:\s]*([A-Z0-9]+)", text)
        if revision_match:
            metadata["revision"] = revision_match.group(1)
            logger.debug(f"找到版本號: {metadata['revision']}")

        # 根據文檔類型提取特定資訊
        if doc_type == "ECN":
            # 設變通知特有：變更原因
            reason_match = re.search(r"原因[：:]\s*([^\n]+)", text)
            if reason_match:
                metadata["change_reason"] = reason_match.group(1)

        elif doc_type == "DFMEA":
            # DFMEA 特有：嚴重度評分
            severity_match = re.search(r"嚴重度[：:]\s*(\d+)", text)
            if severity_match:
                metadata["severity"] = int(severity_match.group(1))

        return metadata

    @classmethod
    def extract_keywords(cls, text: str, product_ids: List[str]) -> List[str]:
        """
        提取關鍵字

        策略：
        1. 檢查預定義的技術關鍵字
        2. 加入產品編號作為關鍵字

        Args:
            text: 文檔內容
            product_ids: 產品編號列表

        Returns:
            list: 關鍵字列表
        """
        keywords = []

        # 福興特定技術關鍵字（基於您提供的 PDF 內容）
        tech_keywords = [
            "排線",
            "內側組合",
            "外側組合",
            "彈簧",
            "套盤",
            "把手",
            "WiFi",
            "deadbolt",
            "轉軸",
            "內軸筒",
            "底板",
            "裝飾",
            "測試",
            "品質",
            "規格",
            "公差",
            "尺寸",
            "材質",
            "Hubspace",
            "設變",
            "改善",
            "優化",
            "不良",
            "矯正",
        ]

        # 檢查每個關鍵字是否出現在文檔中
        for keyword in tech_keywords:
            if keyword in text:
                keywords.append(keyword)

        # 加入前5個產品編號作為關鍵字
        keywords.extend(product_ids[:5])

        # 去重並限制數量
        result = list(set(keywords))[:15]
        logger.debug(f"提取 {len(result)} 個關鍵字")

        return result

    @classmethod
    def generate_summary(cls, text: str, max_length: int = 300) -> str:
        """
        生成文檔摘要

        策略：
        1. 優先提取包含重要關鍵字的句子
        2. 如果沒有，則取文檔開頭部分

        Args:
            text: 文檔內容
            max_length: 摘要最大長度

        Returns:
            str: 摘要文字
        """
        # 清理文本（合併空白字元）
        text = re.sub(r"\s+", " ", text)
        text = text.strip()

        # 優先提取包含重要關鍵字的句子
        important_patterns = [
            r"[^。！？\n]*(?:變更|修改|調整|改善|優化)[^。！？\n]*",
            r"[^。！？\n]*(?:問題|缺陷|不良|異常)[^。！？\n]*",
        ]

        summary_sentences = []
        for pattern in important_patterns:
            matches = re.findall(pattern, text)
            summary_sentences.extend(matches[:2])  # 每種類型最多取2句

        if summary_sentences:
            summary = "。".join(summary_sentences)[:max_length]
        else:
            # 沒有找到重要句子，取前 max_length 字元
            summary = text[:max_length]

        if len(summary) == max_length:
            summary += "..."

        return summary


# ============================================================================
# 主處理服務
# ============================================================================
class PDFProcessorService:
    """
    PDF 處理服務主類

    職責：
    1. 監控 PDF 檔案目錄
    2. 協調解析和存儲流程
    3. 管理處理狀態
    4. 處理錯誤和重試
    """

    def __init__(self):
        """初始化服務"""
        self.db = DatabaseManager()
        self.state = self.load_state()
        self.setup_directories()
        logger.info("PDF 處理服務初始化完成")

    def setup_directories(self):
        """
        建立必要的目錄結構
        確保所有工作目錄存在
        """
        for dir_path in [
            PDF_WATCH_DIR,
            PDF_DONE_DIR,
            PDF_ERROR_DIR,
            PDF_PROCESSING_DIR,
        ]:
            dir_path.mkdir(parents=True, exist_ok=True)
            logger.debug(f"確認目錄存在: {dir_path}")

    def load_state(self) -> Dict:
        """
        載入處理狀態

        狀態檔案記錄每個處理過的檔案的資訊
        避免重複處理相同檔案

        Returns:
            dict: 狀態字典
        """
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    state = json.load(f)
                    logger.info(f"載入狀態檔案，已處理 {len(state)} 個檔案")
                    return state
            except Exception as e:
                logger.warning(f"載入狀態檔案失敗: {e}，使用空狀態")
                return {}
        return {}

    def save_state(self):
        """
        儲存處理狀態
        每處理完一個檔案就儲存，確保斷電也不會遺失進度
        """
        try:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
            logger.debug("狀態已儲存")
        except Exception as e:
            logger.error(f"儲存狀態失敗: {e}")

    def get_file_hash(self, file_path: Path) -> str:
        """
        計算檔案的 SHA256 雜湊值
        用於判斷檔案是否已變更

        Args:
            file_path: 檔案路徑

        Returns:
            str: SHA256 雜湊值（16進制字串）
        """
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            # 分塊讀取，避免大檔案占用過多記憶體
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def process_pdf(self, pdf_path: Path) -> bool:
        """
        處理單一 PDF 檔案的完整流程

        步驟：
        1. 計算檔案雜湊
        2. 移到處理中目錄（避免重複處理）
        3. 解析 PDF 內容
        4. 建立文檔物件
        5. 存入資料庫
        6. 移到完成或錯誤目錄

        Args:
            pdf_path: PDF 檔案路徑

        Returns:
            bool: 處理是否成功
        """
        start_time = time.time()
        file_hash = self.get_file_hash(pdf_path)
        file_size = pdf_path.stat().st_size

        try:
            logger.info(f"📄 開始處理: {pdf_path.name} ({file_size/1024:.1f} KB)")

            # 記錄開始處理
            self.db.log_processing(pdf_path.name, file_hash, "processing")

            # 步驟1：移到處理中目錄（防止其他進程重複處理）
            processing_path = PDF_PROCESSING_DIR / pdf_path.name
            pdf_path.rename(processing_path)
            logger.debug(f"檔案移至處理中: {processing_path}")

            # 步驟2：提取文本
            text, page_count = PDFParser.extract_text(processing_path)

            if not text:
                raise ValueError("無法提取文本內容（檔案可能損壞或為圖片）")

            logger.info(f"  提取文本: {len(text)} 字元, {page_count} 頁")

            # 步驟3：解析文檔資訊
            doc_type = PDFParser.detect_doc_type(text, processing_path.stem)
            product_ids = PDFParser.extract_product_ids(text)
            metadata = PDFParser.extract_metadata(text, doc_type)
            keywords = PDFParser.extract_keywords(text, product_ids)
            summary = PDFParser.generate_summary(text)

            # 從檔名提取文檔編號（福興的命名規則）
            doc_number_match = re.search(
                r"[A-Z]{2,}-[A-Z]-\d{2}-[A-Z]-\d{3}|L\d{6}[A-Z]?\d?",
                processing_path.stem,
            )
            doc_number = (
                doc_number_match.group(0) if doc_number_match else processing_path.stem
            )

            # 步驟4：建立文檔物件
            doc = TechnicalDocument(
                doc_id=hashlib.md5(processing_path.name.encode()).hexdigest(),
                doc_type=doc_type,
                doc_number=doc_number,
                title=processing_path.stem.replace("_", " "),  # 將底線轉為空格
                product_ids=product_ids,
                revision=metadata.get("revision"),
                issue_date=(
                    metadata.get("dates", [None])[0] if metadata.get("dates") else None
                ),
                department=None,  # 可從文檔內容進一步提取
                author=None,  # 可從文檔內容進一步提取
                content=text,
                summary=summary,
                keywords=keywords,
                metadata=metadata,
                file_path=str(processing_path),
                file_hash=file_hash,
                created_at=datetime.now().isoformat(),
                updated_at=datetime.now().isoformat(),
                page_count=page_count,
                file_size=file_size,
            )

            logger.info(
                f"  文檔資訊: 類型={doc_type}, 產品數={len(product_ids)}, 關鍵字數={len(keywords)}"
            )

            # 步驟5：存入資料庫
            if self.db.upsert_document(doc):
                # 成功：移到完成目錄
                done_path = PDF_DONE_DIR / processing_path.name
                processing_path.rename(done_path)

                process_time = int((time.time() - start_time) * 1000)
                self.db.log_processing(
                    pdf_path.name, file_hash, "success", process_time_ms=process_time
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
                process_time_ms=process_time,
            )

            # 記錄錯誤狀態
            self.state[pdf_path.name] = {
                "hash": file_hash,
                "processed_at": datetime.now().isoformat(),
                "status": "error",
                "error": error_msg,
            }
            self.save_state()
            return False

    def scan_and_process(self):
        """
        掃描監控目錄並處理 PDF 檔案

        流程：
        1. 掃描 incoming 目錄
        2. 檢查每個檔案是否已處理
        3. 批次處理新檔案
        4. 記錄處理結果

        Returns:
            int: 本次處理的檔案數
        """
        # 掃描所有 PDF 檔案
        pdf_files = sorted(PDF_WATCH_DIR.glob("*.pdf"))

        if not pdf_files:
            return 0

        logger.info(f"🔍 發現 {len(pdf_files)} 個待處理檔案")

        processed = 0
        # 限制每批處理的檔案數，避免記憶體問題
        for pdf_path in pdf_files[:PROCESS_BATCH_SIZE]:
            # 檢查是否應該停止
            if should_stop:
                logger.info("收到停止信號，中斷處理")
                break

            # 檢查是否已處理過此檔案
            file_hash = self.get_file_hash(pdf_path)
            if pdf_path.name in self.state:
                if self.state[pdf_path.name].get("hash") == file_hash:
                    logger.info(f"⏭️ 跳過已處理: {pdf_path.name}")
                    # 已處理過的檔案移到完成目錄
                    done_path = PDF_DONE_DIR / pdf_path.name
                    pdf_path.rename(done_path)
                    continue

            # 處理檔案
            if self.process_pdf(pdf_path):
                processed += 1

            # 短暫休息，避免 CPU 過載
            time.sleep(1)

        return processed

    def run(self):
        """
        主執行循環
        持續監控目錄並處理新檔案
        """
        logger.info("=" * 60)
        logger.info("🚀 PDF 處理服務啟動")
        logger.info(f"📁 監控目錄: {PDF_WATCH_DIR}")
        logger.info(f"⏱  掃描間隔: {SCAN_INTERVAL} 秒")
        logger.info(f"📦 批次大小: {PROCESS_BATCH_SIZE}")
        logger.info(f"🔍 OCR 狀態: {'啟用' if ENABLE_OCR else '關閉'}")
        logger.info("=" * 60)

        no_file_count = 0  # 連續無檔案的次數

        try:
            while not should_stop:
                try:
                    # 執行一次掃描和處理
                    processed = self.scan_and_process()

                    if processed > 0:
                        no_file_count = 0
                        logger.info(f"✨ 本輪處理完成，共 {processed} 個檔案")
                    else:
                        no_file_count += 1

                        # 動態調整掃描間隔（無檔案時逐漸延長間隔）
                        sleep_time = min(SCAN_INTERVAL * (1 + no_file_count // 10), 300)

                        # 每10次無檔案才輸出一次日誌，避免日誌過多
                        if no_file_count % 10 == 0:
                            logger.info(f"💤 無新檔案，等待 {sleep_time} 秒...")

                    # 休眠等待下次掃描
                    sleep_time = 5 if processed > 0 else SCAN_INTERVAL
                    for _ in range(sleep_time):
                        if should_stop:
                            break
                        time.sleep(1)  # 每秒檢查一次停止信號

                except Exception as e:
                    logger.error(f"處理週期錯誤: {e}", exc_info=True)
                    time.sleep(30)  # 錯誤後等待較長時間

        except KeyboardInterrupt:
            logger.info("收到鍵盤中斷")
        finally:
            logger.info("🛑 服務正在關閉...")
            self.db.close()
            logger.info("👋 服務已停止")


# ============================================================================
# 主程式入口
# ============================================================================
def main():
    """
    主程式入口

    步驟：
    1. 等待 MySQL 就緒
    2. 初始化服務
    3. 啟動主循環
    """
    # 等待 MySQL 就緒（最多等待60秒）
    logger.info("⏳ 等待 MySQL 就緒...")
    for i in range(30):
        try:
            db = DatabaseManager()
            db.close()
            logger.info("✅ MySQL 連線成功")
            break
        except Exception as e:
            if i < 29:
                logger.info(f"等待 MySQL... ({i+1}/30)")
                time.sleep(2)
            else:
                logger.error(f"❌ 無法連線到 MySQL: {e}")
                sys.exit(1)

    # 啟動服務
    service = PDFProcessorService()
    service.run()


if __name__ == "__main__":
    main()
