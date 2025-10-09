#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF 處理器服務 - 含 OCR 功能
專注於將 PDF 掃描並存入 technical_documents 主表
支援文字型和掃描型 PDF
"""

import os
import sys
import time
import json
import hashlib
import re
import pymysql
import pdfplumber
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

# OCR 相關
try:
    from pdf2image import convert_from_path
    import pytesseract
    from PIL import Image
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    logging.warning("OCR 套件未安裝，將無法處理掃描版 PDF")

# ========== 配置 ==========
MYSQL_HOST = os.environ.get('MYSQL_HOST', 'mysql')
MYSQL_PORT = int(os.environ.get('MYSQL_PORT', '3306'))
MYSQL_USER = os.environ.get('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'root')
MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE', 'fuhsin_erp_demo')

PDF_WATCH_DIR = Path(os.environ.get('PDF_WATCH_DIR', '/mnt/pdf/incoming'))
PDF_PROCESSING_DIR = Path(os.environ.get('PDF_PROCESSING_DIR', '/mnt/pdf/processing'))
PDF_DONE_DIR = Path(os.environ.get('PDF_DONE_DIR', '/mnt/pdf/done'))
PDF_ERROR_DIR = Path(os.environ.get('PDF_ERROR_DIR', '/mnt/pdf/error'))

SCAN_INTERVAL = int(os.environ.get('SCAN_INTERVAL', '20'))
BATCH_SIZE = int(os.environ.get('PROCESS_BATCH_SIZE', '3'))

# OCR 配置
ENABLE_OCR = os.environ.get('ENABLE_OCR', 'true').lower() == 'true'
OCR_LANG = os.environ.get('OCR_LANG', 'chi_tra+eng')  # 繁體中文+英文
OCR_DPI = int(os.environ.get('OCR_DPI', '300'))  # OCR 解析度

# ========== 日誌配置 ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========== 資料模型 ==========
@dataclass
class TechnicalDocument:
    """簡化的技術文件模型"""
    doc_id: str
    doc_type: str
    file_name: str
    file_size: int
    page_count: int
    content: str
    created_at: datetime

# ========== 資料庫管理器 ==========
class DatabaseManager:
    """資料庫操作管理器"""
    
    def __init__(self):
        self.connection = None
        self.connect()
        self.init_database()

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
        """初始化資料庫表格 - 只建立主表"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # 建立 technical_documents 主表
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS technical_documents (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    doc_id VARCHAR(32) UNIQUE NOT NULL,
                    doc_type VARCHAR(50),
                    file_name VARCHAR(255) NOT NULL,
                    file_size INT,
                    page_count INT,
                    content LONGTEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_doc_type (doc_type),
                    INDEX idx_created_at (created_at),
                    FULLTEXT idx_content (content)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                
                # 建立 pdf_processing_log 表
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS pdf_processing_log (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    file_name VARCHAR(255) NOT NULL,
                    file_hash VARCHAR(32),
                    status ENUM('processing', 'success', 'error', 'skipped') NOT NULL,
                    error_message TEXT,
                    process_time_ms INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_status (status),
                    INDEX idx_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                
                conn.commit()
                logger.info("✅ 確認表格存在: technical_documents, pdf_processing_log")
                
        except Exception as e:
            logger.error(f"初始化資料庫失敗: {e}")

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
        """儲存文檔到資料庫 - 簡化版"""
        sql = """
        INSERT INTO technical_documents 
        (doc_id, doc_type, file_name, file_size, page_count, content)
        VALUES (%(doc_id)s, %(doc_type)s, %(file_name)s, %(file_size)s, %(page_count)s, %(content)s)
        ON DUPLICATE KEY UPDATE
            doc_type = VALUES(doc_type),
            file_name = VALUES(file_name),
            file_size = VALUES(file_size),
            page_count = VALUES(page_count),
            content = VALUES(content)
        """
        
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(sql, {
                    'doc_id': doc.doc_id,
                    'doc_type': doc.doc_type,
                    'file_name': doc.file_name,
                    'file_size': doc.file_size,
                    'page_count': doc.page_count,
                    'content': doc.content
                })
                conn.commit()
                logger.info(f"✅ 文檔已儲存: {doc.doc_id}")
                return True
        except Exception as e:
            logger.error(f"儲存文檔失敗 {doc.doc_id}: {e}")
            return False
    
    def check_document_exists(self, doc_id: str) -> bool:
        """檢查文檔是否已存在"""
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

# ========== PDF 解析器 ==========
class SimplePDFParser:
    """簡化的 PDF 解析器 - 支援 OCR"""
    
    @staticmethod
    def extract_text(pdf_path: Path) -> tuple[str, int]:
        """提取 PDF 文字內容 - 優先使用文字提取，失敗則使用 OCR"""
        text_parts = []
        page_count = 0
        
        try:
            # 方法 1: 使用 pdfplumber 提取文字
            logger.info(f"  嘗試使用 pdfplumber 提取文字...")
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    page_count += 1
                    page_text = page.extract_text() or ""
                    
                    if page_text and len(page_text.strip()) > 50:
                        text_parts.append(f"[第 {page_num + 1} 頁]\n{page_text}")
            
            combined_text = '\n'.join(text_parts)
            
            # 檢查提取結果
            if combined_text and len(combined_text.strip()) > 100:
                logger.info(f"  ✅ pdfplumber 提取成功: {len(combined_text)} 字元")
                return combined_text, page_count
            
            # 方法 2: 如果文字提取失敗，使用 OCR
            if ENABLE_OCR and OCR_AVAILABLE:
                logger.warning(f"  文字提取結果不足，嘗試使用 OCR...")
                ocr_text, ocr_pages = SimplePDFParser.extract_text_with_ocr(pdf_path)
                
                if ocr_text and len(ocr_text.strip()) > 100:
                    logger.info(f"  ✅ OCR 提取成功: {len(ocr_text)} 字元")
                    return ocr_text, ocr_pages
                else:
                    logger.error(f"  ❌ OCR 提取失敗或結果不足")
            elif ENABLE_OCR and not OCR_AVAILABLE:
                logger.error(f"  ❌ OCR 已啟用但相關套件未安裝")
            else:
                logger.warning(f"  OCR 未啟用，跳過")
            
            return combined_text, page_count
            
        except Exception as e:
            logger.error(f"  提取文字失敗: {e}")
            
            # 最後嘗試：使用 OCR
            if ENABLE_OCR and OCR_AVAILABLE:
                logger.info(f"  最後嘗試使用 OCR...")
                try:
                    ocr_text, ocr_pages = SimplePDFParser.extract_text_with_ocr(pdf_path)
                    if ocr_text:
                        logger.info(f"  ✅ OCR 救援成功: {len(ocr_text)} 字元")
                        return ocr_text, ocr_pages
                except Exception as ocr_error:
                    logger.error(f"  ❌ OCR 救援失敗: {ocr_error}")
            
            return "", 0
    
    @staticmethod
    def extract_text_with_ocr(pdf_path: Path) -> tuple[str, int]:
        """使用 OCR 提取 PDF 文字"""
        if not OCR_AVAILABLE:
            logger.error("OCR 套件未安裝")
            return "", 0
        
        try:
            logger.info(f"  開始 OCR 處理 (DPI: {OCR_DPI}, 語言: {OCR_LANG})...")
            
            # 將 PDF 轉換為圖片
            images = convert_from_path(
                pdf_path,
                dpi=OCR_DPI,
                fmt='png',
                thread_count=2
            )
            
            logger.info(f"  已轉換為 {len(images)} 張圖片")
            
            # 對每張圖片進行 OCR
            text_parts = []
            for i, image in enumerate(images):
                logger.info(f"  處理第 {i+1}/{len(images)} 頁...")
                
                # 使用 pytesseract 進行 OCR
                page_text = pytesseract.image_to_string(
                    image,
                    lang=OCR_LANG,
                    config='--psm 6'  # 假設單一文字塊
                )
                
                if page_text and page_text.strip():
                    text_parts.append(f"[第 {i + 1} 頁]\n{page_text}")
                    logger.debug(f"    提取了 {len(page_text)} 字元")
            
            combined_text = '\n'.join(text_parts)
            return combined_text, len(images)
            
        except Exception as e:
            logger.error(f"OCR 處理失敗: {e}")
            return "", 0
    
    @staticmethod
    def detect_doc_type(text: str, filename: str) -> str:
        """ 偵測文檔類型 """
        # 移除空格，統一處理簡繁體
        text_normalized = text.replace(' ', '').replace('　', '')
        text_lower = text.lower()
        filename_lower = filename.lower()
        
        # FMEA 分析表 - 優先檢查（最具特徵性）
        fmea_keywords = [
            'FEMA', 'DFMEA', 'dfmea', 'PFMEA', 'pfmea',
            'DFMEA表', 'fmea表', 'FMEA Table',
            '失效模式', '失效分析', '失效模式與影響分析',
            '失效成因分析', '效應分析',
            '風險優先數', 'RPN', 'rpn',
            '嚴重度', '發生度', '難檢度',
            '嚴重度S', '發生度O', '難檢度D',
            '開發案號R/RD', '案件名稱',
        ]
        
        # 檢查是否包含多個 FMEA 特徵
        fmea_score = sum(1 for kw in fmea_keywords if kw.replace(' ', '') in text_normalized)
        
        if fmea_score >= 3:  # 至少包含 3 個 FMEA 特徵
            logger.debug(f"  偵測到 FMEA 分析表關鍵字 (匹配度: {fmea_score})")
            return 'FMEA'
        
        # 設變通知單 - 多種關鍵字組合
        ecn_notice_keywords = [
            '設變通知單', '设变通知单',  # 繁簡體
            '設變申請單號', '设变申请单号',
            '設變申請人', '設變說明'
        ]
        if any(kw.replace(' ', '') in text_normalized for kw in ecn_notice_keywords):
            logger.debug(f"  偵測到設變通知單關鍵字")
            return 'ECN_NOTICE'
        
        # 設變申請單 - 排除 FMEA（因為 FMEA 也可能提到設變）
        ecn_application_keywords = [
            '單號：EC-'
            '申請單位', '緣由', '設變執行', '研發單位',
        ]
        if any(kw.replace(' ', '') in text_normalized for kw in ecn_application_keywords):
            # 確認不是 FMEA
            if fmea_score < 2:
                logger.debug(f"  偵測到設變申請單關鍵字")
                return 'ECN_APPLICATION'
        
        # 客訴 - 多種關鍵字組合
        complaint_keywords = [
            '顧客抱怨處理資料', '異常單號', '來源單號',
            '開單類別', '客戶代號/名稱', '客戶抱怨', '不良數/批',
            '抱怨內容描述', '抱怨内容分析', '不良率'
        ]
        if any(kw.replace(' ', '') in text_normalized for kw in complaint_keywords):
            # 確認不是客訴案的 FMEA
            if fmea_score < 2:
                logger.debug(f"  偵測到客訴關鍵字")
                return 'COMPLAINT'
        
        # 從檔名判斷（備用）
        if any(kw in filename_lower for kw in ['fmea', 'dfmea', 'pfmea', 'FMEA']):
            logger.debug(f"  根據檔名判斷為 FMEA")
            return 'FMEA'
        
        if any(kw in filename_lower for kw in ['ecn', '申請單', '申請', 'engineering change']):
            logger.debug(f"  根據檔名判斷為設變申請單")
            return 'ECN_APPLICATION'
        
        if any(kw in filename_lower for kw in ['complaint', '客訴', '客诉', 'cpr']):
            logger.debug(f"  根據檔名判斷為客訴")
            return 'COMPLAINT'
        
        if any(kw in filename_lower for kw in ['notice', '通知', '通知單']):
            logger.debug(f"  根據檔名判斷為設變通知單")
            return 'ECN_NOTICE'
        
        logger.warning(f"  無法識別文檔類型，標記為 OTHER")
        return 'OTHER'

# ========== PDF 處理器 ==========
class PDFProcessor:
    """PDF 處理器"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.parser = SimplePDFParser()
        self.state = self.load_state()
        
        # 確保目錄存在
        for directory in [PDF_WATCH_DIR, PDF_PROCESSING_DIR, PDF_DONE_DIR, PDF_ERROR_DIR]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def load_state(self) -> Dict:
        """載入處理狀態"""
        state_file = Path('/state/processor_state.json')
        if state_file.exists():
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {}
    
    def save_state(self):
        """儲存處理狀態"""
        state_file = Path('/state/processor_state.json')
        state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False)
    
    def calculate_file_hash(self, file_path: Path) -> str:
        """計算檔案 MD5 hash"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def process_pdf(self, pdf_path: Path) -> bool:
        """處理單個 PDF 檔案"""
        start_time = time.time()
        
        try:
            file_size = pdf_path.stat().st_size
            file_hash = self.calculate_file_hash(pdf_path)
            
            # 檢查是否已處理過
            if self.db.check_document_exists(file_hash):
                logger.info(f"⏭️  跳過已處理: {pdf_path.name}")
                self.db.log_processing(pdf_path.name, file_hash, "skipped")
                return True
            
            logger.info(f"📄 開始處理: {pdf_path.name} ({file_size/1024:.1f} KB)")
            
            # 記錄開始處理
            self.db.log_processing(pdf_path.name, file_hash, "processing")
            
            # 移到處理中目錄
            processing_path = PDF_PROCESSING_DIR / pdf_path.name
            pdf_path.rename(processing_path)
            
            # 提取文字
            text, page_count = self.parser.extract_text(processing_path)
            
            if not text:
                raise ValueError("無法提取文字內容")
            
            logger.info(f"  提取文字: {len(text)} 字元, {page_count} 頁")
            
            # 偵測文檔類型
            doc_type = self.parser.detect_doc_type(text, processing_path.stem)
            
            # 建立文檔物件
            doc = TechnicalDocument(
                doc_id=file_hash,
                doc_type=doc_type,
                file_name=processing_path.name,
                file_size=file_size,
                page_count=page_count,
                content=text,
                created_at=datetime.now()
            )
            
            # 存入資料庫
            if self.db.save_document(doc):
                # 成功：移到完成目錄
                done_path = PDF_DONE_DIR / processing_path.name
                processing_path.rename(done_path)
                
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
                    "doc_type": doc_type
                }
                self.save_state()
                
                logger.info(f"  ✅ 成功處理，耗時: {process_time}ms")
                return True
            else:
                raise Exception("資料庫儲存失敗")
                
        except Exception as e:
            logger.error(f"  ❌ 處理失敗: {e}")
            
            # 移到錯誤目錄
            try:
                if processing_path.exists():
                    error_path = PDF_ERROR_DIR / processing_path.name
                    processing_path.rename(error_path)
            except:
                pass
            
            process_time = int((time.time() - start_time) * 1000)
            self.db.log_processing(
                pdf_path.name,
                file_hash,
                "error",
                error_message=str(e),
                process_time_ms=process_time
            )
            
            return False
    
    def scan_and_process(self):
        """掃描並處理 PDF 檔案"""
        pdf_files = sorted(PDF_WATCH_DIR.glob('*.pdf'))[:BATCH_SIZE]
        
        if not pdf_files:
            logger.debug("沒有待處理的 PDF 檔案")
            return
        
        logger.info(f"🔍 發現 {len(pdf_files)} 個 PDF 檔案")
        
        for pdf_file in pdf_files:
            self.process_pdf(pdf_file)
    
    def run(self):
        """主執行迴圈"""
        logger.info("=" * 60)
        logger.info("PDF 處理器服務啟動")
        logger.info(f"監控目錄: {PDF_WATCH_DIR}")
        logger.info(f"掃描間隔: {SCAN_INTERVAL} 秒")
        logger.info(f"批次大小: {BATCH_SIZE}")
        logger.info("=" * 60)
        
        try:
            while True:
                self.scan_and_process()
                time.sleep(SCAN_INTERVAL)
        except KeyboardInterrupt:
            logger.info("\n收到中斷訊號，正在關閉...")
        finally:
            self.db.close()
            logger.info("服務已停止")

# ========== 主程式 ==========
if __name__ == "__main__":
    processor = PDFProcessor()
    processor.run()
