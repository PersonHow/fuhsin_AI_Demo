#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件結構化解析服務 - 修正版
增強解析準確度，正確提取文件編號、部門、產品等資訊
"""

import os, sys, json, re, time
import logging, hashlib, pymysql, signal
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

# 環境變數配置
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "fuhsin_erp_demo")

FILE_SERVICE_BASE_URL = os.getenv("FILE_SERVICE_BASE_URL", "http://localhost:8088")
PDF_STORAGE_PATH = os.getenv("PDF_STORAGE_PATH", "/mnt/pdf/files")  # 修正：確保路徑正確

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
    def clean_ocr_noise(cls, text: str) -> str:
        """清理 OCR 雜訊和亂碼"""
        # 移除常見的 OCR 亂碼模式
        noise_patterns = [
            r'[A-Z]{2,}\s+[A-Z]{2,}\s+[A-Z]{2,}\s+[A-Z]{2,}',  # 連續大寫字母組合
            r'about:blank',  # 瀏覽器產生的文字
            r'\d{4}/\d{1,2}/\d{1,2}\s+[上下]午\d{1,2}:\d{2}',  # 時間戳記
            r'\[OCR\s+第\s*\d+\s*頁\]',  # OCR 頁碼標記
            r'@+',  # 多個 @ 符號
            r'\|+',  # 多個管道符號（表格殘留）
            r'[\x00-\x1F\x7F-\x9F]',  # 控制字符
            r'[^\u4e00-\u9fff\u3000-\u303f\w\s\-\.\,\:\;\!\?\(\)\[\]\/\+\=\*\&\%\$\#\@]',  # 非中文、英文、數字、標點
        ]
        
        cleaned = text
        for pattern in noise_patterns:
            cleaned = re.sub(pattern, ' ', cleaned)
        
        # 清理多餘空白
        cleaned = re.sub(r'\s+', ' ', cleaned)
        cleaned = re.sub(r'\n\s*\n', '\n', cleaned)
        
        return cleaned.strip()

    @classmethod
    def extract_key_info(cls, content: str, doc_type: str) -> Dict[str, any]:
        """提取關鍵資訊（改進版 - 加強 related_doc_numbers）"""
        key_info = {}
        cleaned_content = cls.clean_ocr_noise(content)
        
        # 收集所有相關文件編號（包含品號、單號等）
        related_numbers = []
        
        # 文件編號提取（優化版）
        doc_patterns = {
            'CPR': r'CPR[-\s]?K[-\s]?Q[-\s]?\d{2}[-\s]?[A-Z][-\s]?B?\d{3}',
            'ECN': r'EC[-\s]?K[-\s]?\d{2}[-\s]?[A-Z][-\s]?\d{3}(?:[-\s]?\d)?',
            'LD': r'L[DO]?\d{6,7}[A-Z]?\d?',
            'FORM': r'[A-Z]{2,4}[-\s]?\d{2,4}[-\s]?[A-Z]?\d*',
            'GENERAL_DOC': r'[A-Z]{2,3}[-\s]?\d{2,6}[-\s]?[A-Z]?'
        }
        
        # 提取所有匹配的文件編號
        for doc_key, pattern in doc_patterns.items():
            matches = re.findall(pattern, cleaned_content, re.IGNORECASE)
            for match in matches:
                clean_number = re.sub(r'\s+', '-', match.strip())
                if not key_info.get('doc_number'):
                    key_info['doc_number'] = clean_number  # 第一個作為主要編號
                related_numbers.append(clean_number)
        
        # 設變申請單號（針對ECN文件）
        ecn_patterns = [
            r'設變申請單號\s*[:：]\s*(EC[-\s]?K[-\s]?\d{2}[-\s]?[A-Z][-\s]?\d{3})',
            r'ECN\s*[:：]?\s*(EC[-\s]?K[-\s]?\d{2}[-\s]?[A-Z][-\s]?\d{3})',
            r'變更單號\s*[:：]\s*(EC[-\s]?K[-\s]?\d{2}[-\s]?[A-Z][-\s]?\d{3})'
        ]
        
        for pattern in ecn_patterns:
            ecn_matches = re.findall(pattern, cleaned_content)
            for ecn_match in ecn_matches:
                ecn_number = re.sub(r'\s+', '-', ecn_match.strip())
                if not key_info.get('ecn_number'):
                    key_info['ecn_number'] = ecn_number
                related_numbers.append(ecn_number)
        
        # 品號提取（改進版 - 更全面）
        product_patterns = [
            r'品號\s*[:：]\s*([A-Z0-9\-、]+)',
            r'料號\s*[:：]\s*([A-Z0-9\-、]+)',
            r'產品編號\s*[:：]\s*([A-Z0-9\-、]+)',
            r'[GLF]\d{2}[-\s]?[A-Z0-9]+[-\s]?[A-Z0-9]*',  # 標準產品編號格式
            r'[A-Z]\d{2,3}[-\s]?[A-Z0-9]{3,}',            # 其他產品編號格式
            r'OB\d+-[A-Z0-9\-]+',                         # OB 系列
        ]
        
        products = []
        for pattern in product_patterns:
            matches = re.findall(pattern, cleaned_content)
            for match in matches:
                # 清理並分割多個品號
                if '、' in match:
                    items = [p.strip() for p in match.split('、') if p.strip()]
                    products.extend(items)
                    related_numbers.extend(items)
                elif '，' in match:
                    items = [p.strip() for p in match.split('，') if p.strip()]
                    products.extend(items)
                    related_numbers.extend(items)
                else:
                    clean_product = match.strip()
                    if clean_product:
                        products.append(clean_product)
                        related_numbers.append(clean_product)
        
        # 其他可能的相關編號（工單、採購單等）
        other_patterns = [
            r'工單號\s*[:：]\s*([A-Z0-9\-]+)',
            r'採購單號\s*[:：]\s*([A-Z0-9\-]+)',
            r'客戶訂單號\s*[:：]\s*([A-Z0-9\-]+)',
            r'規格書編號\s*[:：]\s*([A-Z0-9\-]+)',
            r'圖號\s*[:：]\s*([A-Z0-9\-]+)',
        ]
        
        for pattern in other_patterns:
            matches = re.findall(pattern, cleaned_content)
            for match in matches:
                clean_number = match.strip()
                if clean_number:
                    related_numbers.append(clean_number)
        
        # 去重並限制數量
        unique_products = list(dict.fromkeys(products))[:10]
        if unique_products:
            key_info['product_codes'] = unique_products
        
        # 去重相關編號並排序（主要編號放前面）
        unique_related = []
        seen = set()
        
        # 先加入主要編號
        if key_info.get('doc_number') and key_info['doc_number'] not in seen:
            unique_related.append(key_info['doc_number'])
            seen.add(key_info['doc_number'])
            
        if key_info.get('ecn_number') and key_info['ecn_number'] not in seen:
            unique_related.append(key_info['ecn_number'])
            seen.add(key_info['ecn_number'])
        
        # 再加入其他相關編號
        for num in related_numbers:
            if num not in seen and len(unique_related) < 20:
                unique_related.append(num)
                seen.add(num)
        
        key_info['related_doc_numbers'] = unique_related
        
        return key_info

    @classmethod
    def extract_description(cls, content: str, doc_type: str) -> str:
        """提取核心描述（如設變說明）"""
        cleaned_content = cls.clean_ocr_noise(content)
        
        # 根據文件類型提取不同的描述
        description_patterns = {
            'ECN': [
                r'設變說明\s*[:：]\s*(.+?)(?:設變申請人|申請人|$)',
                r'變更說明\s*[:：]\s*(.+?)(?:申請|負責|$)',
                r'說明\s*[:：]\s*(.+?)(?:\n|申請|$)'
            ],
            'CPR': [
                r'客訴內容\s*[:：]\s*(.+?)(?:處理|回覆|$)',
                r'問題描述\s*[:：]\s*(.+?)(?:原因|分析|$)',
                r'不良現象\s*[:：]\s*(.+?)(?:原因|對策|$)'
            ],
            'DEFAULT': [
                r'說明\s*[:：]\s*(.+?)(?:\n{2}|申請|負責|$)',
                r'內容\s*[:：]\s*(.+?)(?:\n{2}|日期|$)',
                r'描述\s*[:：]\s*(.+?)(?:\n{2}|備註|$)'
            ]
        }
        
        patterns = description_patterns.get(doc_type, description_patterns['DEFAULT'])
        
        for pattern in patterns:
            match = re.search(pattern, cleaned_content, re.DOTALL)
            if match:
                description = match.group(1).strip()
                # 清理描述中的雜訊
                description = cls.clean_ocr_noise(description)
                # 限制長度
                if len(description) > 500:
                    description = description[:497] + '...'
                return description
        
        return ""

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
        cleaned_content = cls.clean_ocr_noise(content)
        units = []
        
        # 責任單位模式
        patterns = [
            r'責任單位\s*[:：]\s*([^,，\n]+)',
            r'負責單位\s*[:：]\s*([^,，\n]+)',
            r'承辦單位\s*[:：]\s*([^,，\n]+)',
            r'執行單位\s*[:：]\s*([^,，\n]+)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, cleaned_content)
            units.extend(matches)
        
        # 清理和去重
        clean_units = []
        for unit in units:
            unit = unit.strip()
            if unit and len(unit) < 20 and unit not in clean_units:
                # 嘗試映射到標準部門名稱
                for key, value in cls.DEPARTMENT_MAPPING.items():
                    if key in unit:
                        unit = value
                        break
                clean_units.append(unit)
        
        return clean_units[:5]  # 最多返回5個單位
    
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
    def generate_summary(cls, content: str, doc_type: str = None, file_name: str = "") -> Dict[str, any]:
        """生成精簡摘要並保存原始內容"""
        # 保存原始內容（限制長度以避免過大）
        original_content = content[:5000] if len(content) > 5000 else content
        
        # 清理內容用於分析
        cleaned_content = cls.clean_ocr_noise(content)
        
        # 提取關鍵資訊
        key_info = cls.extract_key_info(cleaned_content, doc_type or '')
        
        # 提取描述
        description = cls.extract_description(cleaned_content, doc_type or '')
        
        # 構建精簡摘要（按照您的格式要求）
        summary_parts = []
        
        # 加入文件編號
        if 'doc_number' in key_info and key_info['doc_number']:
            summary_parts.append(f"單號{key_info['doc_number']}")
        
        # 加入ECN編號（如果有）
        if 'ecn_number' in key_info and key_info['ecn_number']:
            summary_parts.append(f"設變申請單號：{key_info['ecn_number']}")
        
        # 加入品號（如果有，限制3個）
        if 'product_codes' in key_info and key_info['product_codes']:
            products_str = '、'.join(key_info['product_codes'][:3])
            summary_parts.append(f"品號：{products_str}")
        
        # 加入描述（根據文件類型調整）
        if description:
            if doc_type == 'ECN':
                summary_parts.append(f"設變說明：{description}")
            elif doc_type == 'Complaint' or doc_type == 'CPR':
                summary_parts.append(f"客訴內容：{description}")
            elif doc_type == 'TestReport':
                summary_parts.append(f"測試內容：{description}")
            else:
                summary_parts.append(f"說明：{description}")
        
        # 如果沒有找到任何關鍵資訊，從內容中提取重要句子
        if not summary_parts:
            lines = cleaned_content.split('\n')
            meaningful_lines = []
            for line in lines[:15]:  # 看前15行
                line = line.strip()
                # 過濾掉無意義的行
                if (len(line) > 8 and 
                    not line.startswith('=') and 
                    not line.startswith('-') and
                    not re.match(r'^\d{4}/\d{1,2}/\d{1,2}', line) and  # 日期行
                    'about:blank' not in line):
                    meaningful_lines.append(line)
                    if len(meaningful_lines) >= 2:
                        break
            
            if meaningful_lines:
                summary_parts = meaningful_lines[:2]
        
        # 組合摘要
        summary = ' '.join(summary_parts)
        
        # 限制摘要長度
        if len(summary) > 300:
            summary = summary[:297] + '...'
        
        # 生成向量檢索友好的關鍵字
        keywords = []
        
        # 1. 文件類型相關關鍵字
        if doc_type:
            keywords.append(doc_type)
            type_keywords = {
                'ECN': ['設變', '工程變更', '變更單', '設計變更'],
                'Complaint': ['客訴', '客戶投訴', '品質問題', '客戶反映'],
                'CPR': ['客訴', '客戶投訴', '品質問題'],
                'FMEA': ['失效分析', '風險評估', 'FMEA'],
                'TestReport': ['測試', '檢驗', '報告', '品質檢測'],
                'Specification': ['規格', '規範', '標準'],
                'WorkOrder': ['工單', '生產', '製造'],
                'PurchaseOrder': ['採購', '訂單']
            }
            if doc_type in type_keywords:
                keywords.extend(type_keywords[doc_type])
        
        # 2. 產品相關關鍵字
        if 'product_codes' in key_info and key_info['product_codes']:
            # 加入產品編號
            keywords.extend(key_info['product_codes'][:5])
            
            # 根據產品編號推斷產品系列
            for code in key_info['product_codes']:
                if code.startswith('G'):
                    keywords.extend(['G鎖', '掛鎖'])
                elif code.startswith('L'):
                    keywords.extend(['L鎖', '鎖具'])
                elif code.startswith('T'):
                    keywords.extend(['T鎖'])
                elif code.startswith('OB'):
                    keywords.extend(['OB系列'])
                elif code.startswith('F'):
                    keywords.extend(['五金配件'])
        
        # 3. 技術關鍵字（從內容中提取）
        technical_keywords = [
            ('材料', ['材料', '材質']),
            ('製程', ['製程', '工藝', '加工']),
            ('品質', ['品質', 'QC', 'QA', '檢驗']),
            ('尺寸', ['尺寸', '規格', '公差']),
            ('表面處理', ['表面處理', '電鍍', '塗裝']),
            ('組裝', ['組裝', '裝配']),
            ('包裝', ['包裝', '包裝材料']),
            ('交期', ['交期', '出貨', '交貨']),
            ('成本', ['成本', '價格']),
            ('客戶要求', ['客戶要求', '客戶需求']),
            ('改善', ['改善', '優化', '改進']),
            ('問題', ['問題', '異常', '不良']),
            ('緊急', ['緊急', '急件']),
            ('重要', ['重要', '重大'])
        ]
        
        for keyword, variants in technical_keywords:
            for variant in variants:
                if variant in cleaned_content:
                    keywords.append(keyword)
                    break
        
        # 4. 部門關鍵字
        dept_found = cls.extract_department(cleaned_content)
        if dept_found:
            keywords.append(dept_found)
        
        # 5. 從文件編號和相關編號中提取關鍵字
        if 'related_doc_numbers' in key_info:
            # 加入相關編號作為關鍵字（有助於關聯檢索）
            keywords.extend(key_info['related_doc_numbers'][:5])
        
        # 6. 特殊標記關鍵字
        special_markers = [
            (r'緊急|急件|URGENT', '緊急'),
            (r'重要|重大|IMPORTANT', '重要'),
            (r'保密|機密|CONFIDENTIAL', '機密'),
            (r'客戶要求|客戶指定', '客戶要求'),
            (r'成本降低|降本', '成本優化'),
            (r'品質改善|品質提升', '品質改善'),
            (r'交期縮短|急交', '交期'),
            (r'新產品|新開發', '新產品'),
            (r'停產|EOL', '停產'),
            (r'量產|批量', '量產')
        ]
        
        for pattern, keyword in special_markers:
            if re.search(pattern, cleaned_content, re.IGNORECASE):
                keywords.append(keyword)
        
        # 去重並限制數量（保留順序，重要關鍵字在前）
        unique_keywords = []
        seen = set()
        for keyword in keywords:
            if keyword not in seen and len(unique_keywords) < 20:
                unique_keywords.append(keyword)
                seen.add(keyword)
        
        return {
            'summary': summary,
            'original_extracted_content': original_content,  # 原始內容
            'key_info': key_info,
            'keywords': unique_keywords
        }

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
        """初始化資料庫表 - 包含所有必要欄位"""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                # 建立 structured_documents 表 - 完整版本
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
                    related_doc_numbers JSON,
                    
                    applicant VARCHAR(100),
                    department VARCHAR(100),
                    responsible_units JSON,
                    
                    summary TEXT,
                    original_extracted_content LONGTEXT,
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
        """儲存結構化文件 - 完整版本"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # 處理 JSON 欄位
                json_fields = ['product_codes', 'product_names', 'responsible_units', 'keywords', 'related_doc_numbers']
                for field in json_fields:
                    if field in doc_data and doc_data[field] is not None:
                        # 確保是 list 類型才進行 JSON 序列化
                        if isinstance(doc_data[field], list):
                            doc_data[field] = json.dumps(doc_data[field], ensure_ascii=False)
                        else:
                            # 如果不是 list，轉換為空 list
                            doc_data[field] = json.dumps([], ensure_ascii=False)
                
                # 建立 SQL - 包含所有欄位
                allowed_fields = [
                    'original_doc_id', 'doc_type', 'doc_number', 'doc_date',
                    'file_name', 'file_url', 'file_path', 'file_size', 'file_hash',
                    'product_category', 'product_codes', 'product_names', 'related_doc_numbers',
                    'applicant', 'department', 'responsible_units',
                    'summary', 'original_extracted_content', 'keywords', 'status', 'priority', 'parsed_at'
                ]
                
                # 過濾掉不存在的欄位
                filtered_doc_data = {k: v for k, v in doc_data.items() if k in allowed_fields}
                
                fields = list(filtered_doc_data.keys())
                placeholders = ['%s'] * len(fields)
                update_fields = [f"{f}=VALUES({f})" for f in fields if f != 'original_doc_id']
                
                sql = f"""
                INSERT INTO structured_documents ({', '.join(fields)})
                VALUES ({', '.join(placeholders)})
                ON DUPLICATE KEY UPDATE {', '.join(update_fields)}
                """
                
                cursor.execute(sql, list(filtered_doc_data.values()))
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
        """處理單一文件 - 完整版本"""
        try:
            doc_id = doc.get('doc_id')
            content = doc.get('content', '')
            file_name = doc.get('file_name', '')  # 從 technical_documents 取得
            
            if not content:
                logger.warning(f"文件 {doc_id} 沒有內容")
                return False
                
            # 確保有 file_name
            if not file_name:
                logger.warning(f"文件 {doc_id} 沒有檔案名稱，嘗試查詢資料庫")
                # 如果沒有檔案名稱，嘗試直接查詢
                try:
                    conn = self.db.get_connection()
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT file_name FROM technical_documents WHERE doc_id = %s", (doc_id,))
                        result = cursor.fetchone()
                        if result and result['file_name']:
                            file_name = result['file_name']
                            logger.info(f"從資料庫取得檔案名稱: {file_name}")
                        else:
                            file_name = f"document_{doc_id}.pdf"  # 預設檔名
                            logger.warning(f"無法取得檔案名稱，使用預設: {file_name}")
                except Exception as e:
                    logger.error(f"查詢檔案名稱失敗: {e}")
                    file_name = f"document_{doc_id}.pdf"
            
            logger.info(f"處理文件 {doc_id}, 檔案: {file_name}")
            
            # 建立結構化資料
            structured = {
                'original_doc_id': doc_id,
                'parsed_at': datetime.now().isoformat()
            }
            
            # 文件類型偵測
            doc_type = self.parser.detect_doc_type(content)
            structured['doc_type'] = doc_type
            
            # 使用改進的摘要生成
            summary_result = ImprovedContentParser.generate_summary(
                content, 
                doc_type, 
                file_name
            )
            
            # 設定摘要和原始內容
            structured['summary'] = summary_result['summary']
            structured['original_extracted_content'] = summary_result['original_extracted_content']
            
            # 從關鍵資訊中提取各種編號
            key_info = summary_result['key_info']
            
            # 文件編號
            if 'doc_number' in key_info and key_info['doc_number']:
                structured['doc_number'] = key_info['doc_number']
            else:
                # 嘗試從檔名提取編號
                doc_number = self.parser.extract_doc_number(content, file_name)
                structured['doc_number'] = doc_number if doc_number else f"DOC-{doc_id}"
            
            # 相關文件編號（包含品號、單號等）
            structured['related_doc_numbers'] = key_info.get('related_doc_numbers', [])
            
            # 關鍵字（向量檢索用）
            structured['keywords'] = summary_result.get('keywords', [])
            
            # 其他欄位提取
            structured['doc_date'] = self.parser.extract_date(content)
            structured['applicant'] = self.parser.extract_applicant(content)
            structured['department'] = self.parser.extract_department(content)
            structured['responsible_units'] = ImprovedContentParser.extract_responsible_units(content)
            
            # 產品資訊
            if 'product_codes' in key_info and key_info['product_codes']:
                structured['product_codes'] = key_info['product_codes']
                # 可以進一步實作產品名稱提取
                product_codes, product_names, category = self.parser.extract_product_info(content)
                structured['product_names'] = product_names
                structured['product_category'] = category
            else:
                # 備援方案
                product_codes, product_names, category = self.parser.extract_product_info(content)
                structured['product_codes'] = product_codes
                structured['product_names'] = product_names
                structured['product_category'] = category
            
            # 檔案資訊設定（重要：確保檔案連結正確）
            structured['file_name'] = file_name
            
            # 檔案路徑：本地存儲路徑
            structured['file_path'] = f"{PDF_STORAGE_PATH}/{file_name}"
            
            # 檔案 URL：供 RAG-API 和 file-server 使用
            # 格式：http://localhost:8088/download/{doc_id}/{file_name}
            structured['file_url'] = f"{FILE_SERVICE_BASE_URL}/download/{doc_id}/{file_name}"
            
            # 嘗試取得檔案資訊
            try:
                actual_path = structured['file_path']
                if os.path.exists(actual_path):
                    structured['file_size'] = os.path.getsize(actual_path)
                    with open(actual_path, 'rb') as f:
                        structured['file_hash'] = hashlib.sha256(f.read()).hexdigest()
                    logger.info(f"  檔案存在: {actual_path} ({structured['file_size']} bytes)")
                else:
                    # 嘗試其他可能的路徑
                    alternative_paths = [
                        f"{PDF_STORAGE_PATH}/{doc_id}/{file_name}",  # 可能在子目錄
                        f"/mnt/pdf/files/{doc_id}/{file_name}",
                        f"/app/pdf/files/{file_name}",
                    ]
                    
                    file_found = False
                    for alt_path in alternative_paths:
                        if os.path.exists(alt_path):
                            structured['file_path'] = alt_path
                            structured['file_size'] = os.path.getsize(alt_path)
                            with open(alt_path, 'rb') as f:
                                structured['file_hash'] = hashlib.sha256(f.read()).hexdigest()
                            logger.info(f"  檔案找到於: {alt_path}")
                            file_found = True
                            break
                    
                    if not file_found:
                        structured['file_size'] = 0
                        structured['file_hash'] = ''
                        logger.warning(f"  檔案不存在: {actual_path}")
                        
            except Exception as e:
                logger.warning(f"無法讀取檔案資訊 {file_name}: {e}")
                structured['file_size'] = 0
                structured['file_hash'] = ''
            
            # 優先級判定（基於內容分析）
            priority_keywords_high = ['嚴重', '緊急', '立即', '重大', '停線', '客戶抱怨']
            priority_keywords_normal = ['一般', '例行', '定期']
            
            if any(word in content for word in priority_keywords_high):
                structured['priority'] = 'HIGH'
            elif any(word in content for word in priority_keywords_normal):
                structured['priority'] = 'NORMAL'  
            else:
                structured['priority'] = 'LOW'
            
            structured['status'] = 'PARSED'
            
            # 儲存到資料庫
            success = self.db.save_structured_document(structured)
            
            if success:
                logger.info(f"✅ 文件 {doc_id} 處理成功:")
                logger.info(f"   編號: {structured['doc_number']}")
                logger.info(f"   類型: {doc_type}")
                logger.info(f"   檔案: {structured['file_name']}")
                logger.info(f"   檔案路徑: {structured['file_path']}")
                logger.info(f"   檔案URL: {structured['file_url']}")
                logger.info(f"   相關編號: {structured['related_doc_numbers'][:5]}...")
                logger.info(f"   關鍵字: {structured['keywords'][:5]}...")
                logger.debug(f"   摘要: {structured['summary']}")
            else:
                logger.error(f"❌ 文件 {doc_id} 儲存失敗")
            
            return success
            
        except Exception as e:
            logger.error(f"處理文件失敗 {doc_id}: {e}", exc_info=True)
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
