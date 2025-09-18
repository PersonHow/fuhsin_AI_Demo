#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF 表單解析器 - 詳細註解版
專門處理福興工業的技術文件表單
"""

import pdfplumber
import pandas as pd
from typing import Dict, List, Optional, Tuple
import re
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class DetailedPDFTableExtractor:
    """
    PDF 表單解析器 - 針對福興工業文件優化
    
    PDF 解析原理：
    1. PDF 是向量格式，文字和表格是以座標形式存儲
    2. pdfplumber 會分析文字的位置關係來識別表格
    3. 通過行列對齊來重建表格結構
    """
    
    def __init__(self):
        # 定義福興文件的特定欄位名稱（用於識別表格）
        self.fuhsin_table_headers = {
            # 設變通知單的欄位
            'ecn': [
                '簽核人員', '簽核時間', '簽核內容',
                '品保課', '製造課', '倉儲課',
                '設變前', '設變後', '說明'
            ],
            # 產品規格表的欄位
            'spec': [
                '品號', '品名', '規格', '材質', '表面處理',
                '數量', '單位', '備註', '圖號'
            ],
            # DFMEA 表格欄位
            'dfmea': [
                '失效模式', '失效原因', '失效影響',
                '嚴重度', '發生度', '偵測度', 'RPN'
            ],
            # 客訴單欄位
            'complaint': [
                '客戶名稱', '抱怨內容', '原因分析',
                '矯正措施', '預防措施', '結案日期'
            ]
        }
    
    def extract_with_detailed_explanation(self, pdf_path: str) -> Dict:
        """
        主解析函數 - 附詳細說明
        
        Returns:
            包含解析結果和過程說明的字典
        """
        results = {
            'tables': [],
            'forms': [],
            'text_blocks': [],
            'extraction_log': []
        }
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                logger.info(f"📖 開啟 PDF: {pdf_path}")
                logger.info(f"   頁數: {len(pdf.pages)}")
                
                for page_num, page in enumerate(pdf.pages, 1):
                    logger.info(f"\n========== 第 {page_num} 頁 ==========")
                    
                    # ====== 步驟 1: 分析頁面佈局 ======
                    self._analyze_page_layout(page, results)
                    
                    # ====== 步驟 2: 提取表格 ======
                    tables = self._extract_tables_with_explanation(page, page_num, results)
                    
                    # ====== 步驟 3: 提取表單資料 ======
                    forms = self._extract_form_fields(page, page_num, results)
                    
                    # ====== 步驟 4: 提取文字區塊 ======
                    text_blocks = self._extract_text_blocks(page, page_num, results)
                    
        except Exception as e:
            logger.error(f"❌ 解析錯誤: {e}")
            results['extraction_log'].append(f"錯誤: {str(e)}")
        
        return results
    
    def _analyze_page_layout(self, page, results: Dict):
        """
        步驟 1: 分析頁面佈局
        
        原理：
        - PDF 頁面有邊界框 (bounding box)
        - 每個文字、線條都有 x,y 座標
        - 通過分析座標分布可以識別版面配置
        """
        # 取得頁面尺寸
        width = page.width
        height = page.height
        
        logger.info(f"📐 頁面尺寸: {width:.1f} x {height:.1f} points")
        logger.info(f"   (1 point = 1/72 inch)")
        
        # 分析文字密度分布
        chars = page.chars  # 所有字符及其位置
        if chars:
            # 計算文字在頁面上的分布
            x_coords = [c['x0'] for c in chars]
            y_coords = [c['top'] for c in chars]
            
            # 找出主要的文字區域
            left_margin = min(x_coords)
            right_margin = max(x_coords)
            top_margin = min(y_coords)
            bottom_margin = max(y_coords)
            
            logger.info(f"📝 文字區域:")
            logger.info(f"   左邊界: {left_margin:.1f}")
            logger.info(f"   右邊界: {right_margin:.1f}")
            logger.info(f"   上邊界: {top_margin:.1f}")
            logger.info(f"   下邊界: {bottom_margin:.1f}")
            
            results['extraction_log'].append(
                f"頁面文字區域: ({left_margin:.1f}, {top_margin:.1f}) 到 ({right_margin:.1f}, {bottom_margin:.1f})"
            )
    
    def _extract_tables_with_explanation(self, page, page_num: int, results: Dict) -> List[pd.DataFrame]:
        """
        步驟 2: 提取表格 - 詳細解釋版
        
        pdfplumber 表格識別原理：
        1. 尋找垂直和水平線條
        2. 線條交叉形成格子
        3. 將文字分配到對應的格子中
        """
        extracted_tables = []
        
        # 使用不同的表格提取策略
        strategies = [
            {
                'name': '明確線條策略',
                'settings': {
                    "vertical_strategy": "lines",      # 使用線條識別垂直邊界
                    "horizontal_strategy": "lines",    # 使用線條識別水平邊界
                    "explicit_vertical_lines": [],     # 可指定額外的垂直線
                    "explicit_horizontal_lines": [],   # 可指定額外的水平線
                }
            },
            {
                'name': '文字對齊策略',
                'settings': {
                    "vertical_strategy": "text",       # 使用文字邊緣識別垂直邊界
                    "horizontal_strategy": "text",     # 使用文字邊緣識別水平邊界
                    "snap_tolerance": 3,              # 對齊容差（點）
                    "join_tolerance": 3,              # 連接容差（點）
                    "edge_min_length": 3,             # 最小邊緣長度
                    "min_words_vertical": 1,          # 垂直最少文字數
                    "min_words_horizontal": 1,        # 水平最少文字數
                }
            }
        ]
        
        for strategy in strategies:
            logger.info(f"\n🔍 嘗試策略: {strategy['name']}")
            
            try:
                # 使用 pdfplumber 的 find_tables 方法
                # 這個方法會返回 Table 物件列表
                tables = page.find_tables(table_settings=strategy['settings'])
                
                if tables:
                    logger.info(f"   ✅ 找到 {len(tables)} 個表格")
                    
                    for i, table in enumerate(tables):
                        # 取得表格的邊界框
                        bbox = table.bbox  # (x0, top, x1, bottom)
                        logger.info(f"   表格 {i+1} 位置: {bbox}")
                        
                        # 提取表格資料
                        data = table.extract()
                        
                        # 分析表格結構
                        if data:
                            rows = len(data)
                            cols = len(data[0]) if data[0] else 0
                            logger.info(f"   表格 {i+1} 尺寸: {rows} 行 x {cols} 列")
                            
                            # 檢查是否為福興特定表格類型
                            table_type = self._identify_table_type(data)
                            if table_type:
                                logger.info(f"   📊 識別為: {table_type} 表格")
                                
                                # 根據表格類型進行特殊處理
                                processed_data = self._process_fuhsin_table(data, table_type)
                                
                                # 轉換為 DataFrame
                                df = self._create_dataframe(processed_data, table_type)
                                extracted_tables.append(df)
                                
                                # 記錄詳細資訊
                                results['tables'].append({
                                    'page': page_num,
                                    'type': table_type,
                                    'bbox': bbox,
                                    'shape': (rows, cols),
                                    'data': df.to_dict()
                                })
                                
                                # 解釋提取的內容
                                self._explain_table_content(df, table_type, results)
                else:
                    logger.info(f"   ❌ 未找到表格")
                    
            except Exception as e:
                logger.warning(f"   ⚠️ 策略失敗: {e}")
        
        return extracted_tables
    
    def _identify_table_type(self, data: List[List]) -> Optional[str]:
        """
        識別表格類型
        
        方法：
        1. 檢查第一行（通常是標題）
        2. 比對已知的福興表格格式
        3. 計算相似度分數
        """
        if not data or not data[0]:
            return None
        
        # 取得第一行作為潛在標題
        first_row = [str(cell).strip() if cell else '' for cell in data[0]]
        
        logger.debug(f"   第一行內容: {first_row}")
        
        # 與已知表格類型比對
        for table_type, headers in self.fuhsin_table_headers.items():
            # 計算匹配分數
            matches = sum(1 for header in headers if any(header in cell for cell in first_row))
            
            if matches >= len(headers) * 0.3:  # 30% 匹配即認為是該類型
                logger.info(f"   🎯 匹配 {table_type}: {matches}/{len(headers)} 個欄位")
                return table_type
        
        return 'general'  # 未識別的一般表格
    
    def _process_fuhsin_table(self, data: List[List], table_type: str) -> List[List]:
        """
        根據福興表格特性進行處理
        
        常見問題與解決方案：
        1. 合併儲存格：PDF 中會變成空白格，需要向前填充
        2. 多行標題：需要合併成單一標題
        3. 空白行：需要過濾掉
        4. 特殊字符：需要清理
        """
        processed = []
        
        for row_idx, row in enumerate(data):
            # 清理每個單元格
            cleaned_row = []
            for col_idx, cell in enumerate(row):
                if cell is None:
                    # 處理合併儲存格（向左或向上查找）
                    if col_idx > 0 and row[col_idx-1]:
                        cell = ''  # 保持空白，稍後可能需要合併
                    elif row_idx > 0 and data[row_idx-1][col_idx]:
                        cell = ''  # 垂直合併的情況
                    else:
                        cell = ''
                
                # 清理文字
                cell = str(cell).strip()
                cell = re.sub(r'\s+', ' ', cell)  # 多個空白合併為一個
                cell = cell.replace('\n', ' ')     # 換行符替換為空格
                
                cleaned_row.append(cell)
            
            # 過濾全空白行
            if any(cell for cell in cleaned_row):
                processed.append(cleaned_row)
        
        return processed
    
    def _create_dataframe(self, data: List[List], table_type: str) -> pd.DataFrame:
        """
        建立 DataFrame 並進行後處理
        
        特殊處理：
        1. 自動識別標題行
        2. 設定正確的資料類型
        3. 處理缺失值
        """
        if not data:
            return pd.DataFrame()
        
        # 假設第一行是標題
        if len(data) > 1:
            df = pd.DataFrame(data[1:], columns=data[0])
        else:
            df = pd.DataFrame(data)
        
        # 根據表格類型進行特殊處理
        if table_type == 'ecn':
            # 設變通知單特殊處理
            df = self._process_ecn_dataframe(df)
        elif table_type == 'spec':
            # 產品規格表特殊處理
            df = self._process_spec_dataframe(df)
        
        return df
    
    def _process_ecn_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """處理設變通知單 DataFrame"""
        # 識別日期欄位並轉換格式
        date_columns = df.columns[df.columns.str.contains('日期|時間', na=False)]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y/%m/%d')
        
        return df
    
    def _process_spec_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """處理產品規格表 DataFrame"""
        # 識別數值欄位
        numeric_columns = df.columns[df.columns.str.contains('數量|數値|公差', na=False)]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def _extract_form_fields(self, page, page_num: int, results: Dict) -> Dict:
        """
        步驟 3: 提取表單欄位（非表格的結構化資料）
        
        原理：
        福興的文件常有這種格式：
        欄位名稱：欄位值
        或
        □ 選項1 ☑ 選項2 □ 選項3
        """
        form_data = {}
        
        # 取得頁面所有文字
        text = page.extract_text()
        if not text:
            return form_data
        
        # 模式 1: 標籤:值 格式
        label_value_pattern = r'([^：:\n]+)[：:][\s]*([^：:\n]+)'
        matches = re.findall(label_value_pattern, text)
        
        for label, value in matches:
            label = label.strip()
            value = value.strip()
            
            # 過濾掉太長的內容（可能是段落而非欄位）
            if len(label) < 20 and len(value) < 100:
                form_data[label] = value
                logger.debug(f"   📋 表單欄位: {label} = {value[:30]}...")
        
        # 模式 2: 核取方塊
        checkbox_pattern = r'[□☑]\s*([^□☑\n]+)'
        checkboxes = re.findall(checkbox_pattern, text)
        
        if checkboxes:
            form_data['checkboxes'] = checkboxes
            logger.debug(f"   ☑ 核取方塊: {checkboxes}")
        
        # 模式 3: 福興特定格式 - 簽核欄
        if '簽核' in text or '審核' in text:
            approval_data = self._extract_approval_fields(text)
            if approval_data:
                form_data['approvals'] = approval_data
                logger.info(f"   📝 找到簽核資訊: {len(approval_data)} 筆")
        
        if form_data:
            results['forms'].append({
                'page': page_num,
                'fields': form_data
            })
        
        return form_data
    
    def _extract_approval_fields(self, text: str) -> List[Dict]:
        """
        提取簽核欄位
        福興文件的簽核格式通常是表格式的
        """
        approvals = []
        
        # 簽核模式：部門/人員 + 日期
        approval_pattern = r'(品保課|製造課|倉儲課|[\u4e00-\u9fff]+課)[^0-9]*([\d/\-]+)'
        
        for dept, date in re.findall(approval_pattern, text):
            approvals.append({
                'department': dept,
                'date': date
            })
        
        return approvals
    
    def _extract_text_blocks(self, page, page_num: int, results: Dict) -> List[Dict]:
        """
        步驟 4: 提取文字區塊（段落文字）
        
        用於提取：
        - 問題描述
        - 改善說明
        - 備註內容
        """
        text_blocks = []
        
        # 使用 pdfplumber 的 extract_text_lines 取得更精確的文字位置
        lines = page.extract_text_lines()
        
        current_block = []
        current_y = None
        
        for line in lines:
            # 判斷是否為新段落（Y座標差異大於閾值）
            if current_y is None:
                current_y = line['top']
                current_block = [line['text']]
            elif abs(line['top'] - current_y) > 20:  # 新段落
                if current_block:
                    block_text = ' '.join(current_block)
                    if len(block_text) > 50:  # 只保留較長的文字區塊
                        text_blocks.append({
                            'text': block_text,
                            'position': current_y
                        })
                current_block = [line['text']]
                current_y = line['top']
            else:
                current_block.append(line['text'])
        
        # 處理最後一個區塊
        if current_block:
            block_text = ' '.join(current_block)
            if len(block_text) > 50:
                text_blocks.append({
                    'text': block_text,
                    'position': current_y
                })
        
        if text_blocks:
            logger.info(f"   📄 找到 {len(text_blocks)} 個文字區塊")
            results['text_blocks'].extend([{
                'page': page_num,
                **block
            } for block in text_blocks])
        
        return text_blocks
    
    def _explain_table_content(self, df: pd.DataFrame, table_type: str, results: Dict):
        """
        解釋提取的表格內容
        """
        explanation = []
        
        if table_type == 'ecn':
            explanation.append("設變通知單內容：")
            if '設變前' in df.columns and '設變後' in df.columns:
                for idx, row in df.iterrows():
                    explanation.append(f"  - 變更項目 {idx+1}:")
                    explanation.append(f"    設變前: {row.get('設變前', 'N/A')}")
                    explanation.append(f"    設變後: {row.get('設變後', 'N/A')}")
        
        elif table_type == 'spec':
            explanation.append("產品規格表內容：")
            if '品號' in df.columns:
                for idx, row in df.iterrows():
                    explanation.append(f"  - 產品: {row.get('品號', 'N/A')}")
                    explanation.append(f"    規格: {row.get('規格', 'N/A')}")
        
        if explanation:
            results['extraction_log'].extend(explanation)
            for line in explanation:
                logger.info(line)


# ========== 使用範例 ==========
def demo_extraction():
    """
    示範如何使用解析器
    """
    extractor = DetailedPDFTableExtractor()
    
    # 假設有一個福興的 PDF 檔案
    pdf_path = "EC-K-28-C-083.pdf"
    
    # 執行解析
    results = extractor.extract_with_detailed_explanation(pdf_path)
    
    # 輸出結果
    print("\n" + "="*60)
    print("📊 解析結果摘要")
    print("="*60)
    
    print(f"\n找到 {len(results['tables'])} 個表格")
    for table in results['tables']:
        print(f"  - 第 {table['page']} 頁: {table['type']} 表格 ({table['shape'][0]}x{table['shape'][1]})")
    
    print(f"\n找到 {len(results['forms'])} 個表單")
    for form in results['forms']:
        print(f"  - 第 {form['page']} 頁: {len(form['fields'])} 個欄位")
    
    print(f"\n找到 {len(results['text_blocks'])} 個文字區塊")
    
    print("\n📝 解析過程記錄:")
    for log in results['extraction_log'][-10:]:  # 顯示最後10條
        print(f"  {log}")
    
    return results

if __name__ == "__main__":
    demo_extraction()
