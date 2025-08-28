#!/usr/bin/env python3
"""
資料轉換器
處理資料清洗、轉換、標準化
"""

import re
import pandas as pd
from typing import Dict, List, Any
from datetime import datetime
from opencc import OpenCC

class DataTransformer:
    """資料轉換器"""
    
    def __init__(self, logger):
        """
        初始化轉換器
        
        Args:
            logger: 日誌記錄器
        """
        self.logger = logger
        
        # 初始化繁簡轉換器
        self.cc_s2t = OpenCC('s2t')  # 簡體轉繁體
        self.cc_t2s = OpenCC('t2s')  # 繁體轉簡體
        
        # 註冊轉換函數
        self.transformers = {
            'normalize_phone': self.normalize_phone,
            'enum_mapping': self.enum_mapping,
            'currency': self.format_currency,
            'boolean': self.to_boolean,
            'datetime': self.format_datetime,
            'trim': self.trim_string,
            'uppercase': self.to_uppercase,
            'lowercase': self.to_lowercase,
            'remove_html': self.remove_html_tags,
            'traditional_chinese': self.to_traditional,
            'simplified_chinese': self.to_simplified
        }
    
    def apply_transformations(self, df: pd.DataFrame, 
                            transformations: List[Dict]) -> pd.DataFrame:
        """
        套用所有轉換規則
        
        Args:
            df: 原始 DataFrame
            transformations: 轉換規則列表
            
        Returns:
            轉換後的 DataFrame
        """
        df = df.copy()
        
        for transform in transformations:
            field = transform['field']
            transform_type = transform['type']
            
            if field not in df.columns:
                self.logger.warning(f"欄位 {field} 不存在，跳過轉換")
                continue
            
            if transform_type not in self.transformers:
                self.logger.warning(f"未知的轉換類型：{transform_type}")
                continue
            
            try:
                # 取得轉換函數
                transformer = self.transformers[transform_type]
                
                # 套用轉換
                if transform_type == 'enum_mapping':
                    df[field] = df[field].apply(
                        lambda x: transformer(x, transform['mapping'])
                    )
                elif transform_type == 'currency':
                    df[field] = df[field].apply(
                        lambda x: transformer(x, transform.get('currency', 'TWD'))
                    )
                else:
                    df[field] = df[field].apply(transformer)
                
                self.logger.debug(f"已套用轉換 {transform_type} 到欄位 {field}")
                
            except Exception as e:
                self.logger.error(f"轉換失敗 {field}/{transform_type}：{str(e)}")
        
        return df
    
    def normalize_phone(self, phone: Any) -> str:
        """
        標準化電話號碼
        
        Args:
            phone: 原始電話號碼
            
        Returns:
            標準化的電話號碼
        """
        if pd.isna(phone):
            return None
        
        phone = str(phone)
        
        # 移除所有非數字字元
        phone = re.sub(r'[^\d+]', '', phone)
        
        # 台灣手機號碼標準化
        if phone.startswith('09') and len(phone) == 10:
            phone = '+886' + phone[1:]
        elif phone.startswith('886'):
            phone = '+' + phone
        
        return phone
    
    def enum_mapping(self, value: Any, mapping: Dict) -> str:
        """
        枚舉值對應
        
        Args:
            value: 原始值
            mapping: 對應字典
            
        Returns:
            對應後的值
        """
        if pd.isna(value):
            return None
        
        return mapping.get(str(value), str(value))
    
    def format_currency(self, amount: Any, currency: str = 'TWD') -> str:
        """
        格式化貨幣
        
        Args:
            amount: 金額
            currency: 貨幣代碼
            
        Returns:
            格式化的貨幣字串
        """
        if pd.isna(amount):
            return None
        
        try:
            amount = float(amount)
            
            if currency == 'TWD':
                return f"NT${amount:,.0f}"
            elif currency == 'USD':
                return f"${amount:,.2f}"
            elif currency == 'CNY':
                return f"¥{amount:,.2f}"
            else:
                return f"{currency} {amount:,.2f}"
                
        except:
            return str(amount)
    
    def to_boolean(self, value: Any) -> bool:
        """
        轉換為布林值
        
        Args:
            value: 原始值
            
        Returns:
            布林值
        """
        if pd.isna(value):
            return None
        
        if isinstance(value, bool):
            return value
        
        value = str(value).lower()
        
        true_values = ['true', '1', 'yes', 'y', 't', '是', '真']
        false_values = ['false', '0', 'no', 'n', 'f', '否', '假']
        
        if value in true_values:
            return True
        elif value in false_values:
            return False
        else:
            return None
    
    def format_datetime(self, dt: Any) -> str:
        """
        格式化日期時間
        
        Args:
            dt: 原始日期時間
            
        Returns:
            ISO 格式的日期時間字串
        """
        if pd.isna(dt):
            return None
        
        try:
            if isinstance(dt, str):
                # 嘗試解析常見格式
                formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y/%m/%d %H:%M:%S',
                    '%Y-%m-%d',
                    '%Y/%m/%d',
                    '%d/%m/%Y',
                    '%d-%m-%Y'
                ]
                
                for fmt in formats:
                    try:
                        dt = datetime.strptime(dt, fmt)
                        break
                    except:
                        continue
            
            if isinstance(dt, datetime):
                return dt.isoformat()
            else:
                return str(dt)
                
        except:
            return str(dt)
    
    def trim_string(self, value: Any) -> str:
        """
        修剪字串空白
        
        Args:
            value: 原始值
            
        Returns:
            修剪後的字串
        """
        if pd.isna(value):
            return None
        
        return str(value).strip()
    
    def to_uppercase(self, value: Any) -> str:
        """
        轉換為大寫
        
        Args:
            value: 原始值
            
        Returns:
            大寫字串
        """
        if pd.isna(value):
            return None
        
        return str(value).upper()
    
    def to_lowercase(self, value: Any) -> str:
        """
        轉換為小寫
        
        Args:
            value: 原始值
            
        Returns:
            小寫字串
        """
        if pd.isna(value):
            return None
        
        return str(value).lower()
    
    def remove_html_tags(self, text: Any) -> str:
        """
        移除 HTML 標籤
        
        Args:
            text: 原始文字
            
        Returns:
            純文字
        """
        if pd.isna(text):
            return None
        
        text = str(text)
        
        # 移除 HTML 標籤
        text = re.sub(r'<[^>]+>', '', text)
        
        # 解碼 HTML entities
        import html
        text = html.unescape(text)
        
        return text.strip()
    
    def to_traditional(self, text: Any) -> str:
        """
        轉換為繁體中文
        
        Args:
            text: 原始文字
            
        Returns:
            繁體中文
        """
        if pd.isna(text):
            return None
        
        return self.cc_s2t.convert(str(text))
    
    def to_simplified(self, text: Any) -> str:
        """
        轉換為簡體中文
        
        Args:
            text: 原始文字
            
        Returns:
            簡體中文
        """
        if pd.isna(text):
            return None
        
        return self.cc_t2s.convert(str(text))
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清理 DataFrame
        
        Args:
            df: 原始 DataFrame
            
        Returns:
            清理後的 DataFrame
        """
        df = df.copy()
        
        # 移除完全重複的行
        original_count = len(df)
        df = df.drop_duplicates()
        if len(df) < original_count:
            self.logger.info(f"移除了 {original_count - len(df)} 筆重複資料")
        
        # 修剪所有字串欄位的空白
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        
        # 替換空字串為 None
        df = df.replace('', None)
        
        return df
    
    def validate_data(self, df: pd.DataFrame, 
                     validation_rules: List[Dict]) -> pd.DataFrame:
        """
        驗證資料
        
        Args:
            df: 要驗證的 DataFrame
            validation_rules: 驗證規則列表
            
        Returns:
            通過驗證的 DataFrame
        """
        df = df.copy()
        invalid_indices = []
        
        for rule in validation_rules:
            field = rule['field']
            rule_type = rule['type']
            
            if field not in df.columns:
                continue
            
            if rule_type == 'required':
                # 必填欄位
                invalid = df[df[field].isna()].index
                invalid_indices.extend(invalid)
                
            elif rule_type == 'unique':
                # 唯一值
                duplicated = df[df.duplicated(subset=[field], keep=False)].index
                invalid_indices.extend(duplicated)
                
            elif rule_type == 'range':
                # 數值範圍
                min_val = rule.get('min')
                max_val = rule.get('max')
                
                if min_val is not None:
                    invalid = df[df[field] < min_val].index
                    invalid_indices.extend(invalid)
                
                if max_val is not None:
                    invalid = df[df[field] > max_val].index
                    invalid_indices.extend(invalid)
                
            elif rule_type == 'pattern':
                # 正規表達式
                pattern = rule['pattern']
                invalid = df[~df[field].astype(str).str.match(pattern)].index
                invalid_indices.extend(invalid)
        
        # 移除無效資料
        invalid_indices = list(set(invalid_indices))
        if invalid_indices:
            self.logger.warning(f"發現 {len(invalid_indices)} 筆無效資料")
            df = df.drop(invalid_indices)
        
        return df
