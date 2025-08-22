#!/usr/bin/env python3
# opencc-converter/convert_dict.py
# -*- coding: utf-8 -*-
"""
IK 詞典 OpenCC 簡繁轉換工具
"""

import os
import sys
import logging
from opencc import OpenCC
from datetime import datetime
import json

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/logs/conversion.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class IKDictConverter:
    def __init__(self, conversion_type='s2twp'):
        """
        初始化轉換器
        conversion_type: 轉換類型 (s2twp = 簡體到台灣繁體，包含詞彙轉換)
        """
        self.cc = OpenCC(conversion_type)
        self.stats = {
            'total_files': 0,
            'total_words': 0,
            'converted_words': 0,
            'duplicate_words': 0,
            'error_lines': 0
        }
        self.custom_map = {}
        self.load_custom_map()
        
    def load_custom_map(self):
        """載入自定義轉換對照表"""
        custom_map_file = '/config/custom/custom_convert_map.txt'
        if os.path.exists(custom_map_file):
            logger.info(f"載入自定義轉換對照表: {custom_map_file}")
            with open(custom_map_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=>' in line:
                        simplified, traditional = line.split('=>')
                        self.custom_map[simplified.strip()] = traditional.strip()
            logger.info(f"載入 {len(self.custom_map)} 個自定義轉換詞彙")
    
    def convert_word(self, word):
        """轉換單個詞彙，優先使用自定義對照表"""
        word = word.strip()
        
        # 首先檢查自定義對照表
        if word in self.custom_map:
            return self.custom_map[word]
        
        # 使用 OpenCC 轉換
        return self.cc.convert(word)
    
    def convert_file(self, input_file, output_file):
        """轉換單個詞典文件"""
        logger.info(f"開始轉換: {os.path.basename(input_file)}")
        
        # 用於去重
        unique_words = set()
        word_list = []
        
        try:
            with open(input_file, 'r', encoding='utf-8') as fin:
                for line_num, line in enumerate(fin, 1):
                    line = line.strip()
                    
                    # 跳過空行和註釋
                    if not line or line.startswith('#'):
                        word_list.append(line)
                        continue
                    
                    try:
                        # 處理可能的詞頻格式
                        parts = line.split()
                        if len(parts) >= 2 and parts[-1].isdigit():
                            word = ' '.join(parts[:-1])
                            freq = parts[-1]
                            converted = self.convert_word(word)
                            
                            if converted not in unique_words:
                                unique_words.add(converted)
                                word_list.append(f"{converted} {freq}")
                                self.stats['converted_words'] += 1
                            else:
                                self.stats['duplicate_words'] += 1
                        else:
                            converted = self.convert_word(line)
                            
                            if converted not in unique_words:
                                unique_words.add(converted)
                                word_list.append(converted)
                                self.stats['converted_words'] += 1
                            else:
                                self.stats['duplicate_words'] += 1
                                
                    except Exception as e:
                        logger.error(f"轉換第 {line_num} 行時出錯: {e}")
                        logger.error(f"問題行: {line}")
                        self.stats['error_lines'] += 1
                        word_list.append(line)  # 保留原始內容
            
            # 寫入輸出文件
            with open(output_file, 'w', encoding='utf-8') as fout:
                # 寫入 BOM
                fout.write('\ufeff')
                # 寫入轉換結果
                for word in word_list:
                    fout.write(word + '\n')
            
            self.stats['total_files'] += 1
            logger.info(f"轉換完成: {os.path.basename(output_file)}")
            logger.info(f"  - 總詞彙數: {len(unique_words)}")
            logger.info(f"  - 過濾重複: {self.stats['duplicate_words']}")
            
        except Exception as e:
            logger.error(f"處理文件時出錯: {e}")
            raise
    
    def convert_all(self):
        """轉換所有詞典文件"""
        input_dir = '/dictionaries/original'
        output_dir = '/dictionaries/converted'
        
        # 確保輸出目錄存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 需要轉換的詞典文件
        dict_files = [
            'main.dic',
            'quantifier.dic',
            'suffix.dic',
            'surname.dic',
            'stopword.dic',
            'preposition.dic'
        ]
        
        # 添加任何額外的詞典文件
        extra_files = [f for f in os.listdir(input_dir) 
                      if f.endswith('.dic') and f not in dict_files]
        dict_files.extend(extra_files)
        
        logger.info(f"找到 {len(dict_files)} 個詞典文件需要轉換")
        
        for dict_file in dict_files:
            input_path = os.path.join(input_dir, dict_file)
            output_filename = f"tc_{dict_file}"
            output_path = os.path.join(output_dir, output_filename)
            
            if os.path.exists(input_path):
                self.convert_file(input_path, output_path)
            else:
                logger.warning(f"文件不存在: {dict_file}")
        
        # 生成轉換報告
        self.generate_report()
    
    def generate_report(self):
        """生成轉換報告"""
        report_file = '/logs/conversion_report.json'
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'conversion_type': 's2twp',
            'statistics': self.stats,
            'custom_map_count': len(self.custom_map)
        }
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        logger.info("=" * 50)
        logger.info("轉換統計報告:")
        logger.info(f"  - 轉換文件數: {self.stats['total_files']}")
        logger.info(f"  - 轉換詞彙數: {self.stats['converted_words']}")
        logger.info(f"  - 重複詞彙數: {self.stats['duplicate_words']}")
        logger.info(f"  - 錯誤行數: {self.stats['error_lines']}")
        logger.info(f"  - 自定義轉換: {len(self.custom_map)} 個")
        logger.info("=" * 50)

def main():
    """主函數"""
    try:
        logger.info("開始執行 IK 詞典繁體中文轉換...")
        
        # 檢查必要的目錄
        if not os.path.exists('/dictionaries/original'):
            logger.error("原始詞典目錄不存在！")
            sys.exit(1)
        
        # 創建轉換器並執行轉換
        converter = IKDictConverter(conversion_type='s2twp')
        converter.convert_all()
        
        logger.info("所有詞典轉換完成！")
        
    except Exception as e:
        logger.error(f"轉換過程中發生錯誤: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()