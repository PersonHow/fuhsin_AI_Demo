#!/usr/bin/env python3
"""
設定載入器
處理 YAML 設定檔的讀取和環境變數替換
"""

import os
import re
import yaml
from pathlib import Path
from typing import Any, Dict

class ConfigLoader:
    """設定檔載入器"""
    
    def __init__(self, config_path: str = None):
        """
        初始化設定載入器
        
        Args:
            config_path: 設定檔根目錄路徑
        """
        if config_path:
            self.config_path = Path(config_path)
        else:
            self.config_path = Path(os.getenv('CONFIG_PATH', '/app/config'))
        
        if not self.config_path.exists():
            raise FileNotFoundError(f"設定目錄不存在：{self.config_path}")
    
    def load(self, config_file: str) -> Dict[str, Any]:
        """
        載入設定檔
        
        Args:
            config_file: 設定檔名稱（相對於 config_path）
            
        Returns:
            設定字典
        """
        file_path = self.config_path / config_file
        
        if not file_path.exists():
            raise FileNotFoundError(f"設定檔不存在：{file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # 替換環境變數
        content = self._replace_env_vars(content)
        
        # 解析 YAML
        config = yaml.safe_load(content)
        
        return config
    
    def _replace_env_vars(self, content: str) -> str:
        """
        替換設定內容中的環境變數
        
        Args:
            content: 原始設定內容
            
        Returns:
            替換後的內容
        """
        # 尋找 ${VAR_NAME} 或 ${VAR_NAME:default_value} 格式
        pattern = r'\$\{([^}:]+)(?::([^}]*))?\}'
        
        def replacer(match):
            var_name = match.group(1)
            default_value = match.group(2)
            
            # 從環境變數取值
            value = os.getenv(var_name)
            
            if value is None:
                if default_value is not None:
                    value = default_value
                else:
                    # 保持原樣
                    return match.group(0)
            
            return value
        
        return re.sub(pattern, replacer, content)
    
    def load_all(self, directory: str = None) -> Dict[str, Dict]:
        """
        載入目錄下的所有設定檔
        
        Args:
            directory: 子目錄名稱（可選）
            
        Returns:
            設定字典的字典
        """
        if directory:
            target_path = self.config_path / directory
        else:
            target_path = self.config_path
        
        if not target_path.exists():
            return {}
        
        configs = {}
        
        for file_path in target_path.glob('*.yaml'):
            config_name = file_path.stem
            try:
                configs[config_name] = self.load(
                    str(file_path.relative_to(self.config_path))
                )
            except Exception as e:
                print(f"載入設定檔 {file_path} 失敗：{e}")
        
        for file_path in target_path.glob('*.yml'):
            config_name = file_path.stem
            if config_name not in configs:  # 避免重複
                try:
                    configs[config_name] = self.load(
                        str(file_path.relative_to(self.config_path))
                    )
                except Exception as e:
                    print(f"載入設定檔 {file_path} 失敗：{e}")
        
        return configs
    
    def save(self, config: Dict[str, Any], config_file: str):
        """
        儲存設定到檔案
        
        Args:
            config: 設定字典
            config_file: 設定檔名稱
        """
        file_path = self.config_path / config_file
        
        # 確保目錄存在
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
    
    def merge_configs(self, *configs: Dict) -> Dict:
        """
        合併多個設定字典
        
        Args:
            *configs: 要合併的設定字典
            
        Returns:
            合併後的設定字典
        """
        result = {}
        
        for config in configs:
            result = self._deep_merge(result, config)
        
        return result
    
    def _deep_merge(self, dict1: Dict, dict2: Dict) -> Dict:
        """
        深度合併兩個字典
        
        Args:
            dict1: 第一個字典
            dict2: 第二個字典
            
        Returns:
            合併後的字典
        """
        result = dict1.copy()
        
        for key, value in dict2.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
