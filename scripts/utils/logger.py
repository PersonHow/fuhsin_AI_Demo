#!/usr/bin/env python3
"""
日誌工具
統一的日誌設定和管理
"""

import os
import logging
import sys
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from datetime import datetime
from pathlib import Path

def setup_logger(name: str, 
                log_path: str = None,
                log_level: str = None,
                max_bytes: int = 10485760,  # 10MB
                backup_count: int = 5) -> logging.Logger:
    """
    設定日誌記錄器
    
    Args:
        name: 記錄器名稱
        log_path: 日誌檔案路徑
        log_level: 日誌級別
        max_bytes: 單一日誌檔案最大大小
        backup_count: 保留的備份檔案數量
        
    Returns:
        設定好的記錄器
    """
    # 取得或建立記錄器
    logger = logging.getLogger(name)
    
    # 避免重複添加處理器
    if logger.handlers:
        return logger
    
    # 設定日誌級別
    if log_level is None:
        log_level = os.getenv('LOG_LEVEL', 'INFO')
    
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)
    
    # 建立格式化器
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 添加控制台處理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 添加檔案處理器（如果指定了路徑）
    if log_path:
        log_path = Path(log_path)
        log_path.mkdir(parents=True, exist_ok=True)
        
        # 建立日誌檔案名稱
        log_file = log_path / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
        
        # 使用旋轉檔案處理器
        file_handler = RotatingFileHandler(
            filename=str(log_file),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # 添加錯誤日誌處理器
        error_file = log_path / f"{name}_error_{datetime.now().strftime('%Y%m%d')}.log"
        error_handler = RotatingFileHandler(
            filename=str(error_file),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)
    
    return logger

def get_logger(name: str) -> logging.Logger:
    """
    取得現有的記錄器
    
    Args:
        name: 記錄器名稱
        
    Returns:
        記錄器實例
    """
    return logging.getLogger(name)

class LoggerContext:
    """日誌上下文管理器"""
    
    def __init__(self, logger: logging.Logger, prefix: str = None):
        """
        初始化日誌上下文
        
        Args:
            logger: 記錄器
            prefix: 日誌前綴
        """
        self.logger = logger
        self.prefix = prefix
        self.original_format = None
    
    def __enter__(self):
        """進入上下文"""
        if self.prefix:
            # 暫時修改格式
            for handler in self.logger.handlers:
                self.original_format = handler.formatter._fmt
                new_format = f'%(asctime)s | %(levelname)-8s | [{self.prefix}] | %(message)s'
                handler.setFormatter(
                    logging.Formatter(
                        fmt=new_format,
                        datefmt='%Y-%m-%d %H:%M:%S'
                    )
                )
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """離開上下文"""
        if self.prefix and self.original_format:
            # 恢復原始格式
            for handler in self.logger.handlers:
                handler.setFormatter(
                    logging.Formatter(
                        fmt=self.original_format,
                        datefmt='%Y-%m-%d %H:%M:%S'
                    )
                )
