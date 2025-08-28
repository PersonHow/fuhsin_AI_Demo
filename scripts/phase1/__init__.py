#!/usr/bin/env python3
"""
Phase 1: MySQL to Elasticsearch 同步模組
"""

# 版本資訊
__version__ = '1.0.0'
__author__ = 'Cross Search System'

# 匯入主要類別
from .mysql_connector import MySQLConnector
from .es_indexer import ElasticsearchIndexer
from .data_transformer import DataTransformer
from .cross_search_indexer import CrossSearchIndexer

# 匯出的類別和函數
__all__ = [
    'MySQLConnector',
    'ElasticsearchIndexer',
    'DataTransformer',
    'CrossSearchIndexer',
    'main'
]

# 模組說明
MODULE_INFO = {
    'name': 'Phase 1 MySQL Sync',
    'description': 'MySQL 資料庫跨表同步到 Elasticsearch',
    'features': [
        'MySQL 多表同步',
        'IK 中文分詞支持',
        'OpenCC 簡繁體轉換',
        '增量/全量同步模式',
        '跨表搜索索引構建'
    ]
}

def get_module_info():
    """取得模組資訊"""
    return MODULE_INFO

# 初始化時的檢查
def check_dependencies():
    """檢查相依套件"""
    required_packages = [
        'pymysql',
        'sqlalchemy',
        'pandas',
        'requests',
        'opencc',
        'pyyaml'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        raise ImportError(
            f"缺少必要套件: {', '.join(missing_packages)}\n"
            f"請執行: pip install {' '.join(missing_packages)}"
        )

# 執行初始化檢查
try:
    check_dependencies()
except ImportError as e:
    print(f"警告: {str(e)}")
