#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
統一資料轉換器主程式
整合 FileSQLAdapter, JDBCAdapter 和索引管理
"""

import os
import json
import time
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, List

# 從之前的模組導入
from data_transformer_core import DataPipeline, ElasticsearchWriter
from file_sql_adapter import FileSQLAdapter
from jdbc_adapter import JDBCAdapter

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_file: str = "transformer_config.json"):
        self.config_file = config_file
        self.config = self.load_config()
    
    def load_config(self) -> Dict[str, Any]:
        """載入配置檔案"""
        default_config = {
            "elasticsearch": {
                "url": "http://localhost:9200",
                "username": "elastic", 
                "password": "admin@12345"
            },
            "adapters": {
                "file_sql": {
                    "enabled": True,
                    "import_dir": "./data/import",
                    "processed_suffix": ".done"
                },
                "jdbc": {
                    "enabled": False,
                    "database": {
                        "type": "mysql",
                        "host": "localhost",
                        "port": 3306,
                        "user": "root",
                        "password": "",
                        "database": "erp_system"
                    },
                    "tables": [
                        {
                            "table_name": "work_orders",
                            "sync_mode": "incremental",
                            "timestamp_column": "updated_time",
                            "pk_columns": ["work_order_id"]
                        },
                        {
                            "table_name": "employees",
                            "sync_mode": "incremental", 
                            "timestamp_column": "updated_time",
                            "pk_columns": ["employee_id"]
                        }
                    ]
                }
            },
            "indices": {
                "work_orders": {
                    "alias": "erp_orders_search",
                    "version": 1,
                    "base_name": "erp_work_orders"
                },
                "employees": {
                    "alias": "erp_employees_search", 
                    "version": 1,
                    "base_name": "erp_employees"
                },
                "default": {
                    "alias": "erp_data_search",
                    "version": 1,
                    "base_name": "erp_mixed_data"
                }
            },
            "sync_interval": 300,  # 5分鐘
            "batch_size": 500
        }
        
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    user_config = json.load(f)
                    # 深度合併配置
                    return self.deep_merge(default_config, user_config)
            except Exception as e:
                logger.warning(f"載入配置檔失敗，使用預設配置: {e}")
        
        # 儲存預設配置
        self.save_config(default_config)
        return default_config
    
    def save_config(self, config: Dict[str, Any]):
        """儲存配置檔案"""
        with open(self.config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
    
    def deep_merge(self, base: Dict, update: Dict) -> Dict:
        """深度合併字典"""
        result = base.copy()
        for key, value in update.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self.deep_merge(result[key], value)
            else:
                result[key] = value
        return result

class IndexManager:
    """索引管理器 - 處理別名和版本升級"""
    
    def __init__(self, es_writer: ElasticsearchWriter, config: Dict[str, Any]):
        self.es_writer = es_writer
        self.indices_config = config.get("indices", {})
    
    def get_target_index(self, table_name: str) -> str:
        """根據表名取得目標索引"""
        # 尋找對應的索引配置
        for key, index_config in self.indices_config.items():
            if key == table_name or key in table_name:
                base_name = index_config["base_name"]
                version = index_config["version"]
                return f"{base_name}_v{version}"
        
        # 使用預設索引
        default_config = self.indices_config.get("default", {})
        base_name = default_config.get("base_name", "erp_mixed_data")
        version = default_config.get("version", 1)
        return f"{base_name}_v{version}"
    
    def ensure_indices_and_aliases(self):
        """確保所有索引和別名存在"""
        for key, index_config in self.indices_config.items():
            if key == "default":
                continue
                
            base_name = index_config["base_name"]
            version = index_config["version"]
            alias = index_config["alias"]
            
            try:
                index_name = self.es_writer.create_index_with_alias(base_name, version)
                logger.info(f"索引 {index_name} 和別名 {alias} 已準備就緒")
            except Exception as e:
                logger.error(f"建立索引 {base_name} 失敗: {e}")
    
    def upgrade_index_version(self, base_name: str, new_version: int):
        """升級索引版本 - 零停機切換"""
        try:
            # 建立新版本索引
            new_index = f"{base_name}_v{new_version}"
            old_version = new_version - 1
            old_index = f"{base_name}_v{old_version}"
            
            # 找到對應的別名
            alias = None
            for config in self.indices_config.values():
                if config.get("base_name") == base_name:
                    alias = config["alias"]
                    break
            
            if not alias:
                raise ValueError(f"找不到 {base_name} 的別名配置")
            
            # 建立新索引
            self.es_writer.create_index_with_alias(base_name, new_version)
            
            # TODO: 這裡可以加入資料重新索引的邏輯
            # reindex_data(old_index, new_index)
            
            # 切換別名
            self.es_writer.switch_alias(old_index, new_index, alias)
            
            logger.info(f"索引版本升級完成: {old_index} -> {new_index}")
            
        except Exception as e:
            logger.error(f"索引版本升級失敗: {e}")
            raise

class TransformerController:
    """轉換器主控制器"""
    
    def __init__(self, config_file: str = "transformer_config.json"):
        self.config_manager = ConfigManager(config_file)
        self.config = self.config_manager.config
        
        # 初始化核心組件
        es_config = self.config["elasticsearch"]
        self.pipeline = DataPipeline(
            es_config["url"],
            es_config["username"], 
            es_config["password"]
        )
        
        self.index_manager = IndexManager(self.pipeline.es_writer, self.config)
        
        # 初始化適配器
        self.adapters = self._initialize_adapters()
    
    def _initialize_adapters(self) -> List:
        """初始化所有啟用的適配器"""
        adapters = []
        adapter_configs = self.config.get("adapters", {})
        
        # FileSQLAdapter
        if adapter_configs.get("file_sql", {}).get("enabled", False):
            file_config = adapter_configs["file_sql"]
            adapter = FileSQLAdapter(
                import_dir=file_config["import_dir"],
                processed_suffix=file_config["processed_suffix"]
            )
            adapters.append(("file_sql", adapter))
            logger.info("FileSQLAdapter 已初始化")
        
        # JDBCAdapter
        if adapter_configs.get("jdbc", {}).get("enabled", False):
            jdbc_config = adapter_configs["jdbc"]
            adapter = JDBCAdapter(
                db_config=jdbc_config["database"],
                table_configs=jdbc_config["tables"]
            )
            adapters.append(("jdbc", adapter))
            logger.info("JDBCAdapter 已初始化")
        
        return adapters
    
    def run_once(self):
        """執行一次完整的資料同步"""
        logger.info("開始資料同步週期")
        
        # 確保索引和別名存在
        self.index_manager.ensure_indices_and_aliases()
        
        total_processed = 0
        
        for adapter_name, adapter in self.adapters:
            try:
                logger.info(f"處理適配器: {adapter_name}")
                
                # 根據適配器類型選擇目標索引
                if adapter_name == "file_sql":
                    target_index = self.index_manager.get_target_index("mixed_data")
                elif adapter_name == "jdbc":
                    # JDBC 可以根據表名分發到不同索引
                    target_index = self.index_manager.get_target_index("mixed_data")
                else:
                    target_index = self.index_manager.get_target_index("default")
                
                # 執行資料轉換和索引
                count = self.pipeline.run(adapter, target_index)
                total_processed += count
                
                logger.info(f"{adapter_name} 處理完成: {count} 筆記錄")
                
            except Exception as e:
                logger.error(f"適配器 {adapter_name} 處理失敗: {e}")
        
        logger.info(f"本次同步完成，總共處理 {total_processed} 筆記錄")
        return total_processed
    
    def run_daemon(self):
        """背景服務模式運行"""
        sync_interval = self.config.get("sync_interval", 300)
        logger.info(f"資料轉換器背景服務啟動，同步間隔: {sync_interval} 秒")
        
        while True:
            try:
                self.run_once()
                logger.info(f"等待 {sync_interval} 秒後進行下一次同步...")
                time.sleep(sync_interval)
            except KeyboardInterrupt:
                logger.info("收到停止信號，正在關閉...")
                break
            except Exception as e:
                logger.error(f"同步過程發生錯誤: {e}")
                logger.info("5分鐘後重試...")
                time.sleep(300)

def main():
    """主程式入口"""
    parser = argparse.ArgumentParser(description="ERP 資料轉換器")
    parser.add_argument("--config", default="transformer_config.json", help="配置檔路徑")
    parser.add_argument("--mode", choices=["once", "daemon"], default="once", help="執行模式")
    parser.add_argument("--create-config", action="store_true", help="建立配置檔範例")
    parser.add_argument("--upgrade-index", help="升級指定索引版本 (格式: base_name:version)")
    
    args = parser.parse_args()
    
    if args.create_config:
        # 建立配置檔案範例
        config_manager = ConfigManager(args.config)
        print(f"配置檔案已建立: {args.config}")
        print("請編輯配置檔案後再次執行")
        return
    
    # 初始化控制器
    controller = TransformerController(args.config)
    
    if args.upgrade_index:
        # 升級索引版本
        try:
            base_name, version = args.upgrade_index.split(":")
            controller.index_manager.upgrade_index_version(base_name, int(version))
        except ValueError:
            print("錯誤: 索引升級格式應為 'base_name:version'")
            return
    elif args.mode == "once":
        # 單次執行
        controller.run_once()
    elif args.mode == "daemon":
        # 背景服務模式
        controller.run_daemon()

if __name__ == "__main__":
    main()
