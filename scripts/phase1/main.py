#!/usr/bin/env python3
"""
階段一：MySQL 跨表同步主程式
功能：從單一 MySQL 資料庫同步多個表到 Elasticsearch
"""

import os
import sys
import time
import signal
import logging
import argparse  # 添加這行
from datetime import datetime
from pathlib import Path

# 添加專案路徑
sys.path.append(str(Path(__file__).parent.parent.parent))

from scripts.phase1.mysql_connector import MySQLConnector
from scripts.phase1.es_indexer import ElasticsearchIndexer
from scripts.phase1.data_transformer import DataTransformer
from scripts.phase1.cross_search_indexer import CrossSearchIndexer  # 移到這裡
from scripts.utils.logger import setup_logger
from scripts.utils.config_loader import ConfigLoader

# 全域變數
running = True
logger = None

def signal_handler(signum, frame):
    """處理中斷信號"""
    global running
    logger.info(f"收到中斷信號 {signum}，準備停止...")
    running = False

class CrossTableSync:
    """跨表同步主類"""
    
    def __init__(self):
        """初始化同步器"""
        # 載入設定
        self.config = ConfigLoader()
        self.db_config = self.config.load('sync/database.yaml')
        self.tables_config = self.config.load('sync/tables.yaml')
        
        # 設定日誌
        global logger
        logger = setup_logger('phase1_sync', 
                            log_path=os.getenv('LOG_PATH', '/app/logs/sync'))
        
        # 初始化元件
        self.mysql = MySQLConnector(self.db_config['mysql'], logger)
        self.es = ElasticsearchIndexer(self.db_config['elasticsearch'], logger)
        self.transformer = DataTransformer(logger)
        
        # 同步設定
        self.sync_config = self.db_config['sync']
        self.sync_state = self.load_sync_state()
        
        logger.info("=" * 60)
        logger.info("階段一：MySQL 跨表同步系統啟動")
        logger.info(f"同步模式：{self.sync_config['mode']}")
        logger.info(f"同步間隔：{self.sync_config['interval']} 秒")
        logger.info(f"批次大小：{self.sync_config['batch_size']}")
        logger.info("=" * 60)
        
        # 初始化跨表搜索索引器
        self.cross_search_indexer = CrossSearchIndexer(
            self.es, 
            self.transformer, 
            logger
        )
        
        # 確保跨表搜索索引存在
        cross_search_template = self.config.load('sync/cross_search_template.yaml')
        cross_search_index = cross_search_template['cross_search']['index_name']
        
        if not self.es.index_exists(cross_search_index):
            logger.info("跨表搜索索引不存在，正在建立...")
            self.init_cross_search_index()
    
    def init_cross_search_index(self):
        """初始化跨表搜索索引"""
        
        # 載入跨表搜索模板設定
        cross_search_template = self.config.load('sync/cross_search_template.yaml')
        index_config = cross_search_template['cross_search']
        index_name = index_config['index_name']
        
        logger.info("="*60)
        logger.info("初始化跨表搜索索引")
        logger.info("="*60)
        
        # 檢查索引是否存在
        if self.es.index_exists(index_name):
            logger.warning(f"索引 {index_name} 已存在")
            return False
        
        # 建立索引
        index_body = {
            "settings": index_config['settings'],
            "mappings": index_config['mappings']
        }
        
        try:
            import json
            response = self.es.session.put(
                f"{self.es.base_url}/{index_name}",
                data=json.dumps(index_body),
                timeout=30
            )
            response.raise_for_status()
            logger.info(f"✓ 跨表搜索索引已建立：{index_name}")
            return True
            
        except Exception as e:
            logger.error(f"建立跨表搜索索引失敗：{str(e)}")
            return False
    
    def load_sync_state(self):
        """載入同步狀態"""
        state_file = self.sync_config['state_file']
        state_path = Path(state_file)
        
        if state_path.exists():
            import json
            with open(state_path, 'r') as f:
                return json.load(f)
        return {}
    
    def save_sync_state(self):
        """儲存同步狀態"""
        state_file = self.sync_config['state_file']
        state_path = Path(state_file)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        
        import json
        with open(state_path, 'w') as f:
            json.dump(self.sync_state, f, indent=2, default=str)
    
    def sync_table(self, table_config):
        """同步單一表"""
        
        table_name = table_config['name']
        index_name = table_config['index_name']
        
        logger.info(f"開始同步表：{table_name} -> {index_name}")
        
        try:
            # 1. 建立或更新索引映射
            self.es.create_index_if_not_exists(
                index_name,
                table_config,
                self.tables_config['index_template']
            )
            
            # 2. 決定同步策略
            last_sync_time = self.sync_state.get(table_name, {}).get('last_sync')
            
            if self.sync_config['mode'] == 'incremental' and last_sync_time:
                # 增量同步
                query = self.build_incremental_query(table_config, last_sync_time)
                logger.info(f"執行增量同步，從 {last_sync_time} 開始")
            else:
                # 全量同步
                query = f"SELECT * FROM {table_name}"
                logger.info("執行全量同步")
            
            # 3. 查詢資料
            df = self.mysql.query_to_dataframe(query)
            
            if df.empty:
                logger.info(f"表 {table_name} 無新資料需要同步")
                return 0
            
            logger.info(f"查詢到 {len(df)} 筆資料")
            
            # 4. 處理關聯資料
            if 'relations' in table_config:
                df = self.process_relations(df, table_config['relations'])
            
            # 5. 資料轉換
            if 'transformations' in table_config:
                df = self.transformer.apply_transformations(
                    df, 
                    table_config['transformations']
                )
            
            # 6. 準備 ES 文檔
            documents = self.prepare_documents(df, table_config)
            
            # 7. 批次索引到 ES
            batch_size = self.sync_config['batch_size']
            total_indexed = 0
            
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                success_count = self.es.bulk_index(index_name, batch)
                total_indexed += success_count
                
                logger.info(f"已索引 {total_indexed}/{len(documents)} 筆資料")
            
            # 8. 更新同步狀態
            self.sync_state[table_name] = {
                'last_sync': datetime.now().isoformat(),
                'record_count': total_indexed
            }
            self.save_sync_state()
            
            logger.info(f"✓ 表 {table_name} 同步完成，共 {total_indexed} 筆")

            # 9. 同步到跨表搜索索引
            if hasattr(self, 'cross_search_indexer'):
                try:
                    cross_count = self.cross_search_indexer.sync_to_cross_search(
                        table_name,
                        documents,
                        table_config
                    )
                    logger.info(f"✓ 同步 {cross_count} 筆到跨表搜索索引")
                except Exception as e:
                    logger.error(f"同步到跨表搜索索引失敗：{str(e)}")
                    
            return total_indexed
            
        except Exception as e:
            logger.error(f"同步表 {table_name} 失敗：{str(e)}")
            raise
    
    def build_incremental_query(self, table_config, last_sync_time):
        """
        建立增量查詢語句
        依表設定與最後同步時間，產生增量 SQL。
            優先序：
            1) database.yaml -> sync.incremental_fields.<table>
            2) tables.yaml   -> tables[n].incremental_fields
            3) database.yaml -> sync.incremental.timestamp_fields（舊版相容）
            4) 預設欄位：updated_time, created_time, order_date, updated_at, created_at
        """
        table_name = table_config['name']

        # 1) 每表的增量欄位（database.yaml -> sync.incremental_fields.<table>）
        per_table = (self.sync_config.get('incremental_fields') or {}).get(table_name, [])

        # 2) tables.yaml 也可能定義 incremental_fields
        from_table_cfg = table_config.get('incremental_fields', [])

        # 3) 舊寫法：全域 timestamp_fields（database.yaml -> sync.incremental.timestamp_fields）
        global_list = (self.sync_config.get('incremental') or {}).get('timestamp_fields', [])

        # 4) 預設候選（依你實際 schema 調整）
        defaults = ['updated_time', 'created_time', 'order_date', 'updated_at', 'created_at']

        # 按優先序合併去重（保留順序）
        candidates = []
        for src in (per_table, from_table_cfg, global_list, defaults):
            for f in src:
                if f and f not in candidates:
                    candidates.append(f)

        # 找出表中存在的時間戳欄位
        columns = self.mysql.get_table_columns(table_name)

        for field in candidates:
            if field in columns:
                return f"""
                    SELECT * FROM {table_name}
                    WHERE {field} > '{last_sync_time}'
                    ORDER BY {field}
                """

        # 如果沒有時間戳欄位，退回全量同步
        logger.warning(f"表 {table_name} 沒有時間戳欄位，使用全量同步")
        return f"SELECT * FROM {table_name}"
        
    def process_relations(self, df, relations):
        """處理關聯查詢"""
        import pandas as pd
        
        for relation in relations:
            if relation['type'] == 'lookup':
                # 查詢關聯表
                related_table = relation['table']
                local_field = relation['local_field']
                foreign_field = relation['foreign_field']
                fields_to_include = relation['fields_to_include']
                
                # 取得唯一值以優化查詢
                unique_values = df[local_field].unique()
                values_str = ','.join([f"'{v}'" for v in unique_values if pd.notna(v)])
                
                if values_str:
                    # 查詢關聯資料
                    fields_str = ','.join([foreign_field] + fields_to_include)
                    query = f"""
                        SELECT {fields_str}
                        FROM {related_table}
                        WHERE {foreign_field} IN ({values_str})
                    """
                    
                    related_df = self.mysql.query_to_dataframe(query)
                    
                    # 合併資料
                    if not related_df.empty:
                        # 為關聯欄位加上前綴避免命名衝突
                        rename_dict = {
                            col: f"{related_table}_{col}"
                            for col in fields_to_include
                        }
                        related_df = related_df.rename(columns=rename_dict)
                        
                        # 合併
                        df = pd.merge(
                            df, 
                            related_df,
                            left_on=local_field,
                            right_on=foreign_field,
                            how='left'
                        )
                        
                        logger.info(f"已加入關聯資料：{related_table} ({len(fields_to_include)} 個欄位)")
        
        return df
    
    def prepare_documents(self, df, table_config):
        """準備 Elasticsearch 文檔"""
        import pandas as pd  # 添加這行
        import hashlib
        import json
        from datetime import datetime
        from opencc import OpenCC
        
        documents = []
        table_name = table_config['name']
        primary_key = table_config.get('primary_key', 'id')
        search_fields = table_config.get('search_fields', {})
        
        # 初始化轉換器
        cc_s2t = OpenCC('s2t')
        cc_t2s = OpenCC('t2s')
        
        for idx, row in df.iterrows():
            # 轉換為字典
            doc = row.to_dict()
            
            # 清理 NaN 值
            doc = {k: v for k, v in doc.items() if pd.notna(v)}
            
            # 添加元數據
            doc['_metadata'] = {
                'source_table': table_name,
                'indexed_at': datetime.now().isoformat(),
                'sync_version': self.sync_state.get('version', 1)
            }
            
            # 建立搜尋內容（包含繁簡轉換）
            searchable_content = []
            for field, weight in search_fields.items():
                if field in doc and doc[field]:
                    content = str(doc[field])
                    searchable_content.append(content)
                    searchable_content.append(cc_s2t.convert(content))
                    searchable_content.append(cc_t2s.convert(content))
            
            doc['_search_content'] = ' '.join(searchable_content)
            
            # 生成文檔 ID
            if isinstance(primary_key, list):
                # 複合主鍵
                id_parts = [str(doc.get(k, '')) for k in primary_key]
                doc_id = hashlib.sha256('_'.join(id_parts).encode()).hexdigest()
            else:
                # 單一主鍵
                doc_id = str(doc.get(primary_key, idx))
            
            documents.append({
                '_id': doc_id,
                '_source': doc
            })
        
        return documents
    
    def run(self):
        """執行同步循環"""
        global running
        
        # 註冊信號處理
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        while running:
            try:
                sync_start = time.time()
                total_synced = 0
                
                # 同步所有設定的表
                for table_config in self.tables_config['tables']:
                    if not running:
                        break
                    
                    count = self.sync_table(table_config)
                    total_synced += count
                
                sync_duration = time.time() - sync_start
                logger.info(f"本次同步完成：{total_synced} 筆資料，耗時 {sync_duration:.2f} 秒")
                
                # 等待下一次同步
                if running:
                    logger.info(f"等待 {self.sync_config['interval']} 秒後進行下一次同步...")
                    time.sleep(self.sync_config['interval'])
                
            except Exception as e:
                logger.error(f"同步循環發生錯誤：{str(e)}", exc_info=True)
                if running:
                    logger.info("30 秒後重試...")
                    time.sleep(30)
        
        logger.info("同步服務已停止")

def main():
    """主程式入口"""
    parser = argparse.ArgumentParser(description='MySQL 跨表同步系統')
    parser.add_argument(
        '--mode', 
        choices=['sync', 'init', 'test'],
        default='sync',
        help='執行模式: sync(同步), init(初始化), test(測試)'
    )
    
    args = parser.parse_args()
    
    try:
        syncer = CrossTableSync()
        
        if args.mode == 'init':
            # 只執行初始化
            logger.info("執行索引初始化模式")
            if syncer.init_cross_search_index():
                logger.info("✓ 初始化完成")
            else:
                logger.info("初始化失敗或索引已存在")
                
        elif args.mode == 'test':
            # 測試模式 - 同步一次就結束
            logger.info("執行測試模式 - 單次同步")
            for table_config in syncer.tables_config['tables']:
                syncer.sync_table(table_config)
            logger.info("✓ 測試完成")
            
        else:
            # 正常同步模式
            syncer.run()
            
    except KeyboardInterrupt:
        logger.info("收到鍵盤中斷，正在停止...")
    except Exception as e:
        logger.error(f"程式發生嚴重錯誤：{str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
