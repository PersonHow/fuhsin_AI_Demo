#!/usr/bin/env python3
"""
Elasticsearch 索引管理器
處理所有 ES 相關操作
"""

import json
import time
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime

class ElasticsearchIndexer:
    """Elasticsearch 索引管理器"""
    
    def __init__(self, config: Dict[str, Any], logger):
        """
        初始化 ES 索引器
        
        Args:
            config: ES 設定
            logger: 日誌記錄器
        """
        self.config = config
        self.logger = logger
        self.base_url = f"http://{config['host']}:{config['port']}"
        
        # 建立 session
        self.session = requests.Session()
        self.session.auth = (config['user'], config['password'])
        self.session.headers.update({
            'Content-Type': 'application/json'
        })
        
        self._wait_for_elasticsearch()
        self._check_connection()
    
    def _wait_for_elasticsearch(self, max_retries: int = 30):
        """等待 Elasticsearch 啟動"""
        for i in range(max_retries):
            try:
                response = self.session.get(
                    f"{self.base_url}/_cluster/health",
                    timeout=5
                )
                if response.status_code == 200:
                    self.logger.info("✓ Elasticsearch 已就緒")
                    return
            except:
                pass
            
            self.logger.info(f"等待 Elasticsearch 啟動... ({i+1}/{max_retries})")
            time.sleep(2)
        
        raise Exception("Elasticsearch 啟動超時")
    
    def _check_connection(self):
        """檢查 ES 連接"""
        try:
            response = self.session.get(f"{self.base_url}/_cluster/health")
            response.raise_for_status()
            
            health = response.json()
            self.logger.info(f"✓ Elasticsearch 連接成功")
            self.logger.info(f"  叢集名稱：{health['cluster_name']}")
            self.logger.info(f"  叢集狀態：{health['status']}")
            self.logger.info(f"  節點數量：{health['number_of_nodes']}")
            
        except Exception as e:
            self.logger.error(f"Elasticsearch 連接失敗：{str(e)}")
            raise
    
    def create_index_template(self, template_name: str, template_config: Dict):
        """建立索引模板"""
        template = {
            "index_patterns": [f"{template_name}-*"],
            "template": {
                "settings": {
                    "number_of_shards": template_config['settings'].get('number_of_shards', 1),
                    "number_of_replicas": template_config['settings'].get('number_of_replicas', 0),
                    "refresh_interval": template_config['settings'].get('refresh_interval', '1s'),
                    "analysis": {
                        "analyzer": {
                            "ik_analyzer": {
                                "type": "ik_max_word"
                            },
                            "ik_search": {
                                "type": "ik_smart"
                            }
                        }
                    }
                }
            }
        }
        
        try:
            response = self.session.put(
                f"{self.base_url}/_index_template/{template_name}-template",
                data=json.dumps(template),
                timeout=30
            )
            response.raise_for_status()
            self.logger.info(f"✓ 索引模板已建立：{template_name}-template")
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                self.logger.debug(f"索引模板已存在：{template_name}-template")
            else:
                self.logger.error(f"建立索引模板失敗：{str(e)}")
                raise
    
    def create_index_if_not_exists(self, index_name: str, 
                                  table_config: Dict,
                                  template_config: Dict):
        """建立索引（如果不存在）"""
        # 檢查索引是否存在
        if self.index_exists(index_name):
            self.logger.debug(f"索引已存在：{index_name}")
            return
        
        # 建立索引映射
        mappings = self._build_mappings(table_config, template_config)
        
        index_body = {
            "settings": {
                "number_of_shards": template_config['settings'].get('number_of_shards', 1),
                "number_of_replicas": template_config['settings'].get('number_of_replicas', 0),
                "refresh_interval": template_config['settings'].get('refresh_interval', '1s'),
                "analysis": {
                    "analyzer": {
                        "ik_analyzer": {
                            "type": "ik_max_word"
                        },
                        "ik_search": {
                            "type": "ik_smart"
                        }
                    }
                }
            },
            "mappings": mappings
        }
        
        try:
            response = self.session.put(
                f"{self.base_url}/{index_name}",
                data=json.dumps(index_body),
                timeout=30
            )
            response.raise_for_status()
            self.logger.info(f"✓ 索引已建立：{index_name}")
            
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"建立索引失敗：{str(e)}")
            self.logger.error(f"響應內容：{e.response.text}")
            raise
    
    def _build_mappings(self, table_config: Dict, template_config: Dict) -> Dict:
        """建立索引映射"""
        mappings = {
            "dynamic": "true",
            "properties": {
                "_metadata": {
                    "properties": {
                        "source_table": {"type": "keyword"},
                        "indexed_at": {"type": "date"},
                        "sync_version": {"type": "integer"}
                    }
                },
                "_search_content": {
                    "type": "text",
                    "analyzer": "ik_analyzer",
                    "search_analyzer": "ik_search"
                }
            }
        }
        
        # 添加搜尋欄位的特殊映射
        if 'search_fields' in table_config:
            for field, weight in table_config['search_fields'].items():
                mappings['properties'][field] = {
                    "type": "text",
                    "analyzer": "ik_analyzer",
                    "search_analyzer": "ik_search",
                    "boost": weight,
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                }
        
        return mappings
    
    def index_exists(self, index_name: str) -> bool:
        """檢查索引是否存在"""
        try:
            response = self.session.head(f"{self.base_url}/{index_name}")
            return response.status_code == 200
        except:
            return False
    
    def delete_index(self, index_name: str) -> bool:
        """刪除索引"""
        try:
            response = self.session.delete(f"{self.base_url}/{index_name}")
            response.raise_for_status()
            self.logger.info(f"✓ 索引已刪除：{index_name}")
            return True
        except Exception as e:
            self.logger.error(f"刪除索引失敗：{str(e)}")
            return False
    
    def bulk_index(self, index_name: str, documents: List[Dict]) -> int:
        """批量索引文檔"""
        if not documents:
            return 0
        
        # 建立批量請求
        bulk_data = []
        for doc in documents:
            # 索引動作
            action = {
                "index": {
                    "_index": index_name,
                    "_id": doc['_id']
                }
            }
            bulk_data.append(json.dumps(action, ensure_ascii=False))
            bulk_data.append(json.dumps(doc['_source'], ensure_ascii=False))
        
        # 發送批量請求
        bulk_body = "\n".join(bulk_data) + "\n"
        
        try:
            response = self.session.post(
                f"{self.base_url}/_bulk",
                data=bulk_body.encode('utf-8'),
                headers={'Content-Type': 'application/x-ndjson'},
                timeout=60
            )
            response.raise_for_status()
            
            result = response.json()
            
            # 統計結果
            success_count = 0
            error_items = []
            
            for item in result.get('items', []):
                if item['index'].get('status') in [200, 201]:
                    success_count += 1
                else:
                    error_items.append(item['index'])
            
            if error_items:
                self.logger.warning(f"部分文檔索引失敗：{len(error_items)} 個")
                self.logger.debug(f"錯誤詳情：{error_items[:5]}")  # 只顯示前5個錯誤
            
            return success_count
            
        except Exception as e:
            self.logger.error(f"批量索引失敗：{str(e)}")
            raise
    
    def search(self, index_name: str, query: Dict, size: int = 10) -> Dict:
        """執行搜尋"""
        search_body = {
            "size": size,
            "query": query
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/{index_name}/_search",
                data=json.dumps(search_body),
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            self.logger.debug(f"搜尋完成，找到 {result['hits']['total']['value']} 個結果")
            return result
            
        except Exception as e:
            self.logger.error(f"搜尋失敗：{str(e)}")
            raise
    
    def count(self, index_name: str) -> int:
        """計算索引中的文檔數量"""
        try:
            response = self.session.get(f"{self.base_url}/{index_name}/_count")
            response.raise_for_status()
            
            result = response.json()
            return result['count']
            
        except Exception as e:
            self.logger.error(f"計數失敗：{str(e)}")
            return 0
    
    def get_index_stats(self, index_name: str) -> Dict:
        """取得索引統計資訊"""
        try:
            response = self.session.get(f"{self.base_url}/{index_name}/_stats")
            response.raise_for_status()
            
            stats = response.json()
            index_stats = stats['indices'][index_name]
            
            return {
                'document_count': index_stats['primaries']['docs']['count'],
                'deleted_count': index_stats['primaries']['docs']['deleted'],
                'size_in_bytes': index_stats['primaries']['store']['size_in_bytes'],
                'size_in_mb': round(index_stats['primaries']['store']['size_in_bytes'] / 1024 / 1024, 2)
            }
            
        except Exception as e:
            self.logger.error(f"取得索引統計失敗：{str(e)}")
            return {}
    
    def update_document(self, index_name: str, doc_id: str, doc: Dict):
        """更新單一文檔"""
        try:
            response = self.session.post(
                f"{self.base_url}/{index_name}/_update/{doc_id}",
                data=json.dumps({"doc": doc}),
                timeout=30
            )
            response.raise_for_status()
            self.logger.debug(f"文檔已更新：{index_name}/{doc_id}")
            
        except Exception as e:
            self.logger.error(f"更新文檔失敗：{str(e)}")
            raise
    
    def delete_document(self, index_name: str, doc_id: str):
        """刪除單一文檔"""
        try:
            response = self.session.delete(
                f"{self.base_url}/{index_name}/_doc/{doc_id}",
                timeout=30
            )
            response.raise_for_status()
            self.logger.debug(f"文檔已刪除：{index_name}/{doc_id}")
            
        except Exception as e:
            self.logger.error(f"刪除文檔失敗：{str(e)}")
            raise
    
    def refresh_index(self, index_name: str):
        """刷新索引"""
        try:
            response = self.session.post(
                f"{self.base_url}/{index_name}/_refresh",
                timeout=30
            )
            response.raise_for_status()
            self.logger.debug(f"索引已刷新：{index_name}")
            
        except Exception as e:
            self.logger.error(f"刷新索引失敗：{str(e)}")
            raise
