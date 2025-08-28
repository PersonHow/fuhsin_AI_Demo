#!/usr/bin/env python3
"""
跨表搜索索引構建器
將多個表的數據整合到統一的搜索索引中
"""

import hashlib
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd
from opencc import OpenCC

class CrossSearchIndexer:
    """跨表搜索索引構建器"""
    
    def __init__(self, es_indexer, transformer, logger):
        """初始化"""
        self.es = es_indexer
        self.transformer = transformer
        self.logger = logger
        
        # 簡繁體轉換器
        self.cc_s2t = OpenCC('s2t')
        self.cc_t2s = OpenCC('t2s')
        
        # 文檔類型映射
        self.doc_type_mapping = {
            'customers': 'customer',
            'products': 'product',
            'orders': 'order',
            'order_details': 'order_detail'
        }
    
    def build_cross_search_document(self, 
                                  source_table: str,
                                  source_data: Dict,
                                  table_config: Dict) -> Dict:
        """構建跨表搜索文檔"""
        
        doc_type = self.doc_type_mapping.get(source_table, 'other')
        
        # 基礎文檔結構
        doc = {
            'doc_type': doc_type,
            'unified_id': f"{doc_type}_{source_data.get(table_config['primary_key'])}",
            'tags': [],
            'related_ids': [],
            'numeric_values': {},
            'date_values': {},
            'boost_score': 1.0,
            '_metadata': {
                'source_index': table_config['index_name'],
                'source_id': str(source_data.get(table_config['primary_key'])),
                'last_updated': datetime.now().isoformat()
            }
        }
        
        # 根據不同類型填充內容
        if doc_type == 'customer':
            doc['title'] = source_data.get('customer_name', '')
            doc['content'] = ' '.join([
                str(source_data.get('customer_name', '')),
                str(source_data.get('company_name', '')),
                str(source_data.get('address', '')),
                str(source_data.get('city', ''))
            ])
            doc['category'] = 'customer'
            doc['tags'] = [source_data.get('status', ''), source_data.get('city', '')]
            doc['numeric_values']['credit_limit'] = float(source_data.get('credit_limit', 0))
            
        elif doc_type == 'product':
            doc['title'] = source_data.get('product_name', '')
            doc['content'] = ' '.join([
                str(source_data.get('product_name', '')),
                str(source_data.get('description', '')),
                str(source_data.get('product_code', ''))
            ])
            doc['category'] = 'product'
            doc['tags'] = [f"category_{source_data.get('category_id', '')}"]
            doc['numeric_values']['price'] = float(source_data.get('unit_price', 0))
            doc['numeric_values']['quantity'] = int(source_data.get('stock_quantity', 0))
            doc['boost_score'] = 1.5  # 產品搜索權重較高
            
        elif doc_type == 'order':
            doc['title'] = f"訂單 {source_data.get('order_number', '')}"
            doc['content'] = ' '.join([
                str(source_data.get('order_number', '')),
                str(source_data.get('ship_name', '')),
                str(source_data.get('ship_address', ''))
            ])
            doc['category'] = 'order'
            doc['tags'] = [source_data.get('order_status', '')]
            doc['numeric_values']['amount'] = float(source_data.get('total_amount', 0))
            doc['date_values']['order_date'] = source_data.get('order_date')
            
            # 添加關聯
            if source_data.get('customer_id'):
                doc['related_ids'].append({
                    'type': 'customer',
                    'id': str(source_data['customer_id'])
                })
        
        # 生成簡繁體版本
        content = doc['content']
        doc['content_simplified'] = self.cc_t2s.convert(content)
        doc['content_traditional'] = self.cc_s2t.convert(content)
        
        # 移除空標籤
        doc['tags'] = [tag for tag in doc['tags'] if tag]
        
        return doc
    
    def sync_to_cross_search(self, 
                           table_name: str,
                           documents: List[Dict],
                           table_config: Dict) -> int:
        """同步數據到跨表搜索索引"""
        
        cross_search_docs = []
        
        for doc in documents:
            try:
                cross_doc = self.build_cross_search_document(
                    table_name,
                    doc['_source'],
                    table_config
                )
                
                cross_search_docs.append({
                    '_id': cross_doc['unified_id'],
                    '_source': cross_doc
                })
                
            except Exception as e:
                self.logger.error(f"構建跨表搜索文檔失敗: {str(e)}")
                continue
        
        if cross_search_docs:
            success_count = self.es.bulk_index('erp-cross-search', cross_search_docs)
            self.logger.info(f"同步 {success_count} 筆數據到跨表搜索索引")
            return success_count
        
        return 0
