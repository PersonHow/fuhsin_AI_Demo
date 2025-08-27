#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQL 智能解析器 - 將 SQL INSERT 語句解析成結構化數據
替換原有的 auto_importer.py
"""
import os, re, time, json, hashlib
from datetime import datetime, timezone
from pathlib import Path
import requests
from opencc import OpenCC

ES_URL   = os.environ.get("ES_URL",  "http://es01:9200")
ES_USER  = os.environ.get("ES_USER", "elastic")
ES_PASS  = os.environ.get("ES_PASS", "admin@12345")
IMPORT_DIR = Path(os.environ.get("IMPORT_DIR", "/data/import"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))
SLEEP_SEC  = int(os.environ.get("SLEEP", "5"))

session = requests.Session()
session.auth = (ES_USER, ES_PASS)
session.headers.update({"Content-Type": "application/json"})

cc_s2t = OpenCC("s2t")
cc_t2s = OpenCC("t2s")

def log(msg): 
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)

class SQLParser:
    """SQL INSERT 語句解析器"""
    
    @staticmethod
    def parse_insert_statement(sql_text):
        """解析 INSERT INTO 語句，提取表名、欄位名和數據"""
        # 清理 SQL
        clean_sql = re.sub(r'--[^\n]*', '', sql_text)  # 移除行註釋
        clean_sql = re.sub(r'/\*.*?\*/', '', clean_sql, flags=re.DOTALL)  # 移除塊註釋
        clean_sql = re.sub(r'\s+', ' ', clean_sql).strip()  # 規範化空白
        
        # 解析 INSERT INTO table_name(columns) VALUES
        insert_pattern = r'insert\s+into\s+([^\s(]+)\s*\(([^)]+)\)\s+values\s*(.+)'
        match = re.match(insert_pattern, clean_sql, re.IGNORECASE)
        
        if not match:
            return None
        
        table_name = match.group(1).strip()
        columns_str = match.group(2).strip()
        values_str = match.group(3).strip()
        
        # 解析欄位名
        columns = [col.strip() for col in columns_str.split(',')]
        
        # 解析 VALUES 部分的多行數據
        records = SQLParser.parse_values_section(values_str)
        
        return {
            'table_name': table_name,
            'columns': columns,
            'records': records
        }
    
    @staticmethod
    def parse_values_section(values_str):
        """解析 VALUES 部分，支持多行記錄"""
        records = []
        
        # 使用正則表達式找到所有的 (...) 記錄
        pattern = r'\(([^)]+(?:\([^)]*\)[^)]*)*)\)'
        matches = re.findall(pattern, values_str)
        
        for match in matches:
            values = SQLParser.parse_single_record(match)
            if values:
                records.append(values)
        
        return records
    
    @staticmethod
    def parse_single_record(record_str):
        """解析單個記錄的值"""
        values = []
        current_value = ""
        in_quotes = False
        quote_char = None
        paren_depth = 0
        i = 0
        
        while i < len(record_str):
            char = record_str[i]
            
            if char in ('"', "'") and (i == 0 or record_str[i-1] != '\\'):
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                    quote_char = None
                current_value += char
            elif char == '(' and not in_quotes:
                paren_depth += 1
                current_value += char
            elif char == ')' and not in_quotes:
                paren_depth -= 1
                current_value += char
            elif char == ',' and not in_quotes and paren_depth == 0:
                values.append(SQLParser.clean_value(current_value.strip()))
                current_value = ""
            else:
                current_value += char
            
            i += 1
        
        # 添加最後一個值
        if current_value.strip():
            values.append(SQLParser.clean_value(current_value.strip()))
        
        return values
    
    @staticmethod
    def clean_value(value):
        """清理和轉換值"""
        value = value.strip()
        
        # NULL 值
        if value.lower() == 'null':
            return None
        
        # 去除 N' 前綴（Unicode 字符串）
        if value.startswith(("N'", "n'")):
            value = value[2:-1] if value.endswith("'") else value[2:]
            return value
        
        # 普通字符串
        if value.startswith("'") and value.endswith("'"):
            return value[1:-1]
        
        # 數字
        try:
            if '.' in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            pass
        
        # 日期/時間戳
        if value.upper().startswith(('DATE', 'TIMESTAMP')):
            date_match = re.search(r"'([^']+)'", value)
            if date_match:
                return date_match.group(1)
        
        return value

def ensure_structured_index_template():
    """建立結構化索引模板"""
    template = {
        "index_patterns": ["erp-*"],
        "template": {
            "settings": {
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
            "mappings": {
                "dynamic_templates": [
                    {
                        "field_strings": {
                            "match": "field_*",
                            "mapping": {
                                "type": "text",
                                "analyzer": "ik_analyzer",
                                "search_analyzer": "ik_search",
                                "fields": {
                                    "keyword": {"type": "keyword", "ignore_above": 256}
                                }
                            }
                        }
                    }
                ],
                "properties": {
                    "@timestamp": {"type": "date"},
                    "metadata": {
                        "properties": {
                            "source_file": {"type": "keyword"},
                            "table_name": {"type": "keyword"},
                            "record_index": {"type": "integer"},
                            "total_records": {"type": "integer"}
                        }
                    },
                    "searchable_content": {
                        "type": "text",
                        "analyzer": "ik_analyzer", 
                        "search_analyzer": "ik_search"
                    },
                    "all_content": {
                        "type": "text",
                        "analyzer": "ik_analyzer",
                        "search_analyzer": "ik_search"
                    }
                }
            }
        }
    }
    
    try:
        r = session.put(f"{ES_URL}/_index_template/erp-template", 
                       data=json.dumps(template), timeout=30)
        r.raise_for_status()
        log("索引模板已建立: erp-template")
    except requests.exceptions.HTTPError as e:
        log(f"建立索引模板失敗: {e}")
        log(f"回應內容: {e.response.text}")
        # 嘗試使用簡化版模板
        simple_template = {
            "index_patterns": ["erp-*"],
            "template": {
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date"},
                        "metadata": {
                            "properties": {
                                "source_file": {"type": "keyword"},
                                "table_name": {"type": "keyword"},
                                "record_index": {"type": "integer"},
                                "total_records": {"type": "integer"}
                            }
                        }
                    }
                }
            }
        }
        try:
            r2 = session.put(f"{ES_URL}/_index_template/erp-simple-template", 
                           data=json.dumps(simple_template), timeout=30)
            r2.raise_for_status()
            log("簡化索引模板已建立: erp-simple-template")
        except Exception as e2:
            log(f"簡化模板也失敗: {e2}")
            log("將使用默認映射繼續執行")

def create_structured_documents(filename, parsed_data, raw_sql):
    """將解析後的數據轉換為結構化文檔"""
    if not parsed_data:
        return []
    
    table_name = parsed_data['table_name']
    columns = parsed_data['columns']
    records = parsed_data['records']
    
    documents = []
    
    for record_idx, record_values in enumerate(records):
        # 建立基本文檔結構
        doc = {
            "@timestamp": datetime.now(timezone.utc).isoformat(),
            "metadata": {
                "source_file": filename,
                "table_name": table_name,
                "record_index": record_idx,
                "total_records": len(records)
            }
            # 移除 raw_sql 字段以保持結果清爽
        }
        
        # 將每個欄位的值添加到文檔中
        searchable_content = []
        
        for col_idx, column_name in enumerate(columns):
            if col_idx < len(record_values):
                value = record_values[col_idx]
                
                if value is not None:
                    # 清理欄位名（移除可能的空白和特殊字符）
                    clean_col_name = re.sub(r'[^\w]', '_', column_name.lower())
                    field_key = f"field_{clean_col_name}"
                    
                    # 存儲原始值
                    doc[field_key] = str(value)
                    
                    # 如果是文本，加入繁簡轉換
                    if isinstance(value, str) and len(value) > 0:
                        # 轉繁體
                        traditional = cc_s2t.convert(str(value))
                        # 轉簡體  
                        simplified = cc_t2s.convert(str(value))
                        
                        # 加入可搜索內容
                        searchable_content.extend([str(value), traditional, simplified])
        
        # 添加便於搜索的綜合內容字段
        doc["searchable_content"] = " ".join(set(searchable_content))
        
        # 新增：全內容搜索字段（包含所有文本內容）
        all_text_content = []
        for key, value in doc.items():
            if key.startswith('field_') and isinstance(value, str):
                all_text_content.append(str(value))
        doc["all_content"] = " ".join(set(all_text_content + searchable_content))
        
        # 生成文檔 ID
        doc_id = hashlib.sha256(f"{filename}::{table_name}::{record_idx}".encode()).hexdigest()
        
        documents.append((doc_id, doc))
    
    return documents

def get_index_name(table_name):
    """根據表名生成索引名稱"""
    # 將 schema.table 格式轉換為 erp-schema-table-YYYY.MM.DD
    clean_table = re.sub(r'[^\w.]', '_', table_name.lower())
    clean_table = clean_table.replace('.', '-')
    date_suffix = datetime.now(timezone.utc).strftime("%Y.%m.%d")
    return f"erp-{clean_table}-{date_suffix}"

def bulk_index_documents(index_name, documents):
    """批量索引文檔"""
    if not documents:
        return
    
    lines = []
    for doc_id, doc in documents:
        lines.append(json.dumps({"index": {"_index": index_name, "_id": doc_id}}, ensure_ascii=False))
        lines.append(json.dumps(doc, ensure_ascii=False))
    
    payload = "\n".join(lines) + "\n"
    
    r = session.post(f"{ES_URL}/_bulk", 
                    data=payload.encode("utf-8"), 
                    headers={"Content-Type": "application/x-ndjson"}, 
                    timeout=60)
    r.raise_for_status()
    
    result = r.json()
    if result.get("errors"):
        errors = [item for item in result.get("items", []) 
                 if item.get("index", {}).get("error")]
        log(f"⚠️ 索引錯誤範例: {errors[:2]}")
    
    log(f"✅ 成功索引 {len(documents)} 份文檔到 {index_name}")

def robust_read_text(path):
    """強健的文本讀取"""
    b = path.read_bytes()
    encodings = ["utf-8", "utf-8-sig", "cp950", "big5", "utf-16le", "utf-16be"]
    
    for encoding in encodings:
        try:
            return b.decode(encoding).replace("\r", "")
        except:
            continue
    
    return b.decode("utf-8", "ignore").replace("\r", "")

def process_sql_file(file_path):
    """處理 SQL 文件"""
    log(f"🔄 處理文件: {file_path.name}")
    
    try:
        # 讀取文件內容
        content = robust_read_text(file_path)
        
        # 分割 SQL 語句
        statements = []
        for stmt in re.split(r';\s*(?:\n|$)', content):
            stmt = stmt.strip()
            if stmt and not stmt.startswith('--'):
                statements.append(stmt)
        
        total_docs = 0
        
        for stmt_idx, statement in enumerate(statements):
            if 'insert into' in statement.lower():
                # 解析 INSERT 語句
                parsed = SQLParser.parse_insert_statement(statement)
                
                if parsed:
                    # 建立結構化文檔
                    docs = create_structured_documents(file_path.name, parsed, statement)
                    
                    if docs:
                        # 獲取索引名稱
                        index_name = get_index_name(parsed['table_name'])
                        
                        # 批量索引
                        for i in range(0, len(docs), BATCH_SIZE):
                            batch = docs[i:i + BATCH_SIZE]
                            bulk_index_documents(index_name, batch)
                        
                        total_docs += len(docs)
                        log(f"📊 表 {parsed['table_name']}: {len(docs)} 記錄")
                else:
                    log(f"⚠️ 無法解析語句 {stmt_idx + 1}")
        
        # 重命名為 .done
        done_path = file_path.with_suffix(file_path.suffix + ".done")
        try:
            file_path.rename(done_path)
            log(f"✅ 完成處理: {done_path.name} (共 {total_docs} 記錄)")
        except Exception as e:
            log(f"⚠️ 重命名失敗: {e}")
            
    except Exception as e:
        log(f"❌ 處理文件失敗 {file_path.name}: {e}")

def main():
    """主程序"""
    IMPORT_DIR.mkdir(parents=True, exist_ok=True)
    ensure_structured_index_template()
    
    log(f"👁️ 監控目錄: {IMPORT_DIR}")
    log("🚀 SQL 智能解析器啟動")
    
    while True:
        # 尋找待處理的 .sql 文件
        sql_files = sorted([
            f for f in IMPORT_DIR.glob("*.sql") 
            if not f.name.endswith(".done")
        ])
        
        for file_path in sql_files:
            process_sql_file(file_path)
        
        if not sql_files:
            log("😴 沒有新文件，等待中...")
        
        time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()
