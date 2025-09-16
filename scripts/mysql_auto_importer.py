#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
優化版 MySQL 自動導入服務
- 支援大型 SQL 檔案處理
- 智能批次執行
- 交易管理和錯誤恢復
- 進度追蹤
"""

import os, time, json, hashlib, logging, re, pymysql
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Iterator
from pymysql.cursors import DictCursor

# ============== 配置 ==============
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "fuhsin_erp_demo")

# 目錄配置
WATCH_DIR = Path(os.getenv("SQL_WATCH_DIR", "/sql/incoming"))
DONE_DIR = WATCH_DIR / ".done"
ERROR_DIR = WATCH_DIR / ".error"
PROGRESS_DIR = WATCH_DIR / ".progress"
STATE_FILE = WATCH_DIR / ".import_state.json"
LOG_FILE = Path("/logs/importer/mysql_importer.log")

# 效能配置
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "10"))
BATCH_SIZE = int(os.getenv("SQL_BATCH_SIZE", "1000"))      # 每批執行的 INSERT 數量
MAX_RETRY = int(os.getenv("MAX_RETRY", "3"))               # 最大重試次數
CONNECTION_POOL_SIZE = int(os.getenv("POOL_SIZE", "5"))    # 連線池大小

# ============== 日誌設定 ==============
os.makedirs(LOG_FILE.parent, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============== 連線池管理 ==============
class MySQLConnectionPool:
    """MySQL 連線池"""
    
    def __init__(self, size: int = CONNECTION_POOL_SIZE):
        self.size = size
        self.connections = []
        self.used_connections = set()
        self._create_connections()
    
    def _create_connections(self):
        """建立連線池"""
        for _ in range(self.size):
            conn = self._create_connection()
            if conn:
                self.connections.append(conn)
    
    def _create_connection(self):
        """建立單一連線"""
        try:
            conn = pymysql.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE,
                charset='utf8mb4',
                autocommit=False,
                cursorclass=DictCursor
            )
            return conn
        except Exception as e:
            logger.error(f"建立連線失敗: {e}")
            return None
    
    def get_connection(self):
        """取得可用連線"""
        while self.connections:
            conn = self.connections.pop(0)
            try:
                # 檢查連線是否有效
                conn.ping(reconnect=True)
                self.used_connections.add(conn)
                return conn
            except:
                # 連線失效，建立新連線
                new_conn = self._create_connection()
                if new_conn:
                    self.used_connections.add(new_conn)
                    return new_conn
        
        # 無可用連線，等待或建立新連線
        if len(self.used_connections) < self.size:
            new_conn = self._create_connection()
            if new_conn:
                self.used_connections.add(new_conn)
                return new_conn
        
        raise Exception("無可用連線")
    
    def return_connection(self, conn):
        """歸還連線"""
        if conn in self.used_connections:
            self.used_connections.remove(conn)
            try:
                conn.rollback()  # 確保清理狀態
                self.connections.append(conn)
            except:
                # 連線已損壞，不放回池中
                pass
    
    def close_all(self):
        """關閉所有連線"""
        for conn in self.connections + list(self.used_connections):
            try:
                conn.close()
            except:
                pass
        self.connections.clear()
        self.used_connections.clear()

# 全域連線池
connection_pool = MySQLConnectionPool()

# ============== SQL 解析器 ==============
class SQLParser:
    """智能 SQL 解析器"""
    
    @staticmethod
    def parse_file(filepath: Path) -> Iterator[Tuple[str, str]]:
        """
        解析 SQL 檔案，返回 (語句類型, SQL) 的迭代器
        支援大檔案串流處理
        """
        current_statement = []
        in_string = False
        string_char = None
        line_count = 0
        
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line_count += 1
                
                # 跳過註解和空行
                stripped = line.strip()
                if not stripped or stripped.startswith('--') or stripped.startswith('#'):
                    continue
                
                # 處理多行 SQL
                i = 0
                while i < len(line):
                    char = line[i]
                    
                    # 處理字串
                    if not in_string:
                        if char in ["'", '"', '`']:
                            in_string = True
                            string_char = char
                    else:
                        if char == string_char:
                            # 檢查是否轉義
                            if i + 1 < len(line) and line[i + 1] == string_char:
                                i += 1  # 跳過轉義字元
                            else:
                                in_string = False
                                string_char = None
                    
                    current_statement.append(char)
                    
                    # 檢查語句結束
                    if not in_string and char == ';':
                        sql = ''.join(current_statement).strip()
                        if sql and sql != ';':
                            # 判斷語句類型
                            sql_upper = sql.upper()
                            if sql_upper.startswith('INSERT'):
                                stmt_type = 'INSERT'
                            elif sql_upper.startswith('UPDATE'):
                                stmt_type = 'UPDATE'
                            elif sql_upper.startswith('DELETE'):
                                stmt_type = 'DELETE'
                            elif sql_upper.startswith('CREATE'):
                                stmt_type = 'CREATE'
                            elif sql_upper.startswith('DROP'):
                                stmt_type = 'DROP'
                            elif sql_upper.startswith('ALTER'):
                                stmt_type = 'ALTER'
                            else:
                                stmt_type = 'OTHER'
                            
                            yield (stmt_type, sql)
                        
                        current_statement = []
                    
                    i += 1
        
        # 處理最後一個語句（如果沒有分號結尾）
        if current_statement:
            sql = ''.join(current_statement).strip()
            if sql:
                yield ('OTHER', sql)
        
        logger.info(f"📄 解析完成，共 {line_count} 行")
    
    @staticmethod
    def optimize_insert(sql: str) -> List[str]:
        """
        優化 INSERT 語句，將大批量 INSERT 分割成小批次
        """
        # 檢查是否為多值 INSERT
        pattern = r'INSERT\s+INTO\s+`?(\w+)`?\s*\([^)]+\)\s*VALUES\s*(.+);?$'
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            return [sql]
        
        table = match.group(1)
        values_part = match.group(2).rstrip(';')
        
        # 分割 VALUES
        values_list = []
        current_value = []
        paren_count = 0
        in_string = False
        
        for char in values_part:
            if not in_string:
                if char == '(':
                    paren_count += 1
                elif char == ')':
                    paren_count -= 1
                elif char == "'":
                    in_string = True
            else:
                if char == "'" and (len(current_value) == 0 or current_value[-1] != '\\'):
                    in_string = False
            
            current_value.append(char)
            
            if paren_count == 0 and char == ')':
                values_list.append(''.join(current_value).strip())
                current_value = []
                # 跳過逗號和空格
                continue
        
        # 分批建立 INSERT 語句
        if len(values_list) <= BATCH_SIZE:
            return [sql]
        
        # 取得欄位列表
        columns_match = re.search(r'\(([^)]+)\)', sql)
        columns = columns_match.group(0) if columns_match else ''
        
        batches = []
        for i in range(0, len(values_list), BATCH_SIZE):
            batch_values = values_list[i:i + BATCH_SIZE]
            batch_sql = f"INSERT INTO `{table}` {columns} VALUES {','.join(batch_values)};"
            batches.append(batch_sql)
        
        logger.info(f"🔄 分割 INSERT 為 {len(batches)} 批，每批最多 {BATCH_SIZE} 筆")
        return batches

# ============== 進度管理 ==============
class ProgressTracker:
    """進度追蹤器"""
    
    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.progress_file = PROGRESS_DIR / f"{filepath.stem}.progress"
        self.progress = self.load_progress()
    
    def load_progress(self) -> Dict:
        """載入進度"""
        PROGRESS_DIR.mkdir(exist_ok=True)
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        return {
            'total_statements': 0,
            'processed_statements': 0,
            'success_count': 0,
            'error_count': 0,
            'last_position': 0,
            'errors': []
        }
    
    def save_progress(self):
        """儲存進度"""
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2, default=str)
    
    def update(self, success: bool, error_msg: str = None):
        """更新進度"""
        self.progress['processed_statements'] += 1
        if success:
            self.progress['success_count'] += 1
        else:
            self.progress['error_count'] += 1
            if error_msg:
                self.progress['errors'].append({
                    'statement': self.progress['processed_statements'],
                    'error': error_msg[:500],
                    'time': datetime.now().isoformat()
                })
        self.save_progress()
    
    def complete(self):
        """完成處理"""
        if self.progress_file.exists():
            self.progress_file.unlink()

# ============== 檔案處理 ==============
def get_file_hash(filepath: Path) -> str:
    """計算檔案 MD5"""
    md5 = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            md5.update(chunk)
    return md5.hexdigest()

def load_state() -> Dict:
    """載入處理狀態"""
    if STATE_FILE.exists():
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_state(state: Dict):
    """儲存處理狀態"""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2, default=str)

def move_file(src: Path, dst_dir: Path) -> Path:
    """移動檔案"""
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / src.name
    
    # 避免覆蓋
    if dst.exists():
        timestamp = int(time.time())
        dst = dst_dir / f"{src.stem}_{timestamp}{src.suffix}"
    
    src.rename(dst)
    return dst

# ============== SQL 執行 ==============
def execute_sql_batch(connection, statements: List[Tuple[str, str]], progress: ProgressTracker) -> Tuple[int, int]:
    """
    批次執行 SQL 語句
    返回 (成功數, 失敗數)
    """
    success_count = 0
    error_count = 0
    
    with connection.cursor() as cursor:
        for stmt_type, sql in statements:
            try:
                # 對 INSERT 進行優化
                if stmt_type == 'INSERT':
                    optimized_sqls = SQLParser.optimize_insert(sql)
                    for opt_sql in optimized_sqls:
                        cursor.execute(opt_sql)
                        connection.commit()
                        success_count += 1
                else:
                    cursor.execute(sql)
                    connection.commit()
                    success_count += 1
                
                progress.update(True)
                
                # 每 100 個成功語句輸出一次進度
                if success_count % 100 == 0:
                    logger.info(f"✅ 已處理 {success_count} 個語句")
                    
            except Exception as e:
                connection.rollback()
                error_count += 1
                error_msg = str(e)[:200]
                logger.error(f"❌ SQL 執行失敗: {error_msg}")
                progress.update(False, error_msg)
                
                # 如果錯誤太多，停止處理
                if error_count > 100:
                    logger.error("錯誤過多，停止處理")
                    break
    
    return success_count, error_count

def process_sql_file(filepath: Path) -> bool:
    """
    處理單一 SQL 檔案
    返回是否成功
    """
    logger.info(f"📝 開始處理: {filepath.name}")
    
    # 初始化進度追蹤
    progress = ProgressTracker(filepath)
    
    # 取得連線
    connection = None
    try:
        connection = connection_pool.get_connection()
        
        # 解析並執行 SQL
        statements = []
        total_statements = 0
        
        for stmt_type, sql in SQLParser.parse_file(filepath):
            total_statements += 1
            statements.append((stmt_type, sql))
            
            # 批次執行
            if len(statements) >= 100:
                success, errors = execute_sql_batch(connection, statements, progress)
                statements = []
                logger.info(f"批次完成: 成功 {success}, 失敗 {errors}")
        
        # 執行剩餘語句
        if statements:
            success, errors = execute_sql_batch(connection, statements, progress)
            logger.info(f"最後批次: 成功 {success}, 失敗 {errors}")
        
        # 完成處理
        progress.complete()
        
        total_success = progress.progress['success_count']
        total_errors = progress.progress['error_count']
        
        if total_errors == 0:
            logger.info(f"✅ 完成: 成功執行 {total_success} 個語句")
            return True
        else:
            logger.warning(f"⚠️ 完成: 成功 {total_success}, 失敗 {total_errors}")
            return total_errors < total_success * 0.1  # 錯誤率小於 10% 視為成功
            
    except Exception as e:
        logger.error(f"❌ 處理失敗: {e}", exc_info=True)
        return False
        
    finally:
        if connection:
            connection_pool.return_connection(connection)

# ============== 主程式 ==============
def wait_for_mysql(max_retries: int = 30) -> bool:
    """等待 MySQL 就緒"""
    for i in range(max_retries):
        try:
            conn = connection_pool.get_connection()
            connection_pool.return_connection(conn)
            logger.info("✅ MySQL 連線成功")
            return True
        except Exception as e:
            if i < max_retries - 1:
                logger.info(f"⏳ 等待 MySQL 就緒... ({i+1}/{max_retries})")
                time.sleep(2)
            else:
                logger.error(f"❌ MySQL 連線失敗: {e}")
                return False
    return False

def scan_and_process():
    """掃描並處理 SQL 檔案"""
    # 確保目錄存在
    WATCH_DIR.mkdir(parents=True, exist_ok=True)
    DONE_DIR.mkdir(parents=True, exist_ok=True)
    ERROR_DIR.mkdir(parents=True, exist_ok=True)
    
    # 載入狀態
    state = load_state()
    
    # 掃描檔案
    sql_files = sorted(WATCH_DIR.glob("*.sql"))
    
    for sql_file in sql_files:
        # 檢查檔案大小
        file_size = sql_file.stat().st_size / (1024 * 1024)  # MB
        logger.info(f"📦 發現檔案: {sql_file.name} ({file_size:.2f} MB)")
        
        # 計算檔案雜湊
        file_hash = get_file_hash(sql_file)
        file_key = sql_file.name
        
        # 檢查是否已處理
        if file_key in state and state[file_key].get('hash') == file_hash:
            logger.info(f"⏭️ 跳過已處理: {sql_file.name}")
            move_file(sql_file, DONE_DIR)
            continue
        
        # 處理檔案
        start_time = time.time()
        success = process_sql_file(sql_file)
        elapsed = time.time() - start_time
        
        # 更新狀態
        state[file_key] = {
            'hash': file_hash,
            'processed_at': datetime.now().isoformat(),
            'success': success,
            'elapsed_seconds': elapsed,
            'file_size_mb': file_size
        }
        save_state(state)
        
        # 移動檔案
        if success:
            logger.info(f"✅ 處理成功，耗時 {elapsed:.2f} 秒")
            move_file(sql_file, DONE_DIR)
        else:
            logger.error(f"❌ 處理失敗")
            move_file(sql_file, ERROR_DIR)
        
        # 處理大檔案後暫停
        if file_size > 10:  # 大於 10MB
            logger.info("處理大檔案後暫停 5 秒...")
            time.sleep(5)

def main():
    """主程式"""
    logger.info("=" * 60)
    logger.info("🚀 MySQL 自動導入服務啟動")
    logger.info(f"📁 監控目錄: {WATCH_DIR}")
    logger.info(f"🔄 掃描間隔: {SCAN_INTERVAL} 秒")
    logger.info(f"📦 批次大小: {BATCH_SIZE}")
    logger.info("=" * 60)
    
    # 等待 MySQL
    if not wait_for_mysql():
        logger.error("無法連線到 MySQL，服務終止")
        return
    
    # 主循環
    no_file_count = 0
    
    try:
        while True:
            try:
                # 掃描並處理檔案
                files_found = len(list(WATCH_DIR.glob("*.sql")))
                
                if files_found > 0:
                    logger.info(f"🔍 發現 {files_found} 個 SQL 檔案")
                    scan_and_process()
                    no_file_count = 0
                else:
                    no_file_count += 1
                    # 動態調整掃描間隔
                    sleep_time = min(SCAN_INTERVAL * (1 + no_file_count // 10), 60)
                    if no_file_count % 10 == 0:
                        logger.info(f"💤 無新檔案，等待 {sleep_time} 秒...")
                    time.sleep(sleep_time)
                    
            except Exception as e:
                logger.error(f"❌ 處理錯誤: {e}", exc_info=True)
                time.sleep(30)
                
    except KeyboardInterrupt:
        logger.info("⏹️ 收到中斷信號，正在關閉...")
    finally:
        connection_pool.close_all()
        logger.info("👋 服務已停止")

if __name__ == "__main__":
    main()
