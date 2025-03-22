import csv
import logging
import os
from datetime import datetime
import sqlite3
import zipfile
import codecs
import json

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_FILE = 'domain_rank.db'  # 数据库文件名
ZIP_FILE = 'tranco.zip'
CSV_FILE_NAME = 'top-1m.csv'  # CSV 文件名
DOMAINS_HISTORY_FILE = 'domains_history.csv'  # 历史域名数据文件
PROCESS_HISTORY_FILE = 'process_history.json'  # 处理历史记录文件

def create_database():
    """创建数据库表."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # 创建 domains 表，用于存储域名和首次出现日期
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS domains (
                domain TEXT PRIMARY KEY,
                first_seen DATE
            )
        """)

        # 添加性能优化设置
        cursor.execute("PRAGMA journal_mode=WAL")  # 使用WAL模式提高写入性能
        cursor.execute("PRAGMA synchronous=NORMAL")  # 降低同步级别提高性能
        cursor.execute("PRAGMA cache_size=10000")  # 增加缓存大小
        cursor.execute("PRAGMA temp_store=MEMORY")  # 临时表存储在内存中

        conn.commit()
        logging.info(f"Database tables created successfully in {DB_FILE}")
    except Exception as e:
        logging.error(f"Error creating database tables: {e}")
    finally:
        if conn:
            conn.close()

def load_domains_history():
    """从CSV文件加载历史域名数据"""
    domains = {}
    if os.path.exists(DOMAINS_HISTORY_FILE):
        try:
            with open(DOMAINS_HISTORY_FILE, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # 跳过标题行
                for row in reader:
                    if len(row) == 2:
                        domain, first_seen = row
                        domains[domain] = first_seen
            logging.info(f"Loaded {len(domains)} domains from history file")
        except Exception as e:
            logging.error(f"Error loading domains history: {e}")
    return domains

def save_domains_history(domains):
    """将域名数据保存到CSV文件"""
    try:
        with open(DOMAINS_HISTORY_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['domain', 'first_seen'])
            for domain, first_seen in domains.items():
                writer.writerow([domain, first_seen])
        logging.info(f"Saved {len(domains)} domains to history file")
    except Exception as e:
        logging.error(f"Error saving domains history: {e}")

def load_process_history():
    """加载处理历史记录"""
    if os.path.exists(PROCESS_HISTORY_FILE):
        try:
            with open(PROCESS_HISTORY_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading process history: {e}")
    return {'dates': []}

def save_process_history(history):
    """保存处理历史记录"""
    try:
        with open(PROCESS_HISTORY_FILE, 'w') as f:
            json.dump(history, f, indent=2)
        logging.info("Process history updated")
    except Exception as e:
        logging.error(f"Error saving process history: {e}")

def update_database(zip_file):
    """更新数据库，并返回新出现的域名列表."""
    conn = None
    new_domains = []  # Initialize new_domains list
    
    # 加载历史域名数据
    domains_history = load_domains_history()
    
    try:
        with zipfile.ZipFile(zip_file, 'r') as z:
            with z.open(CSV_FILE_NAME, 'r') as csvfile:
                reader = csv.reader(codecs.getreader("utf-8")(csvfile))
                data = list(reader)[1:]  # Skip header and store data

        date = datetime.now().strftime('%Y-%m-%d')
        current_year = datetime.now().year  # get current year
        table_name = f"rankings_{current_year}"

    except FileNotFoundError:
        logging.error(f"Zip file not found: {zip_file}")
        return []  # Return empty list in case of FileNotFoundError
    except Exception as e:
        logging.error(f"Error processing zip file: {e}")
        return []  # Return empty list in case of other errors

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    try:
        # 性能优化设置
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA cache_size=10000")
        cursor.execute("PRAGMA temp_store=MEMORY")
        
        # 开始事务 - 大幅提高批量插入性能
        conn.execute("BEGIN TRANSACTION")
        
        # Check if the table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        table_exists = cursor.fetchone() is not None

        if not table_exists:
            # Create the table if it doesn't exist
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    domain TEXT PRIMARY KEY
                )
            """)
            logging.info(f"Created new table: {table_name}")

        # Check if the column exist
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]  # fetch column names
        if date not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN '{date}' INTEGER")
            logging.info(f"Added new column {date} to table {table_name}")

        # 批量导入历史域名数据
        if domains_history:
            domains_to_import = [(domain, first_seen) for domain, first_seen in domains_history.items()]
            cursor.executemany(
                "INSERT OR IGNORE INTO domains (domain, first_seen) VALUES (?, ?)", 
                domains_to_import
            )
            logging.info(f"Imported {len(domains_history)} domains from history file")

        # 准备批量处理新数据
        domains_to_insert = []
        rankings_to_insert = []
        
        # 预处理数据
        for row in data:
            if len(row) == 2:
                try:
                    rank = int(row[0].strip())
                    domain = row[1].strip()
                    
                    # 检查域名是否已存在
                    if domain not in domains_history:
                        domains_to_insert.append((domain, date))
                        new_domains.append(domain)
                        domains_history[domain] = date
                    
                    rankings_to_insert.append((domain, rank, rank))
                    
                except (ValueError, IndexError) as e:
                    logging.warning(f"Could not process row {row}: {e}")
        
        # 批量插入新域名
        if domains_to_insert:
            cursor.executemany(
                "INSERT OR IGNORE INTO domains (domain, first_seen) VALUES (?, ?)", 
                domains_to_insert
            )
            logging.info(f"Added {len(domains_to_insert)} new domains")
        
        # 批量更新排名
        if rankings_to_insert:
            cursor.executemany(
                f"""
                INSERT INTO {table_name} (domain, '{date}')
                VALUES (?, ?)
                ON CONFLICT(domain) DO UPDATE SET '{date}' = ?
                """, 
                rankings_to_insert
            )
            logging.info(f"Updated {len(rankings_to_insert)} domain rankings")
        
        # 提交事务
        conn.commit()
        
        # 导出最新的域名数据
        save_domains_history(domains_history)
        
        # 更新处理历史记录
        process_history = load_process_history()
        if date not in process_history['dates']:
            process_history['dates'].append(date)
        save_process_history(process_history)
        
    except Exception as e:
        logging.error(f"Error updating database: {e}")
        conn.rollback()  # 出错时回滚事务
    finally:
        if conn:
            conn.close()
    
    return new_domains

def cleanup_database():
    """清理数据库文件"""
    if os.path.exists(DB_FILE):
        try:
            os.remove(DB_FILE)
            logging.info(f"Removed database file: {DB_FILE}")
        except Exception as e:
            logging.error(f"Error removing database file: {e}")

if __name__ == "__main__":
    # 示例用法：
    github_workspace = os.environ.get("GITHUB_WORKSPACE", ".")  # Get workspace, default to current dir

    # 确保 data 目录存在
    if not os.path.exists("data"):
        os.makedirs("data")

    # 下载 zip 文件
    zip_file_path = os.path.join("data", ZIP_FILE)

    if not os.path.exists(zip_file_path):
        logging.info(f"Downloading {ZIP_FILE}...")
        try:
            import requests
            response = requests.get("https://tranco-list.eu/top-1m.csv.zip", stream=True)
            with open(zip_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
            logging.info(f"{ZIP_FILE} downloaded successfully.")
        except ImportError:
            logging.warning("Requests library not found, falling back to os.system")
            os.system(f"wget https://tranco-list.eu/top-1m.csv.zip -O {zip_file_path}")
            logging.info(f"{ZIP_FILE} downloaded successfully.")
        except Exception as e:
            logging.error(f"Error downloading file: {e}")

    # 创建数据库
    create_database()

    # 更新数据库
    new_domains = update_database(zip_file_path)
    if new_domains:
        print(f"New domains added: {new_domains}")
        # 添加日期后缀到文件名
        date_suffix = datetime.now().strftime('%Y-%m-%d')
        new_domains_file = os.path.join(github_workspace, f"new_domains_{date_suffix}.txt")
        with open(new_domains_file, "w") as f:
            for domain in new_domains:
                f.write(f"{domain}\n")
        logging.info(f"New domains written to {new_domains_file}")
    else:
        print("No new domains added today.")
    
    # 清理数据库文件
    cleanup_database()
