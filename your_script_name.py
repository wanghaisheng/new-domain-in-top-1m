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
DOMAINS_RANKINGS_FILE = 'domains_rankings.csv'  # 存储域名排名历史
DOMAINS_FIRST_SEEN_FILE = 'domains_first_seen.csv'  # 存储域名首次出现日期
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

        # 创建2025年的表
        create_year_table(conn, 2025)

        conn.commit()
        logging.info(f"Database tables created successfully in {DB_FILE}")
    except Exception as e:
        logging.error(f"Error creating database tables: {e}")
    finally:
        if conn:
            conn.close()

def create_year_table(conn, year):
    """创建指定年份的排名表并添加所有日期列"""
    cursor = conn.cursor()
    table_name = f"rankings_{year}"
    
    # 检查表是否存在
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    table_exists = cursor.fetchone() is not None
    
    if not table_exists:
        # 创建表
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                domain TEXT PRIMARY KEY
            )
        """)
        logging.info(f"Created new table: {table_name}")
        
        # 为该年份的每一天添加列
        from datetime import date, timedelta
        
        start_date = date(year, 1, 1)
        # 检查是否是闰年
        if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
            end_date = date(year, 12, 31)
        else:
            end_date = date(year, 12, 31)
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN '{date_str}' INTEGER")
                current_date += timedelta(days=1)
            except Exception as e:
                logging.error(f"Error adding column {date_str}: {e}")
                current_date += timedelta(days=1)
                continue
        
        logging.info(f"Added all date columns for year {year}")

def load_domains_history():
    """从CSV文件加载历史域名数据和排名"""
    domains_rankings = {}  # 域名排名历史
    domains_first_seen = {}  # 域名首次出现日期
    
    # 加载域名首次出现日期
    if os.path.exists(DOMAINS_FIRST_SEEN_FILE):
        try:
            with open(DOMAINS_FIRST_SEEN_FILE, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # 跳过标题行
                for row in reader:
                    if len(row) == 2:
                        domain, first_seen = row
                        domains_first_seen[domain] = first_seen
            logging.info(f"Loaded {len(domains_first_seen)} domains first seen dates")
        except Exception as e:
            logging.error(f"Error loading domains first seen dates: {e}")
    
    # 加载域名排名历史
    if os.path.exists(DOMAINS_RANKINGS_FILE):  # 修改这里使用DOMAINS_RANKINGS_FILE
        try:
            with open(DOMAINS_RANKINGS_FILE, 'r', newline='', encoding='utf-8') as f:  # 修改这里使用DOMAINS_RANKINGS_FILE
                reader = csv.reader(f)
                headers = next(reader)  # 获取标题行（日期列）
                
                # 第一列是域名，其余列是日期
                dates = headers[1:]
                
                for row in reader:
                    if len(row) > 1:
                        domain = row[0]
                        domains_rankings[domain] = {}
                        
                        # 将每个日期的排名添加到域名的排名历史中
                        for i, date in enumerate(dates):
                            if i + 1 < len(row) and row[i + 1]:
                                try:
                                    domains_rankings[domain][date] = int(row[i + 1])
                                except ValueError:
                                    domains_rankings[domain][date] = 0
            
            logging.info(f"Loaded ranking history for {len(domains_rankings)} domains across {len(dates)} dates")
        except Exception as e:
            logging.error(f"Error loading domains ranking history: {e}")
    
    return domains_rankings, domains_first_seen

def save_domains_history(domains_rankings, domains_first_seen, current_date):
    """将域名排名数据保存到CSV文件"""
    # 保存域名首次出现日期
    try:
        with open(DOMAINS_FIRST_SEEN_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['domain', 'first_seen'])
            for domain, first_seen in domains_first_seen.items():
                writer.writerow([domain, first_seen])
        logging.info(f"Saved first seen dates for {len(domains_first_seen)} domains")
    except Exception as e:
        logging.error(f"Error saving domains first seen dates: {e}")
    
    # 保存域名排名历史
    try:
        # 获取所有日期列
        all_dates = set()
        for domain_data in domains_rankings.values():
            all_dates.update(domain_data.keys())
        
        # 按日期排序
        sorted_dates = sorted(all_dates)
        
        # 修改这里，使用DOMAINS_RANKINGS_FILE而不是DOMAINS_HISTORY_FILE
        with open(DOMAINS_RANKINGS_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # 写入标题行：域名和所有日期
            header = ['domain'] + sorted_dates
            writer.writerow(header)
            
            # 写入每个域名的排名数据
            for domain, rankings in domains_rankings.items():
                row = [domain]
                for date in sorted_dates:
                    row.append(rankings.get(date, 0))  # 如果没有排名，使用0
                writer.writerow(row)
        
        logging.info(f"Saved ranking history for {len(domains_rankings)} domains across {len(sorted_dates)} dates")
    except Exception as e:
        logging.error(f"Error saving domains ranking history: {e}")

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
    domains_rankings, domains_first_seen = load_domains_history()
    
    # 检查是否是首次运行（没有历史数据）
    is_first_run = len(domains_first_seen) == 0
    
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
        
        # 确保当前年份的表存在
        create_year_table(conn, current_year)
        
        # 确保2025年的表存在
        create_year_table(conn, 2025)

        # Check if the column exist
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]  # fetch column names
        if date not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN '{date}' INTEGER")
            logging.info(f"Added new column {date} to table {table_name}")

        # 批量导入历史域名数据 - 修复这里的错误，使用 domains_first_seen 而不是 domains_history
        if domains_first_seen:
            domains_to_import = [(domain, first_seen) for domain, first_seen in domains_first_seen.items()]
            cursor.executemany(
                "INSERT OR IGNORE INTO domains (domain, first_seen) VALUES (?, ?)", 
                domains_to_import
            )
            logging.info(f"Imported {len(domains_first_seen)} domains from history file")

        # 准备批量处理新数据
        domains_to_insert = []
        rankings_to_insert = []
        
        # 预处理数据
        current_domains = set()  # 当前文件中的所有域名
        
        for row in data:
            if len(row) == 2:
                try:
                    rank = int(row[0].strip())
                    domain = row[1].strip()
                    current_domains.add(domain)
                    
                    # 更新排名历史
                    if domain not in domains_rankings:
                        domains_rankings[domain] = {}
                    domains_rankings[domain][date] = rank
                    
                    # 检查是否为新域名
                    if domain not in domains_first_seen:
                        domains_first_seen[domain] = date
                        domains_to_insert.append((domain, date))  # 添加到待插入列表
                        # 只有在非首次运行时才将域名添加到new_domains
                        if not is_first_run:
                            new_domains.append(domain)
                    
                    rankings_to_insert.append((domain, rank, rank))
                    
                except (ValueError, IndexError) as e:
                    logging.warning(f"Could not process row {row}: {e}")
        
        # 对于历史中存在但当前文件中不存在的域名，将其排名设为0
        for domain in domains_rankings:
            if domain not in current_domains:
                domains_rankings[domain][date] = 0
        
        # 批量插入新域名
        if domains_to_insert:
            cursor.executemany(
                "INSERT OR IGNORE INTO domains (domain, first_seen) VALUES (?, ?)", 
                domains_to_insert
            )
            if is_first_run:
                logging.info(f"First run: Added {len(domains_to_insert)} domains to history")
            else:
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
        save_domains_history(domains_rankings, domains_first_seen, date)
        
        # 更新处理历史记录
        process_history = load_process_history()
        if date not in process_history['dates']:
            process_history['dates'].append(date)
        save_process_history(process_history)
        
        # 首次运行时记录日志
        if is_first_run:
            logging.info("First run detected - no new domains reported")
        
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

    # 创建新域名存储目录
    new_domains_dir = os.path.join(github_workspace, "new_domains")
    if not os.path.exists(new_domains_dir):
        os.makedirs(new_domains_dir)
        logging.info(f"Created directory for new domains: {new_domains_dir}")

    # 更新数据库
    new_domains = update_database(zip_file_path)
    if new_domains:
        print(f"New domains added: {len(new_domains)}")  # 只打印数量，避免输出过多
        # 添加日期后缀到文件名
        date_suffix = datetime.now().strftime('%Y-%m-%d')
        new_domains_file = os.path.join(new_domains_dir, f"{date_suffix}.txt")
        with open(new_domains_file, "w", encoding='utf-8') as f:
            for domain in new_domains:
                f.write(f"{domain}\n")
        logging.info(f"{len(new_domains)} new domains written to {new_domains_file}")
        
        # 同时创建一个固定名称的文件，用于GitHub Actions
        fixed_name_file = os.path.join(github_workspace, "new_domains.txt")
        with open(fixed_name_file, "w", encoding='utf-8') as f:
            for domain in new_domains:
                f.write(f"{domain}\n")
    else:
        print("No new domains added today.")
        # 创建空的固定名称文件，确保GitHub Actions不会失败
        fixed_name_file = os.path.join(github_workspace, "new_domains.txt")
        with open(fixed_name_file, "w", encoding='utf-8') as f:
            pass
        logging.info("Created empty new_domains.txt file")
    
    # 清理数据库文件
    cleanup_database()
