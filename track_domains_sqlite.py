import os
import sys
import csv
import logging
import sqlite3
import zipfile
import codecs
import json
import pandas as pd
from datetime import datetime, date, timedelta

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Paths from workflow
DB_FILE = os.path.join('data', 'persisted-to-cache', 'domain_rank.db')
BACKUP_DIR = 'domains_rankings_backup'
BACKUP_SPLIT_SIZE = 1000000  # 1 million domains per file
PROCESS_HISTORY_FILE = os.path.join('data', 'process_history.json')

# ========== 新增：处理历史记录 ===========
def load_process_history():
    if os.path.exists(PROCESS_HISTORY_FILE):
        try:
            with open(PROCESS_HISTORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"加载处理历史记录失败: {e}")
    return {'dates': []}

def save_process_history(history):
    try:
        with open(PROCESS_HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
        logging.info("处理历史记录已更新")
    except Exception as e:
        logging.error(f"保存处理历史记录失败: {e}")

# ========== 新增：动态创建年度表和每日列 ===========
def create_year_table(conn, year):
    cursor = conn.cursor()
    table_name = f"rankings_{year}"
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    table_exists = cursor.fetchone() is not None
    if not table_exists:
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                domain TEXT PRIMARY KEY
            )
        """)
        logging.info(f"创建新表: {table_name}")
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN '{date_str}' INTEGER")
            except Exception as e:
                logging.error(f"添加列 {date_str} 失败: {e}")
            current_date += timedelta(days=1)
        logging.info(f"为{year}年添加所有日期列")
    conn.commit()

# ========== 新增：首次运行和历史迁移特殊处理 ===========
def migrate_flat_to_year_tables(domains_rankings):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        for domain, date_ranks in domains_rankings.items():
            for date_str, rank in date_ranks.items():
                year = int(date_str[:4])
                table_name = f"rankings_{year}"
                create_year_table(conn, year)
                # 检查是否有该domain
                cursor.execute(f"SELECT domain FROM {table_name} WHERE domain=?", (domain,))
                if cursor.fetchone() is None:
                    cursor.execute(f"INSERT INTO {table_name} (domain) VALUES (?)", (domain,))
                # 更新排名
                try:
                    cursor.execute(f"UPDATE {table_name} SET '{date_str}'=? WHERE domain=?", (rank, domain))
                except Exception as e:
                    logging.error(f"更新{table_name}列{date_str}失败: {e}")
        conn.commit()
        conn.close()
        logging.info("历史数据迁移到年度分表完成")
    except Exception as e:
        logging.error(f"迁移历史数据失败: {e}")

# ========== 新增：结果文件有效性检查 ===========
def check_result_files():
    # 检查数据库和备份文件有效性
    if not os.path.exists(DB_FILE):
        logging.warning(f"数据库文件不存在: {DB_FILE}")
    if not os.path.exists(BACKUP_DIR):
        logging.warning(f"备份目录不存在: {BACKUP_DIR}")

# ========== 新增：数据库初始化和性能优化 ===========
def create_database():
    try:
        if not os.path.exists(os.path.dirname(DB_FILE)):
            os.makedirs(os.path.dirname(DB_FILE))
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        # 创建 domains 表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS domains (
                domain TEXT PRIMARY KEY,
                first_seen DATE
            )
        """)
        # 性能优化 PRAGMA
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA cache_size=10000")
        cursor.execute("PRAGMA temp_store=MEMORY")
        # 创建当前年份和未来一年表
        current_year = datetime.now().year
        create_year_table(conn, current_year)
        create_year_table(conn, current_year + 1)
        conn.commit()
        logging.info(f"数据库表已初始化: {DB_FILE}")
    except Exception as e:
        logging.error(f"数据库初始化失败: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

# ========== 新增：批量插入 domains 表 ===========
def batch_insert_domains(conn, domains_first_seen):
    try:
        cursor = conn.cursor()
        data = [(d, domains_first_seen[d]) for d in domains_first_seen]
        cursor.executemany("INSERT OR IGNORE INTO domains (domain, first_seen) VALUES (?, ?)", data)
        conn.commit()
        logging.info(f"批量插入 domains 表: {len(data)} 条")
    except Exception as e:
        logging.error(f"批量插入 domains 表失败: {e}")

# ========== 新增：首次运行判定 ===========
def is_first_run():
    return not os.path.exists(DB_FILE)

# ========== 修改主流程 ===========
def main():
    # 首次运行数据库初始化
    if is_first_run():
        create_database()
    # 历史迁移
    def load_domains_history_and_cleanup():
        """首次运行时从 parquet 文件加载历史数据并删除文件"""
        domains_rankings = {}
        domains_first_seen = {}
        parquet_rankings = os.path.join('data', 'domains_rankings.parquet')
        parquet_first_seen = os.path.join('data', 'domains_first_seen.parquet')
        # 加载 domains_first_seen
        if os.path.exists(parquet_first_seen):
            try:
                df = pd.read_parquet(parquet_first_seen)
                for _, row in df.iterrows():
                    domains_first_seen[row['domain']] = row['first_seen']
                logging.info(f"加载了 {len(domains_first_seen)} 个域名的首次出现日期")
            except Exception as e:
                logging.error(f"加载域名首次出现日期时出错: {e}")
        # 加载 domains_rankings
        if os.path.exists(parquet_rankings):
            try:
                df = pd.read_parquet(parquet_rankings)
                date_columns = [col for col in df.columns if col != 'domain']
                for _, row in df.iterrows():
                    domain = row['domain']
                    domains_rankings[domain] = {}
                    for date in date_columns:
                        if not pd.isna(row[date]) and row[date] != 0:
                            domains_rankings[domain][date] = int(row[date])
                logging.info(f"加载了 {len(domains_rankings)} 个域名的排名历史，跨越 {len(date_columns)} 个日期")
            except Exception as e:
                logging.error(f"加载域名排名历史时出错: {e}")
        # 删除 parquet 文件
        if os.path.exists(parquet_first_seen):
            # os.remove(parquet_first_seen)
            logging.info(f"已删除历史文件: {parquet_first_seen}")
        if os.path.exists(parquet_rankings):
            # os.remove(parquet_rankings)
            logging.info(f"已删除历史文件: {parquet_rankings}")
        return domains_rankings, domains_first_seen
    domains_rankings, domains_first_seen = load_domains_history_and_cleanup()
    if domains_rankings or domains_first_seen:
        save_domains_to_sqlite(domains_rankings, domains_first_seen)
        migrate_flat_to_year_tables(domains_rankings)
    # 加载处理历史
    process_history = load_process_history()
    # 加载年度分表数据
    conn = sqlite3.connect(DB_FILE)
    current_year = datetime.now().year
    create_year_table(conn, current_year)
    create_year_table(conn, current_year + 1)
    # 批量插入 domains 表
    if domains_first_seen:
        batch_insert_domains(conn, domains_first_seen)
    conn.close()
    # 下载和处理最新排名数据
    ZIP_FILE = "tranco.zip"
    zip_file_path = os.path.join("data", ZIP_FILE)
    if not os.path.exists("data"):
        os.makedirs("data")
    def is_valid_zip(zip_path):
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                bad_file = z.testzip()
                if bad_file is not None:
                    logging.error(f"Corrupted file in zip: {bad_file}")
                    return False
                return True
        except Exception as e:
            logging.error(f"Invalid zip file: {e}")
            return False
    if not os.path.exists(zip_file_path):
        logging.info(f"Downloading {ZIP_FILE}...")
        try:
            import requests
            response = requests.get("https://tranco-list.eu/top-1m.csv.zip", stream=True)
            with open(zip_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logging.info(f"{ZIP_FILE} downloaded successfully.")
            # Check zip validity after download
            if not is_valid_zip(zip_file_path):
                logging.warning(f"Downloaded zip file is invalid or corrupted: {zip_file_path}, retrying with wget...")
                try:
                    os.system(f"wget https://tranco-list.eu/top-1m.csv.zip -O {zip_file_path}")
                    logging.info(f"{ZIP_FILE} re-downloaded with wget.")
                except Exception as e:
                    logging.error(f"Error re-downloading file with wget: {e}")
                # Check again after wget
                if not is_valid_zip(zip_file_path):
                    logging.error(f"Zip file is still invalid after wget retry: {zip_file_path}")
                    return
        except Exception as e:
            logging.error(f"Error downloading file: {e}")
            return
    # Check zip validity before processing
    if not is_valid_zip(zip_file_path):
        logging.error(f"Downloaded zip file is invalid or corrupted: {zip_file_path}")
        return
    # 解压和处理
    new_domains_dir = os.path.join(os.getcwd(), "new_domains")
    if not os.path.exists(new_domains_dir):
        os.makedirs(new_domains_dir)
        logging.info(f"Created directory for new domains: {new_domains_dir}")
    try:
        csv_file_name = "top-1m.csv"
        with zipfile.ZipFile(zip_file_path, 'r') as z:
            with z.open(csv_file_name, 'r') as csvfile:
                reader = csv.reader(codecs.getreader("utf-8")(csvfile))
                data = list(reader)[1:]
        date_str = datetime.now().strftime('%Y-%m-%d')
        year = int(date_str[:4])
        conn = sqlite3.connect(DB_FILE)
        create_year_table(conn, year)
        cursor = conn.cursor()
        new_domains = []
        current_domains = set()
        batch = []
        for row in data:
            if len(row) == 2:
                try:
                    rank = int(row[0].strip())
                    domain = row[1].strip()
                    current_domains.add(domain)
                    # 检查是否有该domain
                    cursor.execute(f"SELECT domain FROM rankings_{year} WHERE domain=?", (domain,))
                    if cursor.fetchone() is None:
                        cursor.execute(f"INSERT INTO rankings_{year} (domain) VALUES (?)", (domain,))
                    # 更新排名
                    try:
                        cursor.execute(f"UPDATE rankings_{year} SET '{date_str}'=? WHERE domain=?", (rank, domain))
                    except Exception as e:
                        logging.error(f"更新{year}年{date_str}失败: {e}")
                    # 检查是否为新域名
                    if not os.path.exists('data/domains_first_seen.parquet') or domain not in domains_first_seen:
                        domains_first_seen[domain] = date_str
                        new_domains.append(domain)
                    # 批量插入 domains
                    batch.append((domain, date_str))
                    if len(batch) >= 1000:
                        cursor.executemany("INSERT OR IGNORE INTO domains (domain, first_seen) VALUES (?, ?)", batch)
                        batch.clear()
                except Exception as e:
                    logging.warning(f"Row parse error: {row}, {e}")
        if batch:
            cursor.executemany("INSERT OR IGNORE INTO domains (domain, first_seen) VALUES (?, ?)", batch)
        conn.commit()
        conn.close()
        # 输出新域名
        if new_domains:
            output_file = os.path.join(new_domains_dir, f"new_domains_{date_str}.txt")
            with open(output_file, 'w', encoding='utf-8') as f:
                for d in new_domains:
                    f.write(d + '\n')
            logging.info(f"新域名已输出到: {output_file}")
        # 备份 domains_rankings 到 CSV 分割文件
        try:
            if not os.path.exists(BACKUP_DIR):
                os.makedirs(BACKUP_DIR)
            # 读取所有排名数据
            cursor.execute(f"SELECT domain FROM rankings_{year}")
            all_domains = [row[0] for row in cursor.fetchall()]
            total = len(all_domains)
            for i in range(0, total, BACKUP_SPLIT_SIZE):
                chunk_domains = all_domains[i:i+BACKUP_SPLIT_SIZE]
                backup_file = os.path.join(BACKUP_DIR, f"domains_rankings_{year}_part_{i//BACKUP_SPLIT_SIZE+1}.csv")
                with open(backup_file, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['domain', date_str])
                    for domain in chunk_domains:
                        cursor.execute(f"SELECT '{date_str}' FROM rankings_{year} WHERE domain=?", (domain,))
                        rank = cursor.fetchone()
                        writer.writerow([domain, rank[0] if rank else ''])
                logging.info(f"备份分割文件已保存: {backup_file}")
        except Exception as e:
            logging.error(f"备份 domains_rankings 分割文件失败: {e}")
        # 更新首次出现日期文件
        # 已移除 parquet/csv 保存逻辑
        try:
            df_first_seen = pd.DataFrame([
                {'domain': domain, 'first_seen': first_seen}
                for domain, first_seen in domains_first_seen.items()
            ])
            df_first_seen.to_csv('data/domains_first_seen.csv', index=False)
            logging.info(f"首次出现日期已保存, 共{len(domains_first_seen)}条")
        except Exception as e:
            logging.error(f"保存首次出现日期失败: {e}")
        # 更新处理历史
        if date_str not in process_history['dates']:
            process_history['dates'].append(date_str)
            save_process_history(process_history)
    except FileNotFoundError:
        logging.error(f"Zip file not found: {zip_file_path}")
    except Exception as e:
        logging.error(f"处理zip文件失败: {e}")
    check_result_files()


def save_domains_to_sqlite(domains_rankings, domains_first_seen):
    """将 domains_rankings 和 domains_first_seen 写入 SQLite"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    # domains 表
    batch_insert_domains(conn, domains_first_seen)
    # rankings 年度分表
    for domain, date_ranks in domains_rankings.items():
        for date_str, rank in date_ranks.items():
            year = int(date_str[:4])
            table_name = f"rankings_{year}"
            create_year_table(conn, year)
            cursor.execute(f"SELECT domain FROM {table_name} WHERE domain=?", (domain,))
            if cursor.fetchone() is None:
                cursor.execute(f"INSERT INTO {table_name} (domain) VALUES (?)", (domain,))
            try:
                cursor.execute(f"UPDATE {table_name} SET '{date_str}'=? WHERE domain=?", (rank, domain))
            except Exception as e:
                logging.error(f"更新{table_name}列{date_str}失败: {e}")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()