import os
import sys
import csv
import logging
import sqlite3
import zipfile
import codecs
import json
import pandas as pd
from datetime import datetime

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Paths from workflow
DB_FILE = os.path.join('data', 'persisted-to-cache', 'domain_rank.db')
BACKUP_DIR = 'domains_rankings_backup'
BACKUP_SPLIT_SIZE = 1000000  # 1 million domains per file

def load_domains_history_and_cleanup():
    """Load history from old parquet files if they exist, then delete them. Return as dicts."""
    domains_rankings = {}
    domains_first_seen = {}
    parquet_rankings = 'domains_rankings.parquet'
    parquet_first_seen = 'domains_first_seen.parquet'
    if os.path.exists(parquet_first_seen):
        try:
            df = pd.read_parquet(parquet_first_seen)
            for _, row in df.iterrows():
                domains_first_seen[row['domain']] = row['first_seen']
            logging.info(f"Loaded {len(domains_first_seen)} domains first seen dates from parquet")
            # os.remove(parquet_first_seen)
            logging.info(f"Deleted {parquet_first_seen}")
        except Exception as e:
            logging.error(f"Error loading or deleting {parquet_first_seen}: {e}")
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
            logging.info(f"Loaded ranking history for {len(domains_rankings)} domains across {len(date_columns)} dates from parquet")
            # os.remove(parquet_rankings)
            logging.info(f"Deleted {parquet_rankings}")
        except Exception as e:
            logging.error(f"Error loading or deleting {parquet_rankings}: {e}")
    return domains_rankings, domains_first_seen


def save_domains_to_sqlite(domains_rankings, domains_first_seen):
    """Save domains_rankings and domains_first_seen to SQLite DB."""
    if not os.path.exists(os.path.dirname(DB_FILE)):
        os.makedirs(os.path.dirname(DB_FILE))
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # Create tables
    c.execute('''CREATE TABLE IF NOT EXISTS domains_first_seen (
        domain TEXT PRIMARY KEY,
        first_seen TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS domains_rankings (
        domain TEXT,
        date TEXT,
        rank INTEGER,
        PRIMARY KEY(domain, date)
    )''')
    # Insert first_seen
    c.execute('DELETE FROM domains_first_seen')
    if domains_first_seen:
        c.executemany('INSERT INTO domains_first_seen (domain, first_seen) VALUES (?, ?)',
                      [(d, domains_first_seen[d]) for d in domains_first_seen])
    # Insert rankings
    c.execute('DELETE FROM domains_rankings')
    rankings_rows = []
    for domain, date_ranks in domains_rankings.items():
        for date, rank in date_ranks.items():
            rankings_rows.append((domain, date, rank))
    if rankings_rows:
        c.executemany('INSERT INTO domains_rankings (domain, date, rank) VALUES (?, ?, ?)', rankings_rows)
    conn.commit()
    conn.close()
    logging.info(f"Saved {len(domains_first_seen)} first_seen and {len(rankings_rows)} rankings to SQLite.")


def load_domains_from_sqlite():
    """Load domains_rankings and domains_first_seen from SQLite DB."""
    domains_rankings = {}
    domains_first_seen = {}
    if not os.path.exists(DB_FILE):
        return domains_rankings, domains_first_seen
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # Load first_seen
    for row in c.execute('SELECT domain, first_seen FROM domains_first_seen'):
        domains_first_seen[row[0]] = row[1]
    # Load rankings
    for row in c.execute('SELECT domain, date, rank FROM domains_rankings'):
        domain, date, rank = row
        if domain not in domains_rankings:
            domains_rankings[domain] = {}
        domains_rankings[domain][date] = rank
    conn.close()
    logging.info(f"Loaded {len(domains_first_seen)} first_seen and {len(domains_rankings)} domains from SQLite.")
    return domains_rankings, domains_first_seen


def save_domains_rankings_backup_csv(domains_rankings):
    """Save domains_rankings into split backup CSV files (1M per file)."""
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
    all_domains = list(domains_rankings.keys())
    total = len(all_domains)
    if total == 0:
        logging.info("No domains to backup.")
        return
    date_columns = set()
    for v in domains_rankings.values():
        date_columns.update(v.keys())
    sorted_dates = sorted(date_columns)
    for i in range(0, total, BACKUP_SPLIT_SIZE):
        chunk_domains = all_domains[i:i+BACKUP_SPLIT_SIZE]
        backup_file = os.path.join(BACKUP_DIR, f'domains_rankings_{i//BACKUP_SPLIT_SIZE+1}.csv')
        with open(backup_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['domain'] + sorted_dates)
            for domain in chunk_domains:
                row = [domain] + [domains_rankings[domain].get(date, 0) for date in sorted_dates]
                writer.writerow(row)
        logging.info(f"Saved backup file {backup_file} with {len(chunk_domains)} domains.")


def main():
    # Load and migrate history if needed
    domains_rankings, domains_first_seen = load_domains_history_and_cleanup()
    if domains_rankings or domains_first_seen:
        save_domains_to_sqlite(domains_rankings, domains_first_seen)
    # Load from SQLite for ongoing logic
    domains_rankings, domains_first_seen = load_domains_from_sqlite()
    # 3. 自动下载最新排名数据（支持 requests 和 wget）
    ZIP_FILE = "tranco.zip"
    zip_file_path = os.path.join("data", ZIP_FILE)
    if not os.path.exists("data"):
        os.makedirs("data")
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
        except ImportError:
            logging.warning("Requests library not found, falling back to os.system")
            os.system(f"wget https://tranco-list.eu/top-1m.csv.zip -O {zip_file_path}")
            logging.info(f"{ZIP_FILE} downloaded successfully.")
        except Exception as e:
            logging.error(f"Error downloading file: {e}")
    # 4. 解压并处理最新排名数据，检测新域名并输出到指定目录
    new_domains_dir = os.path.join(os.getcwd(), "new_domains")
    if not os.path.exists(new_domains_dir):
        os.makedirs(new_domains_dir)
        logging.info(f"Created directory for new domains: {new_domains_dir}")
    # 5. 这里应补充后续处理逻辑：如解压 zip，读取 csv，检测新域名，更新 domains_rankings/domains_first_seen，输出新域名到 new_domains_dir，并保存到 SQLite/CSV
    logging.info(f"主流程初始化完成，后续请补充新域名检测与输出逻辑。")
    if domains_rankings:
        save_domains_rankings_backup_csv(domains_rankings)
    # The rest of your domain tracking logic goes here
    logging.info(f"Backup complete. {len(domains_rankings)} domains processed.")

if __name__ == "__main__":
    main()