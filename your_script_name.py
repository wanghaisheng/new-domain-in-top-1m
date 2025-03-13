import csv
import logging
import os
from datetime import datetime
import sqlite3
import zipfile
import codecs

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_FILE = 'domain_rank.db'  # 数据库文件名
ZIP_FILE = 'tranco.zip'
CSV_FILE_NAME = 'top-1m.csv'  # CSV 文件名

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

        conn.commit()
        logging.info(f"Database tables created successfully in {DB_FILE}")
    except Exception as e:
        logging.error(f"Error creating database tables: {e}")
    finally:
        if conn:
            conn.close()

def update_database(zip_file):
    """更新数据库，并返回新出现的域名列表."""
    conn = None
    new_domains = [] # Initialize new_domains list
    try:
        with zipfile.ZipFile(zip_file, 'r') as z:
            with z.open(CSV_FILE_NAME, 'r') as csvfile:
                reader = csv.reader(codecs.getreader("utf-8")(csvfile))
                data = list(reader)

        date = datetime.now().strftime('%Y-%m-%d')
        current_year = datetime.now().year #get current year
        table_name = f"rankings_{current_year}"

    except FileNotFoundError:
        logging.error(f"Zip file not found: {zip_file}")
        return [] # Return empty list in case of FileNotFoundError
    except Exception as e:
        logging.error(f"Error processing zip file: {e}")
        return [] # Return empty list in case of other errors

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    try:
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
            conn.commit()
            logging.info(f"Created new table: {table_name}")

        #Check if the column exist
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()] # fetch column names
        if date not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN '{date}' INTEGER")
            conn.commit()
            logging.info(f"Added new column {date} to table {table_name}")


        for row in data[1:]:
            if len(row) == 2:
                try:
                    rank = int(row[0].strip())
                    domain = row[1].strip()

                    # Add the domain to the domains table if it doesn't exist
                    cursor.execute("INSERT OR IGNORE INTO domains (domain, first_seen) VALUES (?, ?)", (domain, date))
                    #If added successfully, then save to new_domains
                    if cursor.rowcount > 0: #Check rowcount after INSERT
                        new_domains.append(domain)

                    # Update the rank in the rankings table
                    cursor.execute(f"""
                        INSERT INTO {table_name} (domain, '{date}')
                        VALUES (?, ?)
                        ON CONFLICT(domain) DO UPDATE SET '{date}' = ?
                    """, (domain, rank, rank))  # SQLite 3.9+
                    conn.commit()

                except (ValueError, IndexError) as e:
                    logging.warning(f"Could not process row {row}: {e}")
    except Exception as e:
        logging.error(f"Error updating database: {e}")

    finally:
        if conn:
            conn.close()
    return new_domains


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
        os.system(f"wget https://tranco-list.eu/top-1m.csv.zip -O {zip_file_path}")
        logging.info(f"{ZIP_FILE} downloaded successfully.")

    # 创建数据库
    create_database()

    # 更新数据库
    new_domains = update_database(zip_file_path)
    if new_domains:
        print(f"New domains added: {new_domains}")
        new_domains_file = os.path.join(github_workspace, "new_domains.txt")
        with open(new_domains_file, "w") as f:
            for domain in new_domains:
                f.write(f"{domain}\n")
        logging.info(f"New domains written to {new_domains_file}")
    else:
        print("No new domains added today.")
