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
CSV_FILE_NAME = 'top-1m.csv' # CSV 文件名

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

        # 创建 ranks 表，用于存储域名在不同日期的排名
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ranks (
                domain TEXT,
                date DATE,
                rank INTEGER,
                FOREIGN KEY (domain) REFERENCES domains(domain),
                PRIMARY KEY (domain, date)
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
    conn = None  # Initialize conn to None
    try:
        with zipfile.ZipFile(zip_file, 'r') as z:
            with z.open(CSV_FILE_NAME, 'r') as csvfile: # Open the csv file from zip file
                # 读取 CSV 文件内容
                reader = csv.reader(codecs.getreader("utf-8")(csvfile)) #Handle UnicodeDecodeError with codecs
                data = list(reader) # read the data

        date = datetime.now().strftime('%Y-%m-%d') # Use the current date

    except FileNotFoundError:
        logging.error(f"Zip file not found: {zip_file}")
        return []
    except Exception as e:
        logging.error(f"Error processing zip file: {e}")
        return []

    conn = sqlite3.connect(DB_FILE) # move here so that it's only called after successfully reading zip file.
    cursor = conn.cursor()
    new_domains = []

    for row in data[1:]:  # skip the header
        if len(row) == 2:
            try:
                rank = int(row[0].strip())  # 排名
                domain = row[1].strip()

                # 检查域名是否已存在
                cursor.execute("SELECT first_seen FROM domains WHERE domain = ?", (domain,))
                result = cursor.fetchone()

                if result is None:
                    # 新域名，添加到 domains 表
                    cursor.execute("INSERT INTO domains (domain, first_seen) VALUES (?, ?)", (domain, date))
                    new_domains.append(domain)
                    logging.info(f"New domain {domain} added to domains table.")

                # 添加或更新排名信息到 ranks 表
                cursor.execute("INSERT OR REPLACE INTO ranks (domain, date, rank) VALUES (?, ?, ?)", (domain, date, rank))

            except (IndexError, ValueError) as e:
                logging.error(f"Error processing row in zip file : {e}")

    conn.commit()
    logging.info(f"Database updated successfully with data from {zip_file}")
    return new_domains


if __name__ == "__main__":
    # 示例用法：

    # 确保 data 目录存在
    if not os.path.exists("data"):
        os.makedirs("data")

    # 下载 zip 文件
    zip_file_path = os.path.join("data", ZIP_FILE)  # data 目录下的 tranco.zip

    if not os.path.exists(zip_file_path):
        logging.info(f"Downloading {ZIP_FILE}...")
        os.system(f"wget https://tranco-list.eu/top-1m.csv.zip -O {zip_file_path}")
        logging.info(f"{ZIP_FILE} downloaded successfully.")

    # 创建数据库
    create_database()

    # 更新数据库
    new_domains = update_database(zip_file_path)

    if new_domains:
        print(f"New domains added on {datetime.now().strftime('%Y-%m-%d')}: {new_domains}")
        # Write new domains to a file
        github_workspace = os.environ.get("GITHUB_WORKSPACE", ".")  # Get workspace, default to current dir
        new_domains_file = os.path.join(github_workspace, "new_domains.txt")

        with open(new_domains_file, "w") as f:
            for domain in new_domains:
                f.write(f"{domain}\n")
        logging.info(f"New domains written to {new_domains_file}")

    else:
        print("No new domains added.")

    logging.info("Database update complete.")
