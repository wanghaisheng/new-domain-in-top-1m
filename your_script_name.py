import csv
import logging
import os
from datetime import datetime, date, timedelta
import sqlite3
import zipfile
import codecs

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_FILE = 'domain_rank.db'  # 数据库文件名
ZIP_FILE = 'tranco.zip'
CSV_FILE_NAME = 'top-1m.csv'  # CSV 文件名

def create_table_for_year(conn, year):
    """为指定年份创建表."""
    cursor = conn.cursor()
    table_name = f"ranks_{year}"
    try:
        # 构建列名 (序号, 域名, 每一天的日期)
        columns = ["domain TEXT PRIMARY KEY"] + [f"'{date.strftime('%Y-%m-%d')}' INTEGER"
                                                  for date in (date(year, 1, 1) + timedelta(n)
                                                               for n in range(365 if year % 4 == 0 else 365))]

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {','.join(columns)}
            )
        """)
        conn.commit()
        logging.info(f"Table {table_name} created successfully.")
    except Exception as e:
        logging.error(f"Error creating table {table_name}: {e}")

def initialize_table_with_domains(conn, year):
    """使用去重后的域名数据初始化指定年份的表"""
    cursor = conn.cursor()
    table_name = f"ranks_{year}"

    try:
        # 从现有表格中提取不重复的域名
        cursor.execute("SELECT DISTINCT domain FROM ranks WHERE date < DATE('now', '-1 days')") #所有已经出现过的域名
        existing_domains = [row[0] for row in cursor.fetchall()]

        # 批量插入域名数据
        rows_to_insert = [(domain,) for domain in existing_domains]
        placeholders = ",".join("?" * len(rows_to_insert[0]))  # 生成占位符字符串
        cursor.executemany(f"INSERT OR IGNORE INTO {table_name} (domain) VALUES ({placeholders})", rows_to_insert)  # 插入数据

        conn.commit()
        logging.info(f"{len(existing_domains)} unique domains initialized to {table_name}.")
    except Exception as e:
        logging.error(f"Failed to initialize domains to table {table_name}: {e}")


def update_database(zip_file, year):
    """更新指定年份的数据库表."""
    conn = None
    try:
        with zipfile.ZipFile(zip_file, 'r') as z:
            with z.open(CSV_FILE_NAME, 'r') as csvfile:
                reader = csv.reader(codecs.getreader("utf-8")(csvfile))
                data = list(reader)

        date = datetime.now().strftime('%Y-%m-%d')  # Current date

    except FileNotFoundError:
        logging.error(f"Zip file not found: {zip_file}")
        return
    except Exception as e:
        logging.error(f"Error processing zip file: {e}")
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    table_name = f"ranks_{year}"

    try:
        # 假设数据每天更新，从文件名中获取日期
        current_date = datetime.now().date()
        date_str = current_date.strftime('%Y-%m-%d')
        logging.info(f"Updating database for date: {date_str}")

        # Prepare update statement
        update_statement = f"""
            UPDATE {table_name}
            SET '{date_str}' = (CASE domain """
        for row in data[1:]:
            if len(row) == 2:
                try:
                    rank = int(row[0].strip())
                    domain = row[1].strip()
                    update_statement += f" WHEN '{domain}' THEN {rank}"
                except (ValueError, IndexError) as e:
                    logging.warning(f"Could not process row {row}: {e}")
        update_statement += f""" ELSE NULL END)""" # 如果没有匹配的排名，则为NULL
        cursor.execute(update_statement)

        conn.commit()
        logging.info(f"Database table {table_name} updated for date {date_str}")

    except Exception as e:
        logging.error(f"Error updating table {table_name}: {e}")

    finally:
        if conn:
            conn.close()

def main():
    # 获取年份
    current_year = datetime.now().year

    # 确保 data 目录存在
    if not os.path.exists("data"):
        os.makedirs("data")

    # 下载 zip 文件
    zip_file_path = os.path.join("data", ZIP_FILE)

    if not os.path.exists(zip_file_path):
        logging.info(f"Downloading {ZIP_FILE}
