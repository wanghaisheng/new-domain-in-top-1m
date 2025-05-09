import os
import sys
import csv
import logging
import zipfile
import codecs
import json
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BACKUP_DIR = 'domains_rankings_backup'
BACKUP_SPLIT_SIZE = 1000000
PROCESS_HISTORY_FILE = os.path.join('data', 'process_history.json')

# ========== 处理历史记录 ==========
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

# ========== CSV 持久化相关 ==========
def load_domains_from_csv():
    domains_rankings = {}
    domains_first_seen = {}
    if not os.path.exists(BACKUP_DIR):
        logging.warning(f"备份目录不存在: {BACKUP_DIR}")
        return domains_rankings, domains_first_seen
    for fname in os.listdir(BACKUP_DIR):
        if fname.startswith('domains_rankings_') and fname.endswith('.csv'):
            path = os.path.join(BACKUP_DIR, fname)
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    date_col = header[1] if len(header) > 1 else None
                    for row in reader:
                        if len(row) < 2:
                            continue
                        domain, rank = row[0], row[1]
                        if domain not in domains_rankings:
                            domains_rankings[domain] = {}
                        if date_col:
                            try:
                                domains_rankings[domain][date_col] = int(rank)
                            except:
                                domains_rankings[domain][date_col] = 0
            except Exception as e:
                logging.error(f"读取备份文件 {fname} 失败: {e}")
    # 加载首次出现日期
    first_seen_file = os.path.join(BACKUP_DIR, 'domains_first_seen.csv')
    if os.path.exists(first_seen_file):
        try:
            with open(first_seen_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    if len(row) >= 2:
                        domains_first_seen[row[0]] = row[1]
        except Exception as e:
            logging.error(f"读取首次出现日期失败: {e}")
    return domains_rankings, domains_first_seen

def save_domains_to_csv(domains_rankings, domains_first_seen, date_str):
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
    # 保存 domains_first_seen
    first_seen_file = os.path.join(BACKUP_DIR, 'domains_first_seen.csv')
    try:
        with open(first_seen_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['domain', 'first_seen'])
            for domain, first_seen in domains_first_seen.items():
                writer.writerow([domain, first_seen])
        logging.info(f"首次出现日期已保存: {first_seen_file}")
    except Exception as e:
        logging.error(f"保存首次出现日期失败: {e}")
    # 保存 domains_rankings 按分割
    all_domains = list(domains_rankings.keys())
    total = len(all_domains)
    for i in range(0, total, BACKUP_SPLIT_SIZE):
        chunk_domains = all_domains[i:i+BACKUP_SPLIT_SIZE]
        backup_file = os.path.join(BACKUP_DIR, f"domains_rankings_{date_str}_part_{i//BACKUP_SPLIT_SIZE+1}.csv")
        try:
            with open(backup_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['domain', date_str])
                for domain in chunk_domains:
                    rank = domains_rankings[domain].get(date_str, '')
                    writer.writerow([domain, rank])
            logging.info(f"备份分割文件已保存: {backup_file}")
        except Exception as e:
            logging.error(f"保存分割文件失败: {e}")

# ========== 主流程 ==========
def main():
    # 加载历史数据
    domains_rankings, domains_first_seen = load_domains_from_csv()
    process_history = load_process_history()
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
        logging.error(f"Zip file not found: {zip_file_path}")
        return
    if not is_valid_zip(zip_file_path):
        logging.error(f"Zip file is invalid or corrupted: {zip_file_path}")
        return
    date_str = datetime.now().strftime('%Y-%m-%d')
    zip_file_newname = os.path.join("data", f"tranco_{date_str}.zip")
    if not os.path.exists(zip_file_newname):
        try:
            os.rename(zip_file_path, zip_file_newname)
            logging.info(f"tranco.zip 已重命名为: {zip_file_newname}")
            zip_file_path = zip_file_newname
        except Exception as e:
            logging.error(f"重命名tranco.zip失败: {e}")
    else:
        zip_file_path = zip_file_newname
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
        year = int(date_str[:4])
        new_domains = []
        current_domains = set()
        for row in data:
            if len(row) == 2:
                try:
                    rank = int(row[0].strip())
                    domain = row[1].strip()
                    current_domains.add(domain)
                    # 更新排名
                    if domain not in domains_rankings:
                        domains_rankings[domain] = {}
                    domains_rankings[domain][date_str] = rank
                    # 检查是否为新域名
                    if domain not in domains_first_seen:
                        domains_first_seen[domain] = date_str
                        new_domains.append(domain)
                except Exception as e:
                    logging.warning(f"Row parse error: {row}, {e}")
        # 输出新域名
        if new_domains:
            output_file = os.path.join(new_domains_dir, f"new_domains_{date_str}.txt")
            with open(output_file, 'w', encoding='utf-8') as f:
                for d in new_domains:
                    f.write(d + '\n')
            logging.info(f"新域名已输出到: {output_file}")
        # 保存到 CSV 备份
        save_domains_to_csv(domains_rankings, domains_first_seen, date_str)
    except Exception as e:
        logging.error(f"处理zip文件失败: {e}")
    # 更新处理历史
    process_history['dates'].append(date_str)
    save_process_history(process_history)

if __name__ == "__main__":
    main()