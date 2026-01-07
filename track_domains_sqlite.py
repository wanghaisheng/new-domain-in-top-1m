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
BACKUP_SPLIT_SIZE = 200000
PROCESS_HISTORY_FILE = os.path.join('data', 'process_history.json')
new_domains_dir = os.path.join('./', "new_domains")

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
    # 只读取宽表格式的 domains_rankings_*.csv 文件
    for fname in os.listdir(BACKUP_DIR):
        if fname.startswith('domains_rankings_') and fname.endswith('.csv'):
            path = os.path.join(BACKUP_DIR, fname)
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    date_cols = header[1:]  # 第一列是 domain，后面是日期
                    for row in reader:
                        if len(row) < 2:
                            continue
                        domain = row[0]
                        if domain not in domains_rankings:
                            domains_rankings[domain] = {}
                        for idx, date_col in enumerate(date_cols):
                            if idx+1 < len(row):
                                try:
                                    rank = int(row[idx+1]) if row[idx+1] else None
                                except:
                                    rank = None
                                if rank is not None:
                                    domains_rankings[domain][date_col] = rank
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
    # 保存 domains_rankings 为宽表格式
    all_domains = list(domains_rankings.keys())
    # 收集所有日期
    all_dates = set()
    for v in domains_rankings.values():
        all_dates.update(v.keys())
    all_dates = sorted(all_dates)
    total = len(all_domains)
    for i in range(0, total, BACKUP_SPLIT_SIZE):
        chunk_domains = all_domains[i:i+BACKUP_SPLIT_SIZE]
        backup_file = os.path.join(BACKUP_DIR, f"domains_rankings_part_{i//BACKUP_SPLIT_SIZE+1}.csv")
        try:
            with open(backup_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['domain'] + all_dates)
                for domain in chunk_domains:
                    row = [domain]
                    for date in all_dates:
                        rank = domains_rankings[domain].get(date, '')
                        row.append(rank)
                    writer.writerow(row)
            logging.info(f"宽表备份分割文件已保存: {backup_file}")
        except Exception as e:
            logging.error(f"保存宽表分割文件失败: {e}")

# ========== Parquet 迁移相关 ==========
def migrate_parquet_to_csv():
    parquet_file = 'domains_rankings.parquet'
    if not os.path.exists(parquet_file):
        return
    try:
        df = pd.read_parquet(parquet_file)
        # 兼容不同格式，假设有列：domain, date, rank
        if not {'domain', 'date', 'rank'}.issubset(df.columns):
            logging.error(f"parquet文件缺少必要列: {df.columns}")
            return
        domains_rankings = {}
        domains_first_seen = {}
        for _, row in df.iterrows():
            domain = row['domain']
            date = row['date']
            rank = row['rank']
            if domain not in domains_rankings:
                domains_rankings[domain] = {}
            domains_rankings[domain][date] = rank
            if domain not in domains_first_seen or date < domains_first_seen[domain]:
                domains_first_seen[domain] = date
        # 获取所有日期，按日期分割备份
        all_dates = sorted({d for v in domains_rankings.values() for d in v.keys()})
        for date_str in all_dates:
            save_domains_to_csv(domains_rankings, domains_first_seen, date_str)
        os.remove(parquet_file)
        logging.info(f"parquet历史数据已迁移并删除: {parquet_file}")
    except Exception as e:
        logging.error(f"parquet迁移失败: {e}")

# ========== Parquet 迁移相关 ==========
def migrate_first_seen_parquet_to_csv():
    parquet_file = 'domains_first_seen.parquet'
    first_seen_file = os.path.join(BACKUP_DIR, 'domains_first_seen.csv')
    if not os.path.exists(parquet_file):
        return
    try:
        df = pd.read_parquet(parquet_file)
        if not {'domain', 'first_seen'}.issubset(df.columns):
            logging.error(f"parquet文件缺少必要列: {df.columns}")
            return
        # 读取parquet并合并到csv
        first_seen_dict = {}
        for _, row in df.iterrows():
            domain = row['domain']
            first_seen = row['first_seen']
            first_seen_dict[domain] = first_seen
        # 如果csv已存在，合并历史
        if os.path.exists(first_seen_file):
            try:
                with open(first_seen_file, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    next(reader)
                    for row in reader:
                        if len(row) >= 2:
                            d, fs = row[0], row[1]
                            if d not in first_seen_dict or fs < first_seen_dict[d]:
                                first_seen_dict[d] = fs
            except Exception as e:
                logging.error(f"读取csv历史失败: {e}")
        # 保存合并后的csv
        try:
            with open(first_seen_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['domain', 'first_seen'])
                for domain, first_seen in first_seen_dict.items():
                    writer.writerow([domain, first_seen])
            logging.info(f"首次出现日期parquet已迁移并合并到csv: {first_seen_file}")
        except Exception as e:
            logging.error(f"保存合并csv失败: {e}")
        os.remove(parquet_file)
        logging.info(f"parquet历史数据已迁移并删除: {parquet_file}")
    except Exception as e:
        logging.error(f"parquet迁移失败: {e}")

# ========== 主流程 ==========
def main():
    migrate_parquet_to_csv()
    migrate_first_seen_parquet_to_csv()
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
    new_domains = []

    if not os.path.exists(new_domains_dir):
        os.makedirs(new_domains_dir)
        logging.info(f"Created directory for new domains: {new_domains_dir}")
    else:
        logging.info(f"Directory for new domains already exists: {new_domains_dir}")
        newdomainpath= os.path.join(new_domains_dir, f"{date_str}.txt")
        if  os.path.exists(newdomainpath):
            
            new_domains =  open(newdomainpath    , 'r').read().splitlines()

    try:
        csv_file_name = "top-1m.csv"
        with zipfile.ZipFile(zip_file_path, 'r') as z:
            with z.open(csv_file_name, 'r') as csvfile:
                reader = csv.reader(codecs.getreader("utf-8")(csvfile))
                data = list(reader)[1:]
        year = int(date_str[:4])
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
                    # 检查首次出现
                    if domain not in domains_first_seen:
                        domains_first_seen[domain] = date_str
                        new_domains.append(domain)
                except Exception as e:
                    logging.warning(f"Row parse error: {row}, {e}")
        if os.path.exists(zip_file_newname):
            os.remove(zip_file_newname)
            logging.info(f"Zip file deleted: {zip_file_newname}")
        # 生成真正新增域名
        if new_domains:
            output_file = os.path.join(new_domains_dir, f"{date_str}.txt")
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
