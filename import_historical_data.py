import os
import csv
import logging
import sqlite3
import zipfile
import codecs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 数据库和文件路径
DB_FILE = 'domain_rank.db'
DOMAINS_RANKINGS_FILE = 'domains_rankings.parquet'
DOMAINS_FIRST_SEEN_FILE = 'domains_first_seen.parquet'
HISTORICAL_DATA_DIR = 'historical_extracts'

def load_domains_history():
    """从Parquet文件加载历史域名数据和排名"""
    domains_rankings = {}  # 域名排名历史
    domains_first_seen = {}  # 域名首次出现日期
    
    # 加载域名首次出现日期
    if os.path.exists(DOMAINS_FIRST_SEEN_FILE):
        try:
            df = pd.read_parquet(DOMAINS_FIRST_SEEN_FILE)
            for _, row in df.iterrows():
                domains_first_seen[row['domain']] = row['first_seen']
            logging.info(f"Loaded {len(domains_first_seen)} domains first seen dates")
        except Exception as e:
            logging.error(f"Error loading domains first seen dates: {e}")
    
    # 加载域名排名历史
    if os.path.exists(DOMAINS_RANKINGS_FILE):
        try:
            df = pd.read_parquet(DOMAINS_RANKINGS_FILE)
            
            # 获取所有日期列
            date_columns = [col for col in df.columns if col != 'domain']
            
            # 将DataFrame转换为字典格式
            for _, row in df.iterrows():
                domain = row['domain']
                domains_rankings[domain] = {}
                
                for date in date_columns:
                    if not pd.isna(row[date]) and row[date] != 0:
                        domains_rankings[domain][date] = int(row[date])
            
            logging.info(f"Loaded ranking history for {len(domains_rankings)} domains across {len(date_columns)} dates")
        except Exception as e:
            logging.error(f"Error loading domains ranking history: {e}")
    
    return domains_rankings, domains_first_seen

def save_domains_history(domains_rankings, domains_first_seen):
    """将域名排名数据保存到Parquet文件"""
    # 保存域名首次出现日期
    try:
        df_first_seen = pd.DataFrame([
            {'domain': domain, 'first_seen': first_seen}
            for domain, first_seen in domains_first_seen.items()
        ])
        df_first_seen.to_parquet(DOMAINS_FIRST_SEEN_FILE, index=False)
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
        
        # 创建数据字典
        data = {'domain': []}
        for date in sorted_dates:
            data[date] = []
        
        # 填充数据
        for domain, rankings in domains_rankings.items():
            data['domain'].append(domain)
            for date in sorted_dates:
                data[date].append(rankings.get(date, 0))
        
        # 创建DataFrame并保存为Parquet
        df_rankings = pd.DataFrame(data)
        df_rankings.to_parquet(DOMAINS_RANKINGS_FILE, index=False)
        
        logging.info(f"Saved ranking history for {len(domains_rankings)} domains across {len(sorted_dates)} dates")
    except Exception as e:
        logging.error(f"Error saving domains ranking history: {e}")

def determine_chunk_parameters(date_dirs, requested_start_chunk=0, requested_end_chunk=0, chunk_size=10):
    """
    Automatically determine the optimal chunk start and end based on available data
    
    Args:
        date_dirs: List of date directories to process
        requested_start_chunk: User requested start chunk (default 0)
        requested_end_chunk: User requested end chunk (default 0)
        chunk_size: Number of dates to process in each chunk (default 10)
        
    Returns:
        tuple: (start_chunk, end_chunk, total_chunks)
    """
    total_dirs = len(date_dirs)
    total_chunks = (total_dirs + chunk_size - 1) // chunk_size  # Ceiling division
    
    logging.info(f"Found {total_dirs} date directories, which can be processed in {total_chunks} chunks")
    
    # Validate requested chunks
    if requested_start_chunk < 0:
        requested_start_chunk = 0
    
    if requested_end_chunk <= 0 or requested_end_chunk >= total_chunks:
        requested_end_chunk = total_chunks - 1
    
    if requested_start_chunk > requested_end_chunk:
        requested_start_chunk = requested_end_chunk
    
    logging.info(f"Will process chunks {requested_start_chunk} to {requested_end_chunk} (out of {total_chunks} total chunks)")
    
    return requested_start_chunk, requested_end_chunk, total_chunks

def import_historical_data(start_chunk=0, end_chunk=0, chunk_size=10):
    """导入历史数据"""
    # 加载现有数据
    domains_rankings, domains_first_seen = load_domains_history()
    
    # 检查历史数据目录是否存在
    if not os.path.exists(HISTORICAL_DATA_DIR):
        logging.error(f"Historical data directory not found: {HISTORICAL_DATA_DIR}")
        return
    
    # 获取所有日期目录
    date_dirs = [d for d in os.listdir(HISTORICAL_DATA_DIR) if os.path.isdir(os.path.join(HISTORICAL_DATA_DIR, d))]
    logging.info(f"Found {len(date_dirs)} date directories")
    
    # 按日期排序
    date_dirs.sort()
    
    # 检查数据量是否足够
    if len(date_dirs) < 200:
        logging.warning(f"只找到 {len(date_dirs)} 个日期目录，预期应有200+个日期目录")
        logging.warning("可能原因：1. Git仓库克隆深度不够 2. 提交记录过滤条件有问题 3. 部分日期没有提交记录")
        logging.warning("建议在workflow中增加git clone深度，并检查git log过滤条件")
    
    # 自动确定块参数
    start_chunk, end_chunk, total_chunks = determine_chunk_parameters(
        date_dirs, 
        requested_start_chunk=start_chunk,
        requested_end_chunk=end_chunk,
        chunk_size=chunk_size
    )
    
    # 计算要处理的日期目录范围
    start_idx = start_chunk * chunk_size
    end_idx = min((end_chunk + 1) * chunk_size, len(date_dirs))
    
    # 只处理指定范围内的日期目录
    dirs_to_process = date_dirs[start_idx:end_idx]
    logging.info(f"Processing {len(dirs_to_process)} directories from chunk {start_chunk} to {end_chunk}")
    
    # 逐个处理每个日期目录，每处理一个就更新一次数据库
    for i, date_dir in enumerate(dirs_to_process):
        date = date_dir  # 目录名就是日期
        zip_file = os.path.join(HISTORICAL_DATA_DIR, date_dir, "tranco.zip")
        csv_file = os.path.join(HISTORICAL_DATA_DIR, date_dir, "top-1m.csv")
        
        logging.info(f"Processing directory {i+1}/{len(dirs_to_process)}: {date_dir}")
        
        # 检查是否存在zip文件或csv文件
        if os.path.exists(zip_file):
            # 从zip文件读取数据
            try:
                with zipfile.ZipFile(zip_file, 'r') as z:
                    with z.open("top-1m.csv", 'r') as csvfile:
                        reader = csv.reader(codecs.getreader("utf-8")(csvfile))
                        process_csv_data(reader, date, domains_rankings, domains_first_seen)
                logging.info(f"Processed data from zip file for date: {date}")
            except Exception as e:
                logging.error(f"Error processing zip file for date {date}: {e}")
        elif os.path.exists(csv_file):
            # 直接从csv文件读取数据
            try:
                with open(csv_file, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    process_csv_data(reader, date, domains_rankings, domains_first_seen)
                logging.info(f"Processed data from CSV file for date: {date}")
            except Exception as e:
                logging.error(f"Error processing CSV file for date {date}: {e}")
        else:
            logging.warning(f"No data file found for date: {date}")
        
        # 每处理10个日期，保存一次数据并更新数据库
        if (i + 1) % 10 == 0 or i == len(dirs_to_process) - 1:
            # 保存更新后的数据
            save_domains_history(domains_rankings, domains_first_seen)
            
            # 更新数据库
            update_database(domains_rankings, domains_first_seen)
            
            logging.info(f"Saved data and updated database after processing {date_dir} ({i+1}/{len(dirs_to_process)})")
    
    logging.info(f"Historical data import completed for chunks {start_chunk} to {end_chunk}")
    return True

def process_single_commit(commit_hash, commit_date):
    """处理单个提交"""
    start_time = datetime.now()
    logging.info(f"开始处理提交 {commit_hash} ({commit_date})")
    
    # 检查是否已经处理过该提交
    last_processed_file = "last_processed_date.txt"
    if os.path.exists(last_processed_file):
        with open(last_processed_file, 'r') as f:
            last_date = f.read().strip()
            if last_date >= commit_date:
                logging.info(f"提交 {commit_hash} ({commit_date}) 已处理过，跳过")
                return True
    
    # 加载现有数据
    logging.info(f"正在加载现有数据...")
    domains_rankings, domains_first_seen = load_domains_history()
    logging.info(f"加载完成，共 {len(domains_rankings)} 个域名排名记录和 {len(domains_first_seen)} 个首次出现记录")
    
    # 检查提交目录是否存在
    commit_dir = os.path.join(HISTORICAL_DATA_DIR, commit_date)
    if not os.path.exists(commit_dir):
        logging.error(f"Commit directory not found: {commit_dir}")
        return False
    
    # 检查zip文件或csv文件
    zip_file = os.path.join(commit_dir, "tranco.zip")
    csv_file = os.path.join(commit_dir, "top-1m.csv")
    
    processed = False
    try:
        if os.path.exists(zip_file):
            # 从zip文件读取数据
            logging.info(f"正在从zip文件读取数据: {zip_file}")
            with zipfile.ZipFile(zip_file, 'r') as z:
                with z.open("top-1m.csv", 'r') as csvfile:
                    reader = csv.reader(codecs.getreader("utf-8")(csvfile))
                    process_csv_data(reader, commit_date, domains_rankings, domains_first_seen)
            logging.info(f"Processed data from zip file for date: {commit_date}")
            processed = True
        elif os.path.exists(csv_file):
            # 直接从csv文件读取数据
            logging.info(f"正在从CSV文件读取数据: {csv_file}")
            with open(csv_file, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                process_csv_data(reader, commit_date, domains_rankings, domains_first_seen)
            logging.info(f"Processed data from CSV file for date: {commit_date}")
            processed = True
        else:
            logging.warning(f"No data file found for date: {commit_date}")
        
        if processed:
            # 保存更新后的数据
            logging.info(f"正在保存域名排名和首次出现数据...")
            save_start_time = datetime.now()
            save_domains_history(domains_rankings, domains_first_seen)
            logging.info(f"数据保存完成，耗时: {(datetime.now() - save_start_time).total_seconds():.2f}秒")
            
            # 更新数据库
            logging.info(f"正在更新数据库...")
            db_start_time = datetime.now()
            update_database(domains_rankings, domains_first_seen)
            logging.info(f"数据库更新完成，耗时: {(datetime.now() - db_start_time).total_seconds():.2f}秒")
            
            total_time = (datetime.now() - start_time).total_seconds()
            logging.info(f"提交 {commit_hash} ({commit_date}) 处理完成，总耗时: {total_time:.2f}秒")
            
            # 返回处理成功标志
            return True
    except Exception as e:
        logging.error(f"处理提交 {commit_hash} ({commit_date}) 时发生错误: {e}")
        import traceback
        logging.error(traceback.format_exc())
    
    logging.warning(f"提交 {commit_hash} ({commit_date}) 处理失败")
    return False
def process_csv_data(reader, date, domains_rankings, domains_first_seen):
    """处理CSV数据"""
    data = list(reader)
    if not data:
        logging.warning(f"Empty data for date: {date}")
        return
    
    # 跳过标题行（如果有）
    if data[0][0].isalpha():
        data = data[1:]
    
    # 当前文件中的所有域名
    current_domains = set()
    
    # 处理每一行
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
                
                # 检查是否为新域名或者发现更早的出现日期
                if domain not in domains_first_seen or date < domains_first_seen[domain]:
                    domains_first_seen[domain] = date
                    logging.debug(f"更新域名 {domain} 的首次出现日期为 {date}")
                
            except (ValueError, IndexError) as e:
                logging.warning(f"Could not process row {row} for date {date}: {e}")
    
    # 对于历史中存在但当前文件中不存在的域名，将其排名设为0
    for domain in domains_rankings:
        if domain not in current_domains and date not in domains_rankings[domain]:
            domains_rankings[domain][date] = 0
    
    logging.info(f"Processed {len(current_domains)} domains for date: {date}")

def update_database(domains_rankings, domains_first_seen):
    """更新数据库"""
    conn = None
    try:
        logging.info(f"开始连接数据库...")
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # 性能优化设置
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA cache_size=10000")
        cursor.execute("PRAGMA temp_store=MEMORY")
        
        # 开始事务 - 大幅提高批量插入性能
        conn.execute("BEGIN TRANSACTION")
        logging.info(f"数据库连接和优化设置完成")
        
        # 确保domains表存在
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS domains (
                domain TEXT PRIMARY KEY,
                first_seen DATE
            )
        """)
        
        # 获取数据库中现有的域名首次出现日期
        logging.info(f"正在获取数据库中现有的域名首次出现日期...")
        existing_domains = {}
        cursor.execute("SELECT domain, first_seen FROM domains")
        for domain, first_seen in cursor.fetchall():
            existing_domains[domain] = first_seen
        logging.info(f"获取到 {len(existing_domains)} 个现有域名记录")
        
        # 检查并更新首次出现日期
        logging.info(f"正在准备域名首次出现日期更新...")
        domains_to_update = []
        for domain, first_seen in domains_first_seen.items():
            if domain in existing_domains:
                # 如果数据库中已有该域名，检查是否需要更新首次出现日期
                if first_seen < existing_domains[domain]:
                    domains_to_update.append((first_seen, domain))
            else:
                # 如果数据库中没有该域名，直接插入
                domains_to_update.append((first_seen, domain))
        
        # 批量更新域名首次出现日期
        if domains_to_update:
            logging.info(f"需要更新 {len(domains_to_update)} 个域名的首次出现日期")
            # 分批处理，每批10000条记录
            batch_size = 10000
            for i in range(0, len(domains_to_update), batch_size):
                batch = domains_to_update[i:i+batch_size]
                cursor.executemany(
                    "INSERT OR REPLACE INTO domains (first_seen, domain) VALUES (?, ?)", 
                    batch
                )
                logging.info(f"更新了域名首次出现日期批次 {i//batch_size + 1}/{(len(domains_to_update)-1)//batch_size + 1}")
            logging.info(f"总共更新了 {len(domains_to_update)} 个域名的首次出现日期")
        
        # 批量导入域名首次出现日期（对于未更新的域名）
        domains_to_import = [(domain, first_seen) for domain, first_seen in domains_first_seen.items() 
                            if domain not in [d[1] for d in domains_to_update]]
        
        if domains_to_import:
            # 分批处理，每批10000条记录
            batch_size = 10000
            for i in range(0, len(domains_to_import), batch_size):
                batch = domains_to_import[i:i+batch_size]
                cursor.executemany(
                    "INSERT OR REPLACE INTO domains (domain, first_seen) VALUES (?, ?)", 
                    batch
                )
                logging.info(f"导入域名首次出现日期批次 {i//batch_size + 1}/{(len(domains_to_import)-1)//batch_size + 1}")
            logging.info(f"总共导入了 {len(domains_to_import)} 个域名的首次出现日期")
        
        # 提交当前事务并开始新事务
        conn.commit()
        conn.execute("BEGIN TRANSACTION")
        logging.info("域名首次出现日期更新完成，开始更新排名数据")
        
        # 获取所有年份 (从2024年6月7日开始)
        years = set()
        for date_str in set().union(*[d.keys() for d in domains_rankings.values() if d]):
            try:
                year = int(date_str.split('-')[0])
                # 只处理2024年及以后的数据
                if year >= 2024:
                    years.add(year)
            except (ValueError, IndexError):
                logging.warning(f"Invalid date format: {date_str}")
        
        # 为每个年份创建表并导入数据
        for year in years:
            table_name = f"rankings_{year}"
            logging.info(f"开始处理年份 {year} 的排名数据")
            
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
            
            # 获取该年份的所有日期
            year_dates = [d for d in set().union(*[d.keys() for d in domains_rankings.values() if d]) 
                         if d.startswith(f"{year}-")]
            
            # 过滤掉2024年6月7日之前的日期
            if year == 2024:
                year_dates = [d for d in year_dates if d >= "2024-06-07"]
            
            logging.info(f"年份 {year} 有 {len(year_dates)} 个日期需要处理")
            
            # 确保所有日期列存在
            for date in sorted(year_dates):
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [col[1] for col in cursor.fetchall()]
                
                if date not in columns:
                    try:
                        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN '{date}' INTEGER")
                    except Exception as e:
                        logging.error(f"Error adding column {date}: {e}")
            
            # 提交当前事务并开始新事务
            conn.commit()
            conn.execute("BEGIN TRANSACTION")
            logging.info(f"表 {table_name} 结构更新完成，开始更新排名数据")
            
            # 批量更新排名数据，分批处理域名
            domains_list = list(domains_rankings.keys())
            batch_size = 5000
            total_batches = (len(domains_list) - 1) // batch_size + 1
            
            logging.info(f"需要处理 {len(domains_list)} 个域名的排名数据，分 {total_batches} 批处理")
            
            for batch_idx in range(0, len(domains_list), batch_size):
                batch_start_time = datetime.now()
                batch_domains = domains_list[batch_idx:batch_idx+batch_size]
                batch_num = batch_idx // batch_size + 1
                
                # 使用批量操作替代逐个域名更新
                domains_to_insert = []
                domains_to_update = []
                
                for domain in batch_domains:
                    year_rankings = {date: rank for date, rank in domains_rankings[domain].items() 
                                   if date.startswith(f"{year}-") and (year > 2024 or date >= "2024-06-07")}
                    
                    if not year_rankings:
                        continue
                    
                    # 检查域名是否已存在
                    cursor.execute(f"SELECT domain FROM {table_name} WHERE domain = ?", (domain,))
                    domain_exists = cursor.fetchone() is not None
                    
                    if not domain_exists:
                        # 准备插入域名
                        insert_values = [domain]
                        for date in sorted(year_dates):
                            insert_values.append(year_rankings.get(date, 0))
                        domains_to_insert.append(tuple(insert_values))
                    else:
                        # 准备更新域名
                        update_cols = []
                        update_vals = []
                        
                        for date in sorted(year_dates):
                            if date in year_rankings:
                                update_cols.append(f"'{date}' = ?")
                                update_vals.append(year_rankings[date])
                        
                        if update_cols:
                            # 添加WHERE条件的参数
                            update_vals.append(domain)
                            domains_to_update.append(tuple(update_vals))
                
                # 批量插入新域名
                if domains_to_insert:
                    columns = ["domain"] + [f"'{date}'" for date in sorted(year_dates)]
                    placeholders = ", ".join(["?"] * len(columns))
                    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                    
                    try:
                        cursor.executemany(insert_sql, domains_to_insert)
                        logging.info(f"批量插入了 {len(domains_to_insert)} 个新域名")
                    except Exception as e:
                        logging.error(f"批量插入域名时出错: {e}")
                
                # 批量更新现有域名
                if domains_to_update:
                    for update_vals in domains_to_update:
                        domain = update_vals[-1]
                        update_data = update_vals[:-1]
                        update_cols = [f"'{date}' = ?" for date in sorted(year_dates) if date in year_rankings]
                        
                        if update_cols:
                            try:
                                update_sql = f"UPDATE {table_name} SET {', '.join(update_cols)} WHERE domain = ?"
                                cursor.execute(update_sql, update_data + [domain])
                            except Exception as e:
                                logging.error(f"更新域名 {domain} 排名时出错: {e}")
                
                # 每批次提交一次，避免事务过大
                conn.commit()
                conn.execute("BEGIN TRANSACTION")
                batch_time = (datetime.now() - batch_start_time).total_seconds()
                logging.info(f"已处理年份 {year} 的排名数据批次 {batch_num}/{total_batches}，耗时: {batch_time:.2f}秒")
            
            logging.info(f"完成年份 {year} 的排名数据更新")
        
        # 提交最终事务
        conn.commit()
        logging.info("数据库更新成功完成")
        
    except Exception as e:
        logging.error(f"Error updating database: {e}")
        if conn:
            conn.rollback()
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
        
        # 对于2024年，从6月7日开始
        if year == 2024:
            start_date = date(2024, 6, 7)
        else:
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

if __name__ == "__main__":
    # 确保历史数据目录存在
    if not os.path.exists(HISTORICAL_DATA_DIR):
        os.makedirs(HISTORICAL_DATA_DIR)
        logging.info(f"Created directory for historical data: {HISTORICAL_DATA_DIR}")
    
    # 导入历史数据
    import_historical_data()
    
    # 检查结果文件是否存在及是否为空
    files_to_check = [DOMAINS_RANKINGS_FILE, DOMAINS_FIRST_SEEN_FILE]
    for file_path in files_to_check:
        if not os.path.exists(file_path):
            logging.error(f"结果文件不存在: {file_path}")
        else:
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                logging.error(f"结果文件为空: {file_path}")
            else:
                logging.info(f"结果文件正常: {file_path}, 大小: {file_size} 字节")
                
                # 检查文件内容是否有效
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        first_line = f.readline().strip()
                        if not first_line:
                            logging.error(f"结果文件内容无效: {file_path}, 文件头为空")
                        else:
                            line_count = sum(1 for _ in f) + 1  # +1 是因为已经读取了第一行
                            logging.info(f"结果文件内容有效: {file_path}, 包含 {line_count} 行数据")
                except Exception as e:
                    logging.error(f"检查文件内容时出错: {file_path}, 错误: {e}")