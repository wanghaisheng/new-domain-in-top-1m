# ... existing code ...

def download_and_extract_zip(commit_hash, date_str, output_dir):
    """
    从GitHub下载指定commit的ZIP文件并解压
    
    Args:
        commit_hash: GitHub提交哈希
        date_str: 日期字符串
        output_dir: 输出目录
        
    Returns:
        bool: 是否成功
    """
    import requests
    import zipfile
    import io
    import os
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 构建ZIP文件URL
    zip_url = f"https://github.com/adysec/top_1m_domains/raw/{commit_hash}/tranco.zip"
    logging.info(f"尝试下载ZIP文件: {zip_url}")
    
    try:
        # 下载ZIP文件
        response = requests.get(zip_url, timeout=30)
        if response.status_code != 200:
            logging.error(f"下载ZIP文件失败: {response.status_code} - {response.text}")
            return False
        
        # 保存ZIP文件
        zip_path = os.path.join(output_dir, "tranco.zip")
        with open(zip_path, 'wb') as f:
            f.write(response.content)
        logging.info(f"已保存ZIP文件到: {zip_path}")
        
        # 解压ZIP文件
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
        logging.info(f"已解压ZIP文件到: {output_dir}")
        
        # 查找CSV文件
        csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
        if not csv_files:
            logging.error(f"解压后未找到CSV文件")
            return False
        
        # 重命名CSV文件
        csv_file = os.path.join(output_dir, csv_files[0])
        new_csv_file = os.path.join(output_dir, "top-1m.csv")
        if csv_file != new_csv_file:
            os.rename(csv_file, new_csv_file)
            logging.info(f"已重命名CSV文件: {csv_file} -> {new_csv_file}")
        
        return True
    except Exception as e:
        logging.error(f"下载和解压ZIP文件失败: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False

def extract_domains_from_csv(csv_file):
    """
    从CSV文件中提取域名列表
    
    Args:
        csv_file: CSV文件路径
        
    Returns:
        list: 域名列表
    """
    import csv
    
    domains = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 2:
                    domain = row[1].strip()
                    domains.append(domain)
        
        logging.info(f"从CSV文件 {csv_file} 中提取了 {len(domains)} 个域名")
        return domains
    except Exception as e:
        logging.error(f"从CSV文件中提取域名失败: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return []

def update_sqlite_database(date_str, domains):
    """
    更新SQLite数据库中指定日期的域名数据
    
    Args:
        date_str: 日期字符串
        domains: 域名列表
        
    Returns:
        bool: 是否成功
    """
    import sqlite3
    import os
    
    if not domains:
        logging.warning(f"日期 {date_str} 没有域名数据，跳过")
        return False
    
    try:
        # 数据库路径
        db_dir = os.path.join('data', 'persisted-to-cache')
        os.makedirs(db_dir, exist_ok=True)
        db_file = os.path.join(db_dir, 'domain_rank.db')
        
        # 连接数据库
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # 创建必要的表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS db_metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS domains (
                domain TEXT PRIMARY KEY,
                first_seen DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 获取年份
        year = date_str.split('-')[0]
        table_name = f"rankings_{year}"
        
        # 创建年份表
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                domain TEXT PRIMARY KEY,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 检查日期列是否存在
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]
        
        if date_str not in columns:
            # 添加日期列
            cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "{date_str}" INTEGER')
            logging.info(f"添加日期列: {date_str} 到表 {table_name}")
        
        # 更新domains表
        for domain in domains:
            cursor.execute("""
                INSERT OR IGNORE INTO domains (domain, first_seen)
                VALUES (?, ?)
            """, (domain, date_str))
        
        # 更新rankings表
        for i, domain in enumerate(domains, 1):
            cursor.execute(f"""
                INSERT INTO {table_name} (domain, "{date_str}")
                VALUES (?, ?)
                ON CONFLICT(domain) DO UPDATE SET "{date_str}" = ?
            """, (domain, i, i))
        
        # 保存新域名到文件
        new_domains_dir = 'new_domains'
        os.makedirs(new_domains_dir, exist_ok=True)
        new_domains_file = os.path.join(new_domains_dir, f"{date_str}.txt")
        with open(new_domains_file, 'w', encoding='utf-8') as f:
            for domain in domains:
                f.write(f"{domain}\n")
        
        conn.commit()
        conn.close()
        
        logging.info(f"成功导入 {date_str} 的 {len(domains)} 个域名到数据库")
        return True
    except Exception as e:
        logging.error(f"更新数据库时出错: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False

def process_historical_data(start_date=None, end_date=None, batch_days=5):
    """
    处理历史数据
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        batch_days: 每批处理的天数
        
    Returns:
        bool: 是否成功
    """
    import json
    import os
    from datetime import datetime, timedelta
    
    # 进度文件
    progress_file = 'historical_import_progress.json'
    
    # 加载进度
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r') as f:
                progress = json.load(f)
            logging.info(f"加载进度文件: {progress}")
        except Exception as e:
            logging.error(f"加载进度文件失败: {e}")
            progress = {}
    else:
        progress = {}
    
    # 如果没有进度或需要重新开始
    if not progress or start_date or end_date:
        # 获取commits
        commits = fetch_commits_by_date_range(start_date, end_date)
        if not commits:
            logging.error("没有找到符合日期范围的commits")
            return False
        
        # 初始化进度
        progress = {
            'commits': commits,
            'current_index': 0,
            'total_commits': len(commits),
            'processed_dates': []
        }
    
    # 处理一批数据
    processed_in_batch = 0
    while processed_in_batch < batch_days and progress['current_index'] < progress['total_commits']:
        commit = progress['commits'][progress['current_index']]
        commit_hash = commit['commit_hash']
        date_str = commit['date']
        
        # 如果已经处理过这个日期，跳过
        if date_str in progress['processed_dates']:
            logging.info(f"日期 {date_str} 已处理过，跳过")
            progress['current_index'] += 1
            continue
        
        logging.info(f"处理日期: {date_str} (进度: {progress['current_index']+1}/{progress['total_commits']})")
        
        # 创建临时目录
        temp_dir = os.path.join('temp', date_str)
        os.makedirs(temp_dir, exist_ok=True)
        
        # 下载并解压ZIP文件
        if download_and_extract_zip(commit_hash, date_str, temp_dir):
            # 提取域名
            csv_file = os.path.join(temp_dir, "top-1m.csv")
            if os.path.exists(csv_file):
                domains = extract_domains_from_csv(csv_file)
                
                # 更新数据库
                if domains and update_sqlite_database(date_str, domains):
                    # 更新进度
                    progress['processed_dates'].append(date_str)
                    processed_in_batch += 1
        
        # 更新进度索引
        progress['current_index'] += 1
        
        # 保存进度
        with open(progress_file, 'w') as f:
            json.dump(progress, f, indent=2)
    
    # 检查是否完成所有处理
    if progress['current_index'] >= progress['total_commits']:
        logging.info(f"已完成所有 {progress['total_commits']} 个commits的处理")
        # 可以删除进度文件，下次将重新开始
        if os.path.exists(progress_file):
            os.remove(progress_file)
            logging.info('已删除进度文件，下次将重新开始')
        return True
    else:
        logging.info(f"本次批处理完成，已处理 {progress['current_index']}/{progress['total_commits']} 个commits，下次将继续处理")
        return False

# ... existing code ...

def main():
    try:
        logging.debug("进入main函数")
        
        # 解析命令行参数
        parser = argparse.ArgumentParser(description='Run chunked import of historical domain data')
        # ... existing code ...
        
        # 添加历史数据处理参数
        parser.add_argument('--process-historical', action='store_true', help='处理历史数据')
        parser.add_argument('--batch-days', type=int, default=5, help='每批处理的天数')
        
        args = parser.parse_args()
        logging.debug(f"解析的命令行参数: {args}")
        
        # 记录脚本开始执行时间
        start_time = datetime.now()
        logging.info(f"脚本开始执行时间: {start_time}")
        
        # 如果需要处理历史数据
        if args.process_historical:
            process_historical_data(args.start_date, args.end_date, args.batch_days)
            return
        
        # 如果只需要生成新域名文件
        if args.generate_new_domains:
            generate_new_domains()
            return
        
        # ... existing code ...
    
    except Exception as e:
        logging.critical(f"脚本执行过程中发生未捕获的异常: {e}")
        import traceback
        logging.critical(traceback.format_exc())
        sys.exit(1)

# ... existing code ...