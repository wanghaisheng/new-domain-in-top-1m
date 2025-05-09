#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import logging
import subprocess
import requests
from datetime import datetime
import zipfile
import codecs
import json
import csv
import pandas as pd

# 配置日志记录
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# 添加初始调试信息
logging.debug("脚本开始执行")
logging.debug(f"Python版本: {sys.version}")
logging.debug(f"当前工作目录: {os.getcwd()}")
logging.debug(f"命令行参数: {sys.argv}")

def determine_chunk_parameters(historical_data_dir='historical_extracts', chunk_size=30):
    """
    自动确定块参数
    
    Args:
        historical_data_dir: 历史数据目录
        chunk_size: 每个块包含的日期数量
        
    Returns:
        tuple: (start_chunk, end_chunk, total_chunks)
    """
    logging.debug(f"调用 determine_chunk_parameters 函数，参数: historical_data_dir={historical_data_dir}, chunk_size={chunk_size}")
    
    # 检查历史数据目录是否存在
    if not os.path.exists(historical_data_dir):
        logging.error(f"历史数据目录不存在: {historical_data_dir}")
        return 0, 0, 1
    
    # 获取所有日期目录
    date_dirs = [d for d in os.listdir(historical_data_dir) if os.path.isdir(os.path.join(historical_data_dir, d))]
    
    # 计算总块数
    total_dirs = len(date_dirs)
    total_chunks = (total_dirs + chunk_size - 1) // chunk_size  # 向上取整
    
    logging.info(f"找到 {total_dirs} 个日期目录，可以分为 {total_chunks} 个数据块")
    
    # 默认处理所有块
    return 0, total_chunks - 1, total_chunks

def verify_data_files(historical_data_dir='historical_extracts'):
    """
    验证数据文件的完整性，尝试修复问题
    
    Args:
        historical_data_dir: 历史数据目录
    """
    if not os.path.exists(historical_data_dir):
        logging.error(f"历史数据目录不存在: {historical_data_dir}")
        return
    
    # 获取所有日期目录
    date_dirs = [d for d in os.listdir(historical_data_dir) 
                if os.path.isdir(os.path.join(historical_data_dir, d))]
    
    logging.info(f"开始验证 {len(date_dirs)} 个日期目录的数据文件...")
    
    for date_dir in date_dirs:
        full_dir_path = os.path.join(historical_data_dir, date_dir)
        zip_file = os.path.join(full_dir_path, "tranco.zip")
        csv_file = os.path.join(full_dir_path, "top-1m.csv")
        commit_file = os.path.join(full_dir_path, "commit_hash.txt")
        
        # 读取提交哈希
        commit_hash = None
        if os.path.exists(commit_file):
            try:
                with open(commit_file, 'r') as f:
                    commit_hash = f.read().strip()
            except Exception as e:
                logging.error(f"读取提交哈希文件失败 {date_dir}: {e}")
        
        if commit_hash:
            logging.info(f"验证目录 {date_dir} (提交: {commit_hash})")
            # 验证逻辑...

def fetch_commits_by_date_range(start_date=None, end_date=None, repo="adysec/top_1m_domains"):
    """
    根据日期范围从GitHub获取commit ids
    
    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        repo: GitHub仓库名称
        
    Returns:
        list: 符合日期范围的commit信息列表，每项包含commit_hash和date
    """
    import requests
    from datetime import datetime, timedelta
    import time
    
    logging.info(f"开始根据日期范围查询GitHub commits: {start_date} 到 {end_date}")
    
    # 构建API URL
    api_url = f"https://api.github.com/repos/{repo}/commits"
    
    # 转换日期格式
    if start_date:
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        # GitHub API需要ISO 8601格式
        start_date_iso = start_date_obj.isoformat() + "Z"
    else:
        start_date_iso = None
    
    if end_date:
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        # 将结束日期设为当天的最后一秒
        end_date_obj = end_date_obj.replace(hour=23, minute=59, second=59)
        end_date_iso = end_date_obj.isoformat() + "Z"
    else:
        end_date_iso = None
    
    # 构建查询参数
    params = {"per_page": 100}  # 每页最多100个结果
    if start_date_iso:
        params["since"] = start_date_iso
    if end_date_iso:
        params["until"] = end_date_iso
    
    all_commits = []
    page = 1
    
    # 分页获取所有符合条件的commits
    while True:
        params["page"] = page
        logging.info(f"获取第 {page} 页commits")
        
        try:
            response = requests.get(api_url, params=params)
            
            # 检查是否达到API速率限制
            if response.status_code == 403 and "rate limit" in response.text.lower():
                reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
                wait_time = max(reset_time - time.time(), 0) + 1
                logging.warning(f"达到GitHub API速率限制，等待 {wait_time} 秒后重试")
                time.sleep(wait_time)
                continue
            
            # 检查其他错误
            if response.status_code != 200:
                logging.error(f"GitHub API请求失败: {response.status_code} - {response.text}")
                break
            
            commits_page = response.json()
            
            # 如果没有更多结果，退出循环
            if not commits_page:
                break
            
            # 处理每个commit
            for commit in commits_page:
                commit_hash = commit["sha"]
                commit_date_str = commit["commit"]["committer"]["date"]
                # 转换为YYYY-MM-DD格式
                commit_date_obj = datetime.strptime(commit_date_str, "%Y-%m-%dT%H:%M:%SZ")
                commit_date = commit_date_obj.strftime("%Y-%m-%d")
                
                all_commits.append({
                    "commit_hash": commit_hash,
                    "date": commit_date
                })
                logging.info(f"找到commit: {commit_hash} (日期: {commit_date})")
            
            # 如果结果数量少于每页数量，说明已经是最后一页
            if len(commits_page) < params["per_page"]:
                break
            
            page += 1
            
            # 避免频繁请求触发GitHub API限制
            time.sleep(1)
            
        except Exception as e:
            logging.error(f"获取GitHub commits失败: {e}")
            import traceback
            logging.error(traceback.format_exc())
            break
    
    # 按日期排序
    all_commits.sort(key=lambda x: x["date"])
    
    # 输出查询结果
    if all_commits:
        logging.info(f"在日期范围 {start_date} 到 {end_date} 内找到 {len(all_commits)} 个commits")
        for commit in all_commits:
            logging.info(f"Commit: {commit['commit_hash']} (日期: {commit['date']})")
    else:
        logging.warning(f"在日期范围 {start_date} 到 {end_date} 内没有找到任何commits")
    
    return all_commits

def generate_new_domains():
    """
    生成每日新域名文件
    从commit ID中提取的date.txt文件中获取日期信息
    """
    import re
    import argparse
    
    logging.info("开始生成每日新域名文件...")
    
    # 解析命令行参数
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-date', type=str, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYY-MM-DD)')
    args, _ = parser.parse_known_args()
    
    # 创建新域名目录
    os.makedirs('new_domains', exist_ok=True)
    
    try:
        # 创建日期映射字典 - 从目录名到commit日期
        date_mapping = {}
        historical_data_dir = 'historical_extracts'
        
        # 检查历史数据目录是否存在
        if not os.path.exists(historical_data_dir):
            logging.error(f"历史数据目录不存在: {historical_data_dir}")
            logging.info(f"尝试创建历史数据目录: {historical_data_dir}")
            os.makedirs(historical_data_dir, exist_ok=True)
            return False
        
        # 获取所有日期目录
        date_dirs = [d for d in os.listdir(historical_data_dir) 
                    if os.path.isdir(os.path.join(historical_data_dir, d))]
        
        if not date_dirs:
            logging.error(f"历史数据目录 {historical_data_dir} 中没有找到任何日期目录")
            logging.info("请先运行导入历史数据的脚本，例如: python import_historical_data_chunked.py")
            return False
            
        logging.info(f"找到 {len(date_dirs)} 个日期目录: {date_dirs}")
        
        # 处理每个日期目录
        for date_dir in date_dirs:
            full_dir_path = os.path.join(historical_data_dir, date_dir)
            date_file = os.path.join(full_dir_path, "date.txt")
            commit_file = os.path.join(full_dir_path, "commit_hash.txt")
            
            # 尝试从date.txt文件获取日期
            if os.path.exists(date_file):
                try:
                    with open(date_file, 'r') as f:
                        actual_date = f.read().strip()
                        # 验证日期格式
                        if re.match(r'^\d{4}-\d{2}-\d{2}$', actual_date):
                            date_mapping[date_dir] = actual_date
                            logging.info(f"目录 {date_dir} 对应日期(从本地): {actual_date}")
                            continue
                except Exception as e:
                    logging.error(f"读取日期文件失败 {date_dir}: {e}")
            
            # 如果没有有效的date.txt，尝试从commit_hash.txt获取commit ID并从GitHub获取日期
            if os.path.exists(commit_file):
                try:
                    with open(commit_file, 'r') as f:
                        commit_hash = f.read().strip()
                        
                    # 从GitHub获取日期
                    date_url = f"https://github.com/adysec/top_1m_domains/raw/{commit_hash}/date.txt"
                    logging.info(f"尝试从GitHub获取日期: {date_url}")
                    
                    response = requests.get(date_url, timeout=10)
                    if response.status_code == 200:
                        actual_date = response.text.strip()
                        # 验证日期格式
                        if re.match(r'^\d{4}-\d{2}-\d{2}$', actual_date):
                            date_mapping[date_dir] = actual_date
                            logging.info(f"目录 {date_dir} 对应日期(从GitHub): {actual_date}")
                            
                            # 保存到本地
                            with open(date_file, 'w') as f:
                                f.write(actual_date)
                            logging.info(f"已保存日期到本地: {date_file}")
                        else:
                            logging.warning(f"从GitHub获取的日期格式不正确: {actual_date}")
                except Exception as e:
                    logging.error(f"从GitHub获取日期失败: {e}")
            
            # 如果仍然没有有效日期，使用目录名
            if date_dir not in date_mapping:
                date_mapping[date_dir] = date_dir
                logging.warning(f"无法获取目录 {date_dir} 的日期，使用目录名作为日期")
        
        # 输出所有可用的日期
        all_dates = sorted(list(set(date_mapping.values())))
        logging.info(f"所有可用的日期: {all_dates}")
        
        # 过滤日期范围
        filtered_dates = all_dates.copy()
        if args.start_date:
            filtered_dates = [d for d in filtered_dates if d >= args.start_date]
            logging.info(f"应用开始日期过滤 {args.start_date}，剩余日期数: {len(filtered_dates)}")
        
        if args.end_date:
            filtered_dates = [d for d in filtered_dates if d <= args.end_date]
            logging.info(f"应用结束日期过滤 {args.end_date}，剩余日期数: {len(filtered_dates)}")
        
        if not filtered_dates:
            logging.warning(f"在指定的日期范围内没有找到任何日期")
            return False
        
        # 处理每个日期
        for commit_date in filtered_dates:
            # 创建日期文件 - 使用commit日期作为文件名
            output_file = f'new_domains/{commit_date}.txt'
            
            # 创建一个空文件
            with open(output_file, 'w', encoding='utf-8') as f:
                pass
            
            logging.info(f'生成了 {commit_date} 的新域名文件')
        
        logging.info('每日新域名文件生成完成')
        return True
    except Exception as e:
        logging.error(f"生成每日新域名文件失败: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False

# 在main函数开始添加try-except块
def main():
    try:
        logging.debug("进入main函数")
        
        # 解析命令行参数
        parser = argparse.ArgumentParser(description='Run chunked import of historical domain data')
        parser.add_argument('--start-chunk', type=int, default=0, help='Start chunk ID to process')
        parser.add_argument('--end-chunk', type=int, default=0, help='End chunk ID to process')
        parser.add_argument('--batch-size', type=int, default=5000, help='Batch size for processing')
        parser.add_argument('--retry-failed', action='store_true', help='Retry failed chunks')
        parser.add_argument('--auto-chunks', action='store_true', help='Automatically determine chunk parameters')
        parser.add_argument('--verify-data', action='store_true', help='Verify and fix data files before processing')
        parser.add_argument('--generate-new-domains', action='store_true', help='Generate daily new domains files')
        parser.add_argument('--start-date', type=str, help='开始日期 (YYYY-MM-DD)')
        parser.add_argument('--end-date', type=str, help='结束日期 (YYYY-MM-DD)')
        
        args = parser.parse_args()
        logging.debug(f"解析的命令行参数: {args}")
        
        # 记录脚本开始执行时间
        start_time = datetime.now()
        logging.info(f"脚本开始执行时间: {start_time}")
        
        # 记录输入的日期范围
        if args.start_date or args.end_date:
            date_range_msg = "输入的日期范围过滤条件: "
            if args.start_date:
                date_range_msg += f"开始日期={args.start_date} "
            if args.end_date:
                date_range_msg += f"结束日期={args.end_date}"
            logging.info(date_range_msg)
        
        # 如果只需要生成新域名文件
        if args.generate_new_domains:
            generate_new_domains()
            return
        
        # 验证数据文件
        if args.verify_data:
            verify_data_files()
            return
        
        # 根据日期范围查询commits
        if args.start_date or args.end_date:
            commits = fetch_commits_by_date_range(args.start_date, args.end_date)
            
            # 如果没有找到commits，提前退出
            if not commits:
                logging.error("没有找到符合日期范围的commits，无法继续处理")
                return
            
            # 创建历史数据目录
            historical_data_dir = 'historical_extracts'
            os.makedirs(historical_data_dir, exist_ok=True)
            
            # 为每个commit创建目录并保存commit hash
            for commit in commits:
                commit_hash = commit["commit_hash"]
                commit_date = commit["date"]
                
                # 使用日期作为目录名
                date_dir = os.path.join(historical_data_dir, commit_date)
                os.makedirs(date_dir, exist_ok=True)
                
                # 保存commit hash
                commit_file = os.path.join(date_dir, "commit_hash.txt")
                with open(commit_file, 'w') as f:
                    f.write(commit_hash)
                
                # 保存日期
                date_file = os.path.join(date_dir, "date.txt")
                with open(date_file, 'w') as f:
                    f.write(commit_date)
                
                logging.info(f"创建了日期目录 {date_dir} 并保存了commit信息")
        
        # 如果指定了自动确定块参数
        if args.auto_chunks:
            start_chunk, end_chunk, total_chunks = determine_chunk_parameters()
        else:
            # 使用命令行参数
            start_chunk = args.start_chunk
            end_chunk = args.end_chunk
            
            # 计算总块数
            historical_data_dir = 'historical_extracts'
            if os.path.exists(historical_data_dir):
                date_dirs = [d for d in os.listdir(historical_data_dir) if os.path.isdir(os.path.join(historical_data_dir, d))]
                chunk_size = 30
                total_chunks = (len(date_dirs) + chunk_size - 1) // chunk_size
                
                # 打印找到的日期目录
                if date_dirs:
                    logging.info(f"找到 {len(date_dirs)} 个日期目录: {date_dirs}")
                else:
                    logging.warning("历史数据目录中没有找到任何日期目录")
            else:
                logging.warning(f"历史数据目录 {historical_data_dir} 不存在，将创建该目录")
                os.makedirs(historical_data_dir, exist_ok=True)
                total_chunks = 1
            
            # 如果end_chunk为0或超出范围，设置为最大值
            if end_chunk <= 0 or end_chunk >= total_chunks:
                end_chunk = total_chunks - 1
        
        logging.info(f"总共有 {total_chunks} 个数据块需要处理")
        logging.info(f"将处理数据块 {start_chunk} 到 {end_chunk}")
        
        # 处理每个数据块
        successful_chunks = 0
        for chunk_id in range(start_chunk, end_chunk + 1):
            logging.info(f"开始处理数据块 {chunk_id}/{end_chunk}")
            
            # 构建命令
            cmd = [
                "python", 
                "import_historical_data_chunked.py", 
                "--chunk-id", str(chunk_id),
                "--batch-size", str(args.batch_size)
            ]
            
            # 添加日期范围参数
            if args.start_date:
                cmd.extend(["--start-date", args.start_date])
            if args.end_date:
                cmd.extend(["--end-date", args.end_date])
            
            # 执行命令
            logging.info(f"执行命令: {' '.join(cmd)}")
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                logging.info(result.stdout)
                if result.stderr:
                    logging.error(result.stderr)
                successful_chunks += 1
                logging.info(f"数据块 {chunk_id} 处理成功")
            except subprocess.CalledProcessError as e:
                logging.error(f"数据块 {chunk_id} 处理失败: {e}")
                logging.error(e.stdout)
                logging.error(e.stderr)
        
        logging.info(f"所有数据块处理完成，开始合并结果")
        
        # 合并所有数据块的结果
        merge_cmd = ["python", "import_historical_data_chunked.py", "--merge-only"]
        logging.info(f"执行命令: {' '.join(merge_cmd)}")
        try:
            result = subprocess.run(merge_cmd, capture_output=True, text=True, check=True)
            logging.info(result.stdout)
            if result.stderr:
                logging.error(result.stderr)
        except subprocess.CalledProcessError as e:
            logging.error(f"合并结果失败: {e}")
            logging.error(e.stdout)
            logging.error(e.stderr)
        
        # 记录脚本结束执行时间
        end_time = datetime.now()
        logging.info(f"脚本结束执行时间: {end_time}")
        logging.info(f"总执行时间: {end_time - start_time}")
        logging.info(f"成功处理的数据块: {successful_chunks}/{end_chunk - start_chunk + 1}")
    
    except Exception as e:
        logging.critical(f"脚本执行过程中发生未捕获的异常: {e}")
        import traceback
        logging.critical(traceback.format_exc())
        sys.exit(1)

# 进度记录相关
PROCESS_HISTORY_FILE = os.path.join('data', 'process_history_chunked.json')
def load_process_history():
    if os.path.exists(PROCESS_HISTORY_FILE):
        try:
            with open(PROCESS_HISTORY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"加载处理历史记录失败: {e}")
    return {'commits': []}
def save_process_history(history):
    try:
        with open(PROCESS_HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
        logging.info("处理历史记录已更新")
    except Exception as e:
        logging.error(f"保存处理历史记录失败: {e}")
# ========== 域名数据持久化 ==========
BACKUP_DIR = 'domains_rankings_backup'
BACKUP_SPLIT_SIZE = 1000000
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
def process_historical_zips_by_commit(start_date=None, end_date=None, repo="adysec/top_1m_domains"):
    """
    遍历2024年至今所有commit，按日期构造zip下载链接，下载并处理zip，支持断点续传。
    """
    # 加载历史数据
    domains_rankings, domains_first_seen = load_domains_from_csv()
    process_history = load_process_history()
    processed_commits = set(process_history.get('commits', []))
    # 获取commit列表
    commits = fetch_commits_by_date_range(start_date, end_date, repo)
    if not commits:
        logging.error("未获取到任何commit，无法处理历史数据")
        return
    if not os.path.exists("data"):
        os.makedirs("data")
    new_domains_dir = os.path.join(os.getcwd(), "new_domains")
    if not os.path.exists(new_domains_dir):
        os.makedirs(new_domains_dir)
    for c in commits:
        commit_hash = c['commit_hash']
        date_str = c['date']
        if commit_hash in processed_commits:
            logging.info(f"已处理过commit {commit_hash}，跳过")
            continue
        zip_url = f"https://github.com/adysec/top_1m_domains/raw/{commit_hash}/tranco.zip"
        zip_file_path = os.path.join("data", f"tranco_{date_str}_{commit_hash[:8]}.zip")
        # 下载zip
        try:
            if not os.path.exists(zip_file_path):
                logging.info(f"下载zip: {zip_url}")
                resp = requests.get(zip_url, timeout=60)
                if resp.status_code == 200:
                    with open(zip_file_path, 'wb') as f:
                        f.write(resp.content)
                    logging.info(f"已保存zip到: {zip_file_path}")
                else:
                    logging.warning(f"下载zip失败: {zip_url} 状态码: {resp.status_code}")
                    continue
            # 校验zip
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
            if not is_valid_zip(zip_file_path):
                logging.error(f"Zip文件无效或损坏: {zip_file_path}")
                continue
            # 解压并处理csv
            csv_file_name = "top-1m.csv"
            with zipfile.ZipFile(zip_file_path, 'r') as z:
                with z.open(csv_file_name, 'r') as csvfile:
                    reader = csv.reader(codecs.getreader("utf-8")(csvfile))
                    data = list(reader)[1:]
            new_domains = []
            current_domains = set()
            for row in data:
                if len(row) == 2:
                    try:
                        rank = int(row[0].strip())
                        domain = row[1].strip()
                        current_domains.add(domain)
                        if domain not in domains_rankings:
                            domains_rankings[domain] = {}
                        domains_rankings[domain][date_str] = rank
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
            # 保存到CSV备份
            save_domains_to_csv(domains_rankings, domains_first_seen, date_str)
            # 记录进度
            process_history['commits'].append(commit_hash)
            save_process_history(process_history)
        except Exception as e:
            logging.error(f"处理commit {commit_hash} 失败: {e}")
            import traceback
            logging.error(traceback.format_exc())
    logging.info("所有历史zip处理完成")
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-date', type=str, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYY-MM-DD)')
    args = parser.parse_args()
    process_historical_zips_by_commit(args.start_date, args.end_date)