#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import logging
import subprocess
import requests
from datetime import datetime

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def determine_chunk_parameters(historical_data_dir='historical_extracts', chunk_size=30):
    """
    自动确定块参数
    
    Args:
        historical_data_dir: 历史数据目录
        chunk_size: 每个块包含的日期数量
        
    Returns:
        tuple: (start_chunk, end_chunk, total_chunks)
    """
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
            
            # 检查CSV文件是否存在且有效
            if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
                # 检查CSV文件内容
                try:
                    with open(csv_file, 'r', encoding='utf-8') as f:
                        first_line = f.readline().strip()
                        if ',' in first_line and ('rank' in first_line.lower() or first_line[0].isdigit()):
                            logging.info(f"CSV文件有效: {csv_file}")
                            continue
                        else:
                            logging.warning(f"CSV文件格式可能不正确: {csv_file}, 第一行: {first_line}")
                except Exception as e:
                    logging.error(f"读取CSV文件失败 {csv_file}: {e}")
            
            # 如果CSV文件不存在或无效，尝试从ZIP文件恢复
            if os.path.exists(zip_file) and os.path.getsize(zip_file) > 0:
                logging.info(f"尝试从ZIP文件恢复: {zip_file}")
                try:
                    # 检查文件类型
                    import magic
                    file_type = magic.from_file(zip_file)
                    logging.info(f"文件类型: {file_type}")
                    
                    if "Zip archive" in file_type:
                        # 解压ZIP文件
                        import zipfile
                        with zipfile.ZipFile(zip_file, 'r') as z:
                            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                            if csv_files:
                                with z.open(csv_files[0]) as zf, open(csv_file, 'wb') as f:
                                    f.write(zf.read())
                                logging.info(f"从ZIP文件恢复了CSV: {csv_files[0]} -> {csv_file}")
                                continue
                    else:
                        # 可能是CSV文件但扩展名错误
                        with open(zip_file, 'r', encoding='utf-8', errors='ignore') as f:
                            first_line = f.readline().strip()
                            if ',' in first_line and ('rank' in first_line.lower() or first_line[0].isdigit()):
                                # 复制为CSV文件
                                import shutil
                                shutil.copy2(zip_file, csv_file)
                                logging.info(f"将文本文件复制为CSV: {zip_file} -> {csv_file}")
                                continue
                except ImportError:
                    logging.warning("未安装python-magic库，无法检测文件类型")
                except Exception as e:
                    logging.error(f"从ZIP文件恢复失败 {zip_file}: {e}")
            
            # 如果本地文件都无效，尝试从GitHub重新下载
            if commit_hash:
                logging.info(f"尝试从GitHub重新下载数据 (提交: {commit_hash})")
                
                # 只保留有效的URL
                possible_urls = [
                    f"https://github.com/adysec/top_1m_domains/raw/{commit_hash}/tranco.zip"
                ]
                
                # 尝试下载date.txt文件获取日期信息
                date_url = f"https://github.com/adysec/top_1m_domains/raw/{commit_hash}/date.txt"
                date_file_path = os.path.join(full_dir_path, "date.txt")
                
                try:
                    logging.info(f"尝试从GitHub下载日期文件: {date_url}")
                    date_response = requests.get(date_url, timeout=10)
                    if date_response.status_code == 200:
                        with open(date_file_path, 'wb') as f:
                            f.write(date_response.content)
                        logging.info(f"成功下载日期文件: {date_file_path}")
                        
                        # 读取日期内容
                        with open(date_file_path, 'r') as f:
                            date_content = f.read().strip()
                            logging.info(f"日期文件内容: {date_content}")
                except Exception as e:
                    logging.error(f"下载或读取日期文件失败: {e}")
                
                download_success = False
                
                # 首先尝试使用requests下载
                for url in possible_urls:
                    try:
                        logging.info(f"使用requests尝试URL: {url}")
                        response = requests.head(url, timeout=10)  # 添加超时设置
                        if response.status_code == 200:
                            logging.info(f"找到可用URL: {url}")
                            
                            # 下载文件
                            response = requests.get(url, timeout=30)  # 增加下载超时时间
                            if response.status_code == 200:
                                # 保存为ZIP文件
                                with open(zip_file, 'wb') as f:
                                    f.write(response.content)
                                
                                logging.info(f"成功下载并保存为ZIP: {zip_file}")
                                download_success = True
                                break
                    except requests.exceptions.RequestException as e:
                        logging.error(f"requests下载失败 {url}: {e}")
                
                # 如果requests下载失败，尝试使用wget
                if not download_success:
                    for url in possible_urls:
                        try:
                            logging.info(f"使用wget尝试URL: {url}")
                            import subprocess
                            
                            # 使用wget下载文件
                            wget_cmd = ["wget", "-q", "--tries=3", "--timeout=30", "-O", zip_file, url]
                            logging.info(f"执行命令: {' '.join(wget_cmd)}")
                            
                            result = subprocess.run(wget_cmd, capture_output=True, text=True)
                            
                            # 检查wget是否成功
                            if result.returncode == 0 and os.path.exists(zip_file) and os.path.getsize(zip_file) > 0:
                                logging.info(f"wget成功下载文件: {zip_file}")
                                download_success = True
                                break
                            else:
                                logging.error(f"wget下载失败: {result.stderr}")
                        except Exception as e:
                            logging.error(f"执行wget命令失败: {e}")
                
                # 如果下载成功，尝试处理文件
                if download_success:
                    # 尝试解压ZIP文件
                    try:
                        import zipfile
                        with zipfile.ZipFile(zip_file, 'r') as z:
                            csv_files = [f for f in z.namelist() if f.endswith('.csv')]
                            if csv_files:
                                with z.open(csv_files[0]) as zf, open(csv_file, 'wb') as f:
                                    f.write(zf.read())
                                logging.info(f"从下载的ZIP文件解压了CSV: {csv_files[0]} -> {csv_file}")
                    except Exception as e:
                        logging.error(f"解压下载的ZIP文件失败: {e}")
                        # 如果解压失败，可能是直接的CSV文件
                        try:
                            with open(zip_file, 'r', encoding='utf-8', errors='ignore') as f:
                                first_line = f.readline().strip()
                                if ',' in first_line:
                                    import shutil
                                    shutil.copy2(zip_file, csv_file)
                                    logging.info(f"将下载的文件复制为CSV: {zip_file} -> {csv_file}")
                        except Exception as e2:
                            logging.error(f"尝试将下载文件作为CSV处理失败: {e2}")
        else:
            logging.warning(f"目录 {date_dir} 没有提交哈希文件")
    
    logging.info("数据文件验证完成")

def generate_new_domains():
    """
    生成每日新域名文件
    从commit ID中提取的date.txt文件中获取日期信息
    """
    import re
    import argparse
    
    logging.info("开始生成每日新域名文件...")
    
    # 解析命令行参数，获取日期范围
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-date', type=str, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYY-MM-DD)')
    args, _ = parser.parse_known_args()
    
    # 记录输入的日期范围
    if args.start_date or args.end_date:
        date_range_msg = "日期范围过滤条件: "
        if args.start_date:
            date_range_msg += f"开始日期={args.start_date} "
        if args.end_date:
            date_range_msg += f"结束日期={args.end_date}"
        logging.info(date_range_msg)
    
    # 创建新域名目录
    os.makedirs('new_domains', exist_ok=True)
    
    try:
        # 创建日期映射字典 - 从目录名到commit日期
        date_mapping = {}
        historical_data_dir = 'historical_extracts'
        
        # 检查历史数据目录是否存在
        if not os.path.exists(historical_data_dir):
            logging.error(f"历史数据目录不存在: {historical_data_dir}")
            return False
        
        # 获取所有日期目录
        date_dirs = [d for d in os.listdir(historical_data_dir) 
                    if os.path.isdir(os.path.join(historical_data_dir, d))]
        
        if not date_dirs:
            logging.error(f"历史数据目录 {historical_data_dir} 中没有找到任何日期目录")
            return False
            
        logging.info(f"找到 {len(date_dirs)} 个日期目录")
        
        # 从每个目录的date.txt文件中读取日期
        for date_dir in date_dirs:
            date_file = os.path.join(historical_data_dir, date_dir, "date.txt")
            commit_file = os.path.join(historical_data_dir, date_dir, "commit_hash.txt")
            
            # 首先尝试从本地date.txt读取日期
            if os.path.exists(date_file):
                try:
                    with open(date_file, 'r') as f:
                        actual_date = f.read().strip()
                        # 验证日期格式 (YYYY-MM-DD)
                        if re.match(r'^\d{4}-\d{2}-\d{2}$', actual_date):
                            date_mapping[date_dir] = actual_date
                            logging.info(f"目录 {date_dir} 对应日期(从date.txt): {actual_date}")
                        else:
                            logging.warning(f"日期格式不正确: {actual_date}，尝试其他方法")
                except Exception as e:
                    logging.error(f"读取日期文件失败 {date_file}: {e}")
            
            # 如果本地没有有效的date.txt，尝试从GitHub获取
            if date_dir not in date_mapping and os.path.exists(commit_file):
                try:
                    with open(commit_file, 'r') as f:
                        commit_hash = f.read().strip()
                    
                    # 构建GitHub上date.txt的URL
                    date_url = f"https://github.com/adysec/top_1m_domains/raw/{commit_hash}/date.txt"
                    logging.info(f"尝试从GitHub获取日期: {date_url}")
                    
                    # 下载date.txt
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

# 在main函数中添加对这个函数的调用
def main():
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
        
        # 根据日期范围查询commits
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
    
    # ... 继续现有代码 ...