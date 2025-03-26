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
    
    # 创建新域名目录
    os.makedirs('new_domains', exist_ok=True)
    
    try:
        # 创建日期映射字典 - 从目录名到commit日期
        date_mapping = {}
        historical_data_dir = 'historical_extracts'
        
        # 获取所有日期目录
        if os.path.exists(historical_data_dir):
            date_dirs = [d for d in os.listdir(historical_data_dir) 
                        if os.path.isdir(os.path.join(historical_data_dir, d))]
            
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
        
        # 过滤日期范围
        filtered_dates = sorted(list(set(date_mapping.values())))
        if args.start_date:
            filtered_dates = [d for d in filtered_dates if d >= args.start_date]
            logging.info(f"应用开始日期过滤 {args.start_date}，剩余日期数: {len(filtered_dates)}")
        
        if args.end_date:
            filtered_dates = [d for d in filtered_dates if d <= args.end_date]
            logging.info(f"应用结束日期过滤 {args.end_date}，剩余日期数: {len(filtered_dates)}")
        
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
    
    args = parser.parse_args()
    
    # 记录脚本开始执行时间
    start_time = datetime.now()
    logging.info(f"脚本开始执行时间: {start_time}")
    
    # 如果只需要生成新域名文件
    if args.generate_new_domains:
        generate_new_domains()
        return
    
    # 验证数据文件
    if args.verify_data:
        verify_data_files()
        return
    
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
        else:
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
            
            # 如果设置了重试失败的块
            if args.retry_failed:
                logging.info(f"重试数据块 {chunk_id}")
                try:
                    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                    logging.info(result.stdout)
                    if result.stderr:
                        logging.error(result.stderr)
                    successful_chunks += 1
                    logging.info(f"数据块 {chunk_id} 重试成功")
                except subprocess.CalledProcessError as e:
                    logging.error(f"数据块 {chunk_id} 重试失败: {e}")
                    logging.error(e.stdout)
                    logging.error(e.stderr)
    
    # 所有数据块处理完成后，合并结果
    logging.info("所有数据块处理完成，开始合并结果")
    
    # 构建合并命令
    merge_cmd = ["python", "import_historical_data_chunked.py", "--merge-only"]
    
    # 执行合并命令
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

if __name__ == "__main__":
    main()