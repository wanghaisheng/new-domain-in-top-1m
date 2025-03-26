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
    
    except Exception as e:
        logging.critical(f"脚本执行过程中发生未捕获的异常: {e}")
        import traceback
        logging.critical(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    try:
        logging.debug("脚本作为主程序启动")
        main()
    except Exception as e:
        logging.critical(f"主程序执行过程中发生未捕获的异常: {e}")
        import traceback
        logging.critical(traceback.format_exc())
        sys.exit(1)