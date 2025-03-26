#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import time
import logging
import json
import argparse
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 检查点文件
CHECKPOINT_FILE = 'import_checkpoint.json'

def load_checkpoint():
    """加载检查点信息"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"加载检查点文件时出错: {e}")
    return {
        'processed_commits': [],
        'last_processed_chunk': None,
        'last_processed_date': None,
        'total_domains_updated': 0
    }

def run_command(cmd):
    """运行命令并返回结果"""
    logging.info(f"执行命令: {cmd}")
    try:
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # 实时输出日志
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                logging.info(output.strip())
        
        # 获取错误输出
        stderr = process.stderr.read()
        if stderr:
            logging.error(stderr)
        
        return process.poll()
    except Exception as e:
        logging.error(f"执行命令时出错: {e}")
        return -1

def get_total_chunks():
    """获取总块数"""
    # 检查历史数据目录是否存在
    if not os.path.exists('historical_extracts'):
        logging.error("历史数据目录不存在: historical_extracts")
        return 0
    
    # 获取所有日期目录
    commit_dirs = [d for d in os.listdir('historical_extracts') if os.path.isdir(os.path.join('historical_extracts', d))]
    
    # 计算块数
    chunk_size = 30  # 每个块处理30个提交
    return (len(commit_dirs) + chunk_size - 1) // chunk_size

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='运行分块导入历史域名排名数据')
    parser.add_argument('--start-chunk', type=int, help='开始处理的块ID')
    parser.add_argument('--end-chunk', type=int, help='结束处理的块ID')
    parser.add_argument('--batch-size', type=int, default=5000, help='每批处理的域名数量')
    parser.add_argument('--retry-failed', action='store_true', help='重试失败的块')
    args = parser.parse_args()
    
    start_time = datetime.now()
    logging.info(f"脚本开始执行时间: {start_time}")
    
    # 获取总块数
    total_chunks = get_total_chunks()
    if total_chunks == 0:
        logging.error("没有找到可处理的数据块")
        return
    
    logging.info(f"总共有 {total_chunks} 个数据块需要处理")
    
    # 加载检查点
    checkpoint = load_checkpoint()
    last_processed_chunk = checkpoint.get('last_processed_chunk')
    
    # 确定开始和结束块
    start_chunk = args.start_chunk if args.start_chunk is not None else (int(last_processed_chunk) + 1 if last_processed_chunk else 1)
    end_chunk = args.end_chunk if args.end_chunk is not None else total_chunks
    
    logging.info(f"将处理数据块 {start_chunk} 到 {end_chunk}")
    
    # 处理每个块
    successful_chunks = []
    failed_chunks = []
    
    for chunk_id in range(start_chunk, end_chunk + 1):
        logging.info(f"开始处理数据块 {chunk_id}/{end_chunk}")
        
        # 如果不是重试模式，且该块已处理过，则跳过
        if not args.retry_failed and last_processed_chunk and chunk_id <= int(last_processed_chunk):
            logging.info(f"数据块 {chunk_id} 已处理过，跳过")
            successful_chunks.append(chunk_id)
            continue
        
        # 运行导入脚本处理该块
        cmd = f'python import_historical_data_chunked.py --chunk-id {chunk_id} --batch-size {args.batch_size}'
        result = run_command(cmd)
        
        if result == 0:
            logging.info(f"数据块 {chunk_id} 处理成功")
            successful_chunks.append(chunk_id)
        else:
            logging.error(f"数据块 {chunk_id} 处理失败，返回码: {result}")
            failed_chunks.append(chunk_id)
            
            # 如果连续失败3次，暂停一段时间
            if len(failed_chunks) >= 3 and all(c == failed_chunks[-1] - i for i, c in enumerate(failed_chunks[-3:])):
                logging.warning("连续失败3次，暂停5分钟后继续")
                time.sleep(300)
    
    # 如果所有块都处理完成，合并结果
    if not failed_chunks and successful_chunks:
        logging.info("所有数据块处理完成，开始合并结果")
        run_command('python import_historical_data_chunked.py --merge-only')
    else:
        logging.warning(f"有 {len(failed_chunks)} 个数据块处理失败: {failed_chunks}")
        logging.warning("请修复问题后使用 --retry-failed 参数重试失败的块")
    
    end_time = datetime.now()
    duration = end_time - start_time
    logging.info(f"脚本结束执行时间: {end_time}")
    logging.info(f"总执行时间: {duration}")
    logging.info(f"成功处理的数据块: {len(successful_chunks)}/{end_chunk - start_chunk + 1}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"主程序执行出错: {e}")
        import traceback
        logging.error(traceback.format_exc())
        sys.exit(1)