#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import csv
import logging
import sqlite3
import zipfile
import codecs
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
import time
from datetime import datetime

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 数据库和文件路径
DB_FILE = 'domain_rank.db'
DOMAINS_RANKINGS_FILE = 'domains_rankings.parquet'
DOMAINS_FIRST_SEEN_FILE = 'domains_first_seen.parquet'
HISTORICAL_DATA_DIR = 'historical_extracts'
CHECKPOINT_FILE = 'import_checkpoint.json'

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
            logging.info(f"加载了 {len(domains_first_seen)} 个域名的首次出现日期")
        except Exception as e:
            logging.error(f"加载域名首次出现日期时出错: {e}")
    
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
            
            logging.info(f"加载了 {len(domains_rankings)} 个域名的排名历史，跨越 {len(date_columns)} 个日期")
        except Exception as e:
            logging.error(f"加载域名排名历史时出错: {e}")
    
    return domains_rankings, domains_first_seen

def save_domains_history(domains_rankings, domains_first_seen, chunk_id=None):
    """将域名排名数据保存到Parquet文件，可选择保存为分块文件"""
    # 确定文件名
    first_seen_file = f"domains_first_seen_chunk_{chunk_id}.parquet" if chunk_id else DOMAINS_FIRST_SEEN_FILE
    rankings_file = f"domains_rankings_chunk_{chunk_id}.parquet" if chunk_id else DOMAINS_RANKINGS_FILE
    
    # 保存域名首次出现日期
    try:
        df_first_seen = pd.DataFrame([
            {'domain': domain, 'first_seen': first_seen}
            for domain, first_seen in domains_first_seen.items()
        ])
        df_first_seen.to_parquet(first_seen_file, index=False)
        logging.info(f"保存了 {len(domains_first_seen)} 个域名的首次出现日期到 {first_seen_file}")
    except Exception as e:
        logging.error(f"保存域名首次出现日期时出错: {e}")
    
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
        df_rankings.to_parquet(rankings_file, index=False)
        
        logging.info(f"保存了 {len(domains_rankings)} 个域名的排名历史到 {rankings_file}，跨越 {len(sorted_dates)} 个日期")
    except Exception as e:
        logging.error(f"保存域名排名历史时出错: {e}")

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

def save_checkpoint(checkpoint):
    """保存检查点信息"""
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)
        logging.info("检查点已更新")
    except Exception as e:
        logging.error(f"保存检查点文件时出错: {e}")

def process_csv_data(reader, date, domains_rankings, domains_first_seen):
    """处理CSV数据，更新域名排名和首次出现日期"""
    try:
        # 跳过标题行（如果有）
        first_row = next(reader, None)
        if first_row and not first_row[0].isdigit():
            data = list(reader)
        else:
            data = [first_row] + list(reader)
        
        # 当前日期中的所有域名
        current_domains = set()
        
        # 处理每一行
        for row in data:
            if len(row) >= 2:
                try:
                    rank = int(row[0].strip())
                    domain = row[1].strip()
                    current_domains.add(domain)
                    
                    # 更新排名历史
                    if domain not in domains_rankings:
                        domains_rankings[domain] = {}
                    domains_rankings[domain][date] = rank
                    
                    # 更新首次出现日期
                    if domain not in domains_first_seen or date < domains_first_seen[domain]:
                        domains_first_seen[domain] = date
                except (ValueError, IndexError) as e:
                    logging.warning(f"处理行 {row} 时出错: {e}")
        
        # 对于历史中存在但当前文件中不存在的域名，将其排名设为0
        for domain in domains_rankings:
            if domain not in current_domains and date not in domains_rankings[domain]:
                domains_rankings[domain][date] = 0
        
        return len(current_domains)
    except Exception as e:
        logging.error(f"处理CSV数据时出错: {e}")
        return 0

def process_commit_chunk(commit_chunk, start_index, chunk_id, batch_size=5000):
    """处理一组提交"""
    start_time = datetime.now()
    logging.info(f"开始处理数据块 {chunk_id}，包含 {len(commit_chunk)} 个提交")
    
    # 加载检查点
    checkpoint = load_checkpoint()
    processed_commits = set(checkpoint['processed_commits'])
    
    # 如果这个块已经处理过，直接返回
    if checkpoint['last_processed_chunk'] and int(checkpoint['last_processed_chunk']) >= chunk_id:
        logging.info(f"数据块 {chunk_id} 已处理过，跳过")
        return True
    
    # 加载现有数据
    domains_rankings, domains_first_seen = load_domains_history()
    logging.info(f"加载了 {len(domains_rankings)} 个域名的排名记录和 {len(domains_first_seen)} 个首次出现记录")
    
    # 处理每个提交
    total_processed = 0
    total_domains_updated = 0
    
    for i, (commit_hash, commit_date) in enumerate(commit_chunk):
        # 如果已经处理过这个提交，跳过
        if commit_hash in processed_commits:
            logging.info(f"提交 {commit_hash} ({commit_date}) 已处理过，跳过")
            continue
        
        logging.info(f"处理提交 {commit_hash} ({commit_date})，进度: {i+1}/{len(commit_chunk)}")
        
        # 检查提交目录是否存在
        commit_dir = os.path.join(HISTORICAL_DATA_DIR, commit_date)
        if not os.path.exists(commit_dir):
            logging.warning(f"提交目录不存在: {commit_dir}，跳过")
            continue
        
        # 检查zip文件或csv文件
        zip_file = os.path.join(commit_dir, "tranco.zip")
        csv_file = os.path.join(commit_dir, "top-1m.csv")
        
        domains_updated = 0
        
        if os.path.exists(zip_file):
            # 从zip文件读取数据
            try:
                with zipfile.ZipFile(zip_file, 'r') as z:
                    with z.open("top-1m.csv", 'r') as csvfile:
                        reader = csv.reader(codecs.getreader("utf-8")(csvfile))
                        domains_updated = process_csv_data(reader, commit_date, domains_rankings, domains_first_seen)
                logging.info(f"从zip文件处理了 {commit_date} 的数据，更新了 {domains_updated} 个域名")
            except Exception as e:
                logging.error(f"处理 {commit_date} 的zip文件时出错: {e}")
                continue
        elif os.path.exists(csv_file):
            # 直接从csv文件读取数据
            try:
                with open(csv_file, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    domains_updated = process_csv_data(reader, commit_date, domains_rankings, domains_first_seen)
                logging.info(f"从CSV文件处理了 {commit_date} 的数据，更新了 {domains_updated} 个域名")
            except Exception as e:
                logging.error(f"处理 {commit_date} 的CSV文件时出错: {e}")
                continue
        else:
            logging.warning(f"未找到 {commit_date} 的数据文件，跳过")
            continue
        
        # 更新处理状态
        processed_commits.add(commit_hash)
        total_processed += 1
        total_domains_updated += domains_updated
        
        # 每处理一定数量的提交，保存一次检查点
        if total_processed % 10 == 0 or i == len(commit_chunk) - 1:
            # 更新检查点
            checkpoint['processed_commits'] = list(processed_commits)
            checkpoint['last_processed_date'] = commit_date
            checkpoint['total_domains_updated'] = checkpoint.get('total_domains_updated', 0) + total_domains_updated
            save_checkpoint(checkpoint)
            
            # 输出进度
            elapsed_time = (datetime.now() - start_time).total_seconds()
            logging.info(f"已处理 {total_processed}/{len(commit_chunk)} 个提交，耗时 {elapsed_time:.2f} 秒")
            logging.info(f"总共更新了 {total_domains_updated} 个域名的排名")
    
    # 保存最终结果
    save_domains_history(domains_rankings, domains_first_seen, chunk_id)
    
    # 更新检查点
    checkpoint['last_processed_chunk'] = chunk_id
    checkpoint['total_domains_updated'] = checkpoint.get('total_domains_updated', 0) + total_domains_updated
    save_checkpoint(checkpoint)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logging.info(f"数据块 {chunk_id} 处理完成，总共处理了 {total_processed} 个提交，更新了 {total_domains_updated} 个域名")
    logging.info(f"处理时间: {duration:.2f} 秒")
    
    return True

def merge_chunk_results():
    """合并所有数据块的结果"""
    logging.info("开始合并所有数据块的结果...")
    
    try:
        # 合并排名数据
        ranking_files = [f for f in os.listdir('.') if f.startswith('domains_rankings_chunk_') and f.endswith('.parquet')]
        if ranking_files:
            dfs = []
            for file in ranking_files:
                logging.info(f"读取文件: {file}")
                df = pd.read_parquet(file)
                dfs.append(df)
            
            if dfs:
                # 合并所有DataFrame
                combined_df = pd.concat(dfs, ignore_index=True)
                
                # 处理重复的域名（取最新的数据）
                combined_df = combined_df.drop_duplicates(subset=['domain'], keep='last')
                
                logging.info(f"合并后的排名数据大小: {len(combined_df)} 行")
                combined_df.to_parquet(DOMAINS_RANKINGS_FILE)
                logging.info("排名数据已合并并保存")
        
        # 合并首次出现日期数据
        first_seen_files = [f for f in os.listdir('.') if f.startswith('domains_first_seen_chunk_') and f.endswith('.parquet')]
        if first_seen_files:
            dfs = []
            for file in first_seen_files:
                logging.info(f"读取文件: {file}")
                df = pd.read_parquet(file)
                dfs.append(df)
            
            if dfs:
                # 合并所有DataFrame
                combined_df = pd.concat(dfs, ignore_index=True)
                
                # 对于重复的域名，保留最早的首次出现日期
                combined_df = combined_df.sort_values('first_seen')
                combined_df = combined_df.drop_duplicates(subset=['domain'], keep='first')
                
                logging.info(f"合并后的首次出现日期数据大小: {len(combined_df)} 行")
                combined_df.to_parquet(DOMAINS_FIRST_SEEN_FILE)
                logging.info("首次出现日期数据已合并并保存")
                
                # 输出总共更新的域名数量
                logging.info(f"总共更新了 {len(combined_df)} 个域名的首次出现日期")
        
        return True
    except Exception as e:
        logging.error(f"合并结果时出错: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='导入历史域名排名数据')
    parser.add_argument('--batch-size', type=int, default=5000, help='每批处理的域名数量')
    parser.add_argument('--chunk-id', type=int, help='指定要处理的数据块ID')
    parser.add_argument('--start-date', type=str, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--merge-only', action='store_true', help='仅合并已有的数据块结果')
    args = parser.parse_args()
    
    start_time = datetime.now()
    logging.info(f"脚本开始执行时间: {start_time}")
    
    # 如果只需要合并结果
    if args.merge_only:
        merge_chunk_results()
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"脚本结束执行时间: {end_time}")
        logging.info(f"总执行时间: {duration}")
        return
    
    # 检查历史数据目录是否存在
    if not os.path.exists(HISTORICAL_DATA_DIR):
        logging.error(f"历史数据目录不存在: {HISTORICAL_DATA_DIR}")
        return
    
    # 获取所有日期目录
    commit_dirs = [d for d in os.listdir(HISTORICAL_DATA_DIR) if os.path.isdir(os.path.join(HISTORICAL_DATA_DIR, d))]
    
    # 按日期排序
    commit_dirs.sort()
    
    # 如果指定了日期范围，过滤目录
    if args.start_date:
        commit_dirs = [d for d in commit_dirs if d >= args.start_date]
    if args.end_date:
        commit_dirs = [d for d in commit_dirs if d <= args.end_date]
    
    logging.info(f"找到 {len(commit_dirs)} 个日期目录")
    
    # 将目录转换为(commit_hash, commit_date)格式
    # 这里我们假设目录名就是日期，commit_hash暂时用日期代替
    commits = [(d, d) for d in commit_dirs]
    
    # 将提交分成多个块
    chunk_size = 30  # 每个块处理30个提交
    commit_chunks = [commits[i:i+chunk_size] for i in range(0, len(commits), chunk_size)]
    
    logging.info(f"将 {len(commits)} 个提交分成 {len(commit_chunks)} 个数据块")
    
    # 如果指定了块ID，只处理该块
    if args.chunk_id is not None:
        if 0 <= args.chunk_id < len(commit_chunks):
            logging.info(f"只处理数据块 {args.chunk_id}")
            process_commit_chunk(commit_chunks[args.chunk_id], args.chunk_id * chunk_size, args.chunk_id, args.batch_size)
        else:
            logging.error(f"指定的数据块ID {args.chunk_id} 超出范围 (0-{len(commit_chunks)-1})")
    else:
        # 处理所有块
        for i, chunk in enumerate(commit_chunks):
            logging.info(f"处理数据块 {i+1}/{len(commit_chunks)}")
            process_commit_chunk(chunk, i * chunk_size, i+1, args.batch_size)
        
        # 合并所有块的结果
        merge_chunk_results()
    
    end_time = datetime.now()
    duration = end_time - start_time
    logging.info(f"脚本结束执行时间: {end_time}")
    logging.info(f"总执行时间: {duration}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"主程序执行出错: {e}")
        import traceback
        logging.error(traceback.format_exc())
        sys.exit(1)