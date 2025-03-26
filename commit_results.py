#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def run_command(cmd, check=True):
    """运行命令并返回结果"""
    logging.info(f"执行命令: {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, check=check, capture_output=True, text=True)
        if result.stdout:
            logging.info(result.stdout)
        if result.stderr:
            logging.error(result.stderr)
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"命令执行失败: {e}")
        if e.stdout:
            logging.info(e.stdout)
        if e.stderr:
            logging.error(e.stderr)
        if check:
            raise
        return e.returncode

def main():
    """主函数"""
    start_time = datetime.now()
    logging.info(f"脚本开始执行时间: {start_time}")
    
    # 检查结果文件是否存在
    files_to_check = ['domains_rankings.parquet', 'domains_first_seen.parquet']
    files_exist = True
    
    for file_path in files_to_check:
        if not os.path.exists(file_path):
            logging.error(f"结果文件不存在: {file_path}")
            files_exist = False
        else:
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                logging.error(f"结果文件为空: {file_path}")
                files_exist = False
            else:
                logging.info(f"结果文件正常: {file_path}, 大小: {file_size} 字节")
    
    if not files_exist:
        logging.error("缺少必要的结果文件，跳过Git操作")
        sys.exit(1)
    
    # 提交更改
    try:
        # 配置Git
        logging.info("配置Git用户信息...")
        run_command('git config --local user.email "actions@github.com"')
        run_command('git config --local user.name "GitHub Actions"')
        
        # 检查Git状态
        logging.info("检查Git状态...")
        status_result = run_command('git status', check=False)
        
        # 添加文件
        logging.info("添加文件到Git...")
        add_result = run_command('git add domains_rankings.parquet domains_first_seen.parquet', check=False)
        if add_result.returncode == 0:
            logging.info("文件添加成功")
        else:
            logging.error(f"文件添加失败: {add_result.stderr}")
        
        # 提交更改
        logging.info("提交更改到Git...")
        commit_result = run_command('git commit -m "Import historical domain rank data"', check=False)
        if commit_result.returncode == 0:
            logging.info("更改已提交")
            
            # 推送更改
            logging.info("推送更改到远程仓库...")
            push_result = run_command('git push', check=False)
            if push_result.returncode == 0:
                logging.info("更改已推送到远程仓库")
            else:
                logging.error(f"推送失败: {push_result.stderr}")
        else:
            logging.info(f"没有更改需要提交或提交失败: {commit_result.stdout}")
            if commit_result.stderr:
                logging.info(f"提交错误: {commit_result.stderr}")
    except Exception as e:
        logging.error(f"Git操作过程中发生错误: {e}")
        import traceback
        logging.error(traceback.format_exc())
    
    end_time = datetime.now()
    duration = end_time - start_time
    logging.info(f"脚本结束执行时间: {end_time}")
    logging.info(f"总执行时间: {duration}")
    logging.info("提交过程完成")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"主程序执行出错: {e}")
        import traceback
        logging.error(traceback.format_exc())
        sys.exit(1)