#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

def create_prepare_commits_script():
    """创建准备提交列表的脚本"""
    script = """#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import argparse
import requests
from datetime import datetime
import time

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='准备提交列表')
    parser.add_argument('--start-date', type=str, required=False, default='2024-06-08',
                        help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=False, default='',
                        help='结束日期 (YYYY-MM-DD)')
    args = parser.parse_args()
    
    # 设置日期范围
    start_date = args.start_date
    end_date = args.end_date if args.end_date else datetime.now().strftime('%Y-%m-%d')
    
    print(f"获取 {start_date} 到 {end_date} 之间的提交")
    
    # 创建历史数据目录
    os.makedirs('historical_extracts', exist_ok=True)
    
    # 使用GitHub API获取提交列表
    repo = "adysec/top_1m_domains"
    page = 1
    per_page = 100
    all_commits = []
    
    while True:
        url = f"https://api.github.com/repos/{repo}/commits?per_page={per_page}&page={page}"
        response = requests.get(url)
        
        if response.status_code != 200:
            print(f"API请求失败: {response.status_code}")
            print(response.text)
            sys.exit(1)
        
        commits = response.json()
        
        # 检查是否为空数组
        if not commits:
            break
        
        # 提取提交信息
        for commit in commits:
            sha = commit['sha']
            date = commit['commit']['committer']['date'][:10]  # 只取日期部分
            all_commits.append((sha, date))
        
        page += 1
        
        # 避免API限制
        time.sleep(1)
    
    # 过滤日期范围内的提交并排序
    filtered_commits = []
    for sha, date in all_commits:
        if start_date <= date <= end_date:
            filtered_commits.append((sha, date))
    
    # 按日期排序（从早到晚）
    filtered_commits.sort(key=lambda x: x[1])
    
    # 写入文件
    with open('historical_commits.txt', 'w') as f:
        for sha, date in filtered_commits:
            f.write(f"{sha} {date}\\n")
    
    # 检查提交记录数量
    commit_count = len(filtered_commits)
    print(f"找到 {commit_count} 条提交记录")
    
    if commit_count < 1:
        print("错误：没有找到指定日期范围内的提交记录")
        sys.exit(1)
    
    print("提交列表准备完成")

if __name__ == "__main__":
    main()
"""
    
    # 写入文件
    with open('prepare_commits.py', 'w', encoding='utf-8') as f:
        # 修复换行符问题 - 使用正则表达式替换所有的转义序列
        import re
        fixed_script = re.sub(r'\\n', '\n', script)
        # 特别处理f-string中的换行符问题
        fixed_script = fixed_script.replace('{sha} {date}\\n', '{sha} {date}\\n')
        f.write(fixed_script)
    
    print("已创建 prepare_commits.py 脚本")

def create_process_commits_script():
    """创建处理提交的脚本"""
    script = """#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import subprocess
import time
from datetime import datetime

def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='处理提交列表')
    parser.add_argument('--max-commits', type=int, default=50,
                        help='单次运行最大处理提交数')
    args = parser.parse_args()
    
    # 如果没有提交记录，跳过处理
    if not os.path.exists('historical_commits.txt') or os.path.getsize('historical_commits.txt') == 0:
        print("没有新的提交记录需要处理")
        sys.exit(0)
    
    # 检查是否存在上次处理的日期记录
    last_processed_date = ""
    if os.path.exists("last_processed_date.txt"):
        with open("last_processed_date.txt", 'r') as f:
            last_processed_date = f.read().strip()
        print(f"发现上次处理记录，将从 {last_processed_date} 之后继续处理")
    
    # 创建临时文件存储待处理的提交
    with open('historical_commits.txt', 'r') as f:
        all_commits = [line.strip().split() for line in f if line.strip()]
    
    # 过滤出上次处理日期之后的提交
    if last_processed_date:
        commits_to_process = [commit for commit in all_commits if commit[1] > last_processed_date]
        print(f"过滤出 {len(commits_to_process)} 条待处理的提交")
    else:
        commits_to_process = all_commits
    
    # 每处理N个提交保存一次进度，避免单次运行时间过长
    commit_count = 0
    max_commits_per_run = args.max_commits
    
    for commit_hash, commit_date in commits_to_process:
        print(f"处理 {commit_date} 的提交 {commit_hash}")
        
        # 直接构造URL下载zip文件
        zip_url = f"https://github.com/adysec/top_1m_domains/raw/{commit_hash}/tranco.zip"
        target_dir = f"historical_extracts/{commit_date}"
        os.makedirs(target_dir, exist_ok=True)
        
        print(f"从 {zip_url} 下载文件")
        download_cmd = f'curl -L -o "{target_dir}/tranco.zip" "{zip_url}"'
        
        try:
            # 下载文件
            result = subprocess.run(download_cmd, shell=True, check=True)
            print("下载成功，开始处理数据")
            
            # 直接处理这个提交
            process_cmd = f'python -c "import sys; sys.path.append(\\'.\\'); from import_historical_data import process_single_commit; process_single_commit(\\'{commit_hash}\\', \\'{commit_date}\\')"'
            result = subprocess.run(process_cmd, shell=True)
            
            # 检查处理结果
            if result.returncode == 0:
                print(f"成功处理 {commit_date} 的数据")
                # 更新最后处理的日期
                last_processed_date = commit_date
                # 保存最后处理的日期到文件
                with open("last_processed_date.txt", 'w') as f:
                    f.write(last_processed_date)
                
                # 增加计数器
                commit_count += 1
                
                # 检查是否达到单次运行的最大提交数
                if commit_count >= max_commits_per_run:
                    print(f"已处理 {max_commits_per_run} 个提交，本次运行结束")
                    print("请再次运行脚本继续处理剩余提交")
                    break
            else:
                print(f"处理 {commit_date} 的数据失败")
        except subprocess.CalledProcessError:
            print(f"下载 {commit_hash} ({commit_date}) 的 tranco.zip 文件失败")
        except Exception as e:
            print(f"处理过程中发生错误: {e}")
        finally:
            # 删除已处理的ZIP文件，节省空间
            if os.path.exists(f"{target_dir}/tranco.zip"):
                os.remove(f"{target_dir}/tranco.zip")
    
    print(f"历史数据处理完成，最后处理的日期: {last_processed_date}")
    print(f"共处理了 {commit_count} 个提交")
    
    # 检查是否还有未处理的提交
    if last_processed_date:
        remaining_commits = len([commit for commit in all_commits if commit[1] > last_processed_date])
        if remaining_commits > 0:
            print(f"还有 {remaining_commits} 个提交未处理，请再次运行脚本")
        else:
            print("所有提交已处理完成")

if __name__ == "__main__":
    main()
"""
    
    # 写入文件
    with open('process_commits.py', 'w', encoding='utf-8') as f:
        f.write(script)
    
    print("已创建 process_commits.py 脚本")

def create_import_data_script():
    """创建导入历史数据的脚本"""
    script = """#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import subprocess
import time
from datetime import datetime

def main():
    print("开始导入历史数据...")
    
    # 运行导入脚本
    try:
        result = subprocess.run('python import_historical_data.py', shell=True, check=True)
        print("历史数据导入完成")
    except subprocess.CalledProcessError as e:
        print(f"导入历史数据失败: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"导入过程中发生错误: {e}")
        sys.exit(1)
    
    # 提交更改
    try:
        # 配置Git
        subprocess.run('git config --local user.email "actions@github.com"', shell=True, check=True)
        subprocess.run('git config --local user.name "GitHub Actions"', shell=True, check=True)
        
        # 添加文件
        subprocess.run('git add domains_rankings.parquet domains_first_seen.parquet', shell=True, check=True)
        
        # 提交更改
        result = subprocess.run('git commit -m "Import historical domain rank data"', shell=True)
        if result.returncode != 0:
            print("没有更改需要提交")
        else:
            print("更改已提交")
            
            # 推送更改
            subprocess.run('git push', shell=True, check=True)
            print("更改已推送到远程仓库")
    except subprocess.CalledProcessError as e:
        print(f"提交更改失败: {e}")
    except Exception as e:
        print(f"提交过程中发生错误: {e}")

if __name__ == "__main__":
    main()
"""
    
    # 写入文件
    with open('import_data.py', 'w', encoding='utf-8') as f:
        f.write(script)
    
    print("已创建 import_data.py 脚本")

# 在文件末尾添加主函数
def main():
    """创建所有辅助脚本"""
    create_prepare_commits_script()
    create_process_commits_script()
    create_import_data_script()
    print("所有辅助脚本创建完成")

if __name__ == "__main__":
    main()