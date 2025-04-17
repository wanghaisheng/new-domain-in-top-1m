import csv
import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import argparse

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 文件路径配置
DB_FILE = os.path.join('data', 'persisted-to-cache', 'domain_rank.db')
REPORT_DIR = os.path.join('reports')

def ensure_report_dir():
    """确保报告目录存在"""
    if not os.path.exists(REPORT_DIR):
        os.makedirs(REPORT_DIR)
        logging.info(f"创建报告目录: {REPORT_DIR}")

def load_rankings_data():
    """从SQLite数据库加载域名排名数据"""
    if not os.path.exists(DB_FILE):
        logging.error(f"数据库文件不存在: {DB_FILE}")
        return None
    
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # 获取当前年份的表名
        current_year = datetime.now().year
        table_name = f"rankings_{current_year}"
        
        # 获取所有列名
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]
        date_columns = [col for col in columns if col not in ['domain', 'last_updated']]
        
        if not date_columns:
            logging.error(f"表 {table_name} 中没有日期列")
            return None
        
        # 构建查询
        query = f"SELECT domain, {', '.join(date_columns)} FROM {table_name}"
        
        # 使用pandas读取SQL查询结果
        df = pd.read_sql_query(query, conn)
        logging.info(f"成功加载排名数据，共 {len(df)} 个域名")
        
        conn.close()
        return df
    except Exception as e:
        logging.error(f"加载排名数据失败: {e}")
        return None

def get_date_range(period_type):
    """获取日期范围"""
    today = datetime.now()
    end_date = today.strftime('%Y-%m-%d')
    
    if period_type == 'week':
        start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    elif period_type == 'month':
        start_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    else:
        raise ValueError(f"不支持的周期类型: {period_type}")
    
    return start_date, end_date

def calculate_rank_changes(df, start_date, end_date):
    """计算排名变化"""
    if start_date not in df.columns or end_date not in df.columns:
        available_dates = [col for col in df.columns if col != 'domain']
        available_dates.sort()
        
        if start_date not in df.columns:
            available_start_dates = [d for d in available_dates if d <= start_date]
            if available_start_dates:
                start_date = available_start_dates[-1]
                logging.info(f"使用最近的可用开始日期: {start_date}")
            else:
                logging.error(f"找不到合适的开始日期")
                return None
        
        if end_date not in df.columns:
            available_end_dates = [d for d in available_dates if d <= end_date]
            if available_end_dates:
                end_date = available_end_dates[-1]
                logging.info(f"使用最近的可用结束日期: {end_date}")
            else:
                logging.error(f"找不到合适的结束日期")
                return None
    
    result = pd.DataFrame()
    result['domain'] = df['domain']
    result['start_rank'] = df[start_date]
    result['end_rank'] = df[end_date]
    
    def calculate_change(row):
        start = row['start_rank']
        end = row['end_rank']
        
        if start == 0 and end > 0:
            return 1000000  # 新进入
        if start > 0 and end == 0:
            return -1000000  # 退出
        if start == 0 and end == 0:
            return 0
        return start - end
    
    result['rank_change'] = result.apply(calculate_change, axis=1)
    
    def calculate_percent(row):
        start = row['start_rank']
        end = row['end_rank']
        change = row['rank_change']
        
        if change == 1000000:
            return "新进入"
        if change == -1000000:
            return "退出排名"
        if start == 0 or end == 0:
            return "0%"
        
        percent = (start - end) / start * 100
        return f"{percent:.2f}%"
    
    result['change_percent'] = result.apply(calculate_percent, axis=1)
    return result

def generate_report(changes_df, period_type, start_date, end_date):
    """生成报告"""
    if changes_df is None or len(changes_df) == 0:
        logging.error("没有数据可生成报告")
        return
    
    ensure_report_dir()
    timestamp = datetime.now().strftime('%Y%m%d')
    period_name = "周报" if period_type == 'week' else "月报"
    
    meaningful_changes = changes_df[changes_df['rank_change'] != 0].copy()
    meaningful_changes['abs_change'] = meaningful_changes['rank_change'].abs()
    top_changes = meaningful_changes.sort_values('abs_change', ascending=False).head(100)
    
    rising_domains = changes_df[changes_df['rank_change'] > 0].sort_values('rank_change', ascending=False).head(50)
    falling_domains = changes_df[changes_df['rank_change'] < 0].sort_values('rank_change', ascending=True).head(50)
    
    try:
        # 保存CSV报告
        report_file = os.path.join(REPORT_DIR, f"域名排名变化{period_name}_{timestamp}.csv")
        top_changes[['domain', 'start_rank', 'end_rank', 'rank_change', 'change_percent']].to_csv(
            report_file, index=False, encoding='utf-8'
        )
        
        rising_file = os.path.join(REPORT_DIR, f"排名上升域名{period_name}_{timestamp}.csv")
        rising_domains[['domain', 'start_rank', 'end_rank', 'rank_change', 'change_percent']].to_csv(
            rising_file, index=False, encoding='utf-8'
        )
        
        falling_file = os.path.join(REPORT_DIR, f"排名下降域名{period_name}_{timestamp}.csv")
        falling_domains[['domain', 'start_rank', 'end_rank', 'rank_change', 'change_percent']].to_csv(
            falling_file, index=False, encoding='utf-8'
        )
        
        # 生成可视化图表
        generate_visualization(rising_domains, falling_domains, period_type, timestamp)
        
        logging.info(f"已保存所有报告到 {REPORT_DIR}")
    except Exception as e:
        logging.error(f"保存报告失败: {e}")

def generate_visualization(rising_domains, falling_domains, period_type, timestamp):
    """生成可视化图表"""
    try:
        period_name = "周报" if period_type == 'week' else "月报"
        charts_dir = os.path.join(REPORT_DIR, 'charts')
        if not os.path.exists(charts_dir):
            os.makedirs(charts_dir)
        
        # 上升域名图表
        plt.figure(figsize=(12, 8))
        top10_rising = rising_domains.head(10)
        plt.barh(top10_rising['domain'], top10_rising['rank_change'], color='green')
        plt.xlabel('排名提升')
        plt.ylabel('域名')
        plt.title(f'排名上升Top10域名 - {period_name}')
        plt.tight_layout()
        plt.savefig(os.path.join(charts_dir, f'top_rising_{period_type}_{timestamp}.png'))
        plt.close()
        
        # 下降域名图表
        plt.figure(figsize=(12, 8))
        top10_falling = falling_domains.head(10)
        plt.barh(top10_falling['domain'], top10_falling['rank_change'], color='red')
        plt.xlabel('排名下降')
        plt.ylabel('域名')
        plt.title(f'排名下降Top10域名 - {period_name}')
        plt.tight_layout()
        plt.savefig(os.path.join(charts_dir, f'top_falling_{period_type}_{timestamp}.png'))
        plt.close()
        
        logging.info("已生成可视化图表")
    except Exception as e:
        logging.error(f"生成可视化图表失败: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='分析域名排名变化并生成报告')
    parser.add_argument('--period', choices=['week', 'month', 'both'], default='both',
                      help='指定生成报告的周期: week, month, 或 both (默认)')
    args = parser.parse_args()
    
    df = load_rankings_data()
    if df is None:
        exit(1)
    
    if args.period in ['week', 'both']:
        start_date_week, end_date_week = get_date_range('week')
        logging.info(f"生成周报，时间范围: {start_date_week} 至 {end_date_week}")
        weekly_changes = calculate_rank_changes(df, start_date_week, end_date_week)
        generate_report(weekly_changes, 'week', start_date_week, end_date_week)
    
    if args.period in ['month', 'both']:
        start_date_month, end_date_month = get_date_range('month')
        logging.info(f"生成月报，时间范围: {start_date_month} 至 {end_date_month}")
        monthly_changes = calculate_rank_changes(df, start_date_month, end_date_month)
        generate_report(monthly_changes, 'month', start_date_month, end_date_month)
    
    logging.info("域名排名变化分析完成")