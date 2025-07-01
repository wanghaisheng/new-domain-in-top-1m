import csv
import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import argparse

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REPORT_DIR = os.path.join('reports')
BACKUP_DIR = 'domains_rankings_backup'


def ensure_report_dir():
    """确保报告目录存在"""
    if not os.path.exists(REPORT_DIR):
        os.makedirs(REPORT_DIR)
        logging.info(f"创建报告目录: {REPORT_DIR}")

def load_rankings_data():
    """从 domains_rankings_backup 目录下所有宽表分割 CSV 文件加载域名排名数据，并加载首次出现日期"""
    if not os.path.exists(BACKUP_DIR):
        logging.error(f"备份目录不存在: {BACKUP_DIR}")
        return None
    all_data = {}
    date_set = set()
    # 加载宽表分片
    for fname in os.listdir(BACKUP_DIR):
        if fname.startswith('domains_rankings_') and fname.endswith('.csv'):
            path = os.path.join(BACKUP_DIR, fname)
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    date_cols = header[1:]
                    date_set.update(date_cols)
                    for row in reader:
                        if len(row) < 2:
                            continue
                        domain = row[0]
                        if domain not in all_data:
                            all_data[domain] = {}
                        for idx, date_col in enumerate(date_cols):
                            if idx+1 < len(row):
                                try:
                                    rank = int(row[idx+1]) if row[idx+1] else 0
                                except:
                                    rank = 0
                                all_data[domain][date_col] = rank
            except Exception as e:
                logging.error(f"读取备份文件 {fname} 失败: {e}")
    # 加载首次出现日期
    first_seen_dict = {}
    first_seen_file = os.path.join(BACKUP_DIR, 'domains_first_seen.csv')
    if os.path.exists(first_seen_file):
        try:
            with open(first_seen_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    if len(row) >= 2:
                        first_seen_dict[row[0]] = row[1]
        except Exception as e:
            logging.error(f"读取首次出现日期失败: {e}")
    if not all_data or not date_set:
        logging.error("没有有效的域名排名数据")
        return None
    sorted_dates = sorted(list(date_set))
    data = {'domain': []}
    for d in sorted_dates:
        data[d] = []
    data['first_seen'] = []
    for domain, date_ranks in all_data.items():
        data['domain'].append(domain)
        for d in sorted_dates:
            data[d].append(date_ranks.get(d, 0))
        data['first_seen'].append(first_seen_dict.get(domain, ''))
    df = pd.DataFrame(data)
    logging.info(f"成功加载排名数据，共 {len(df)} 个域名，{len(sorted_dates)} 个日期")
    return df

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

def generate_report_top100(changes_df, period_type, start_date, end_date):
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
def generate_report(changes_df, period_type, start_date, end_date):
    """生成报告"""
    if changes_df is None or len(changes_df) == 0:
        logging.error("没有数据可生成报告")
        return

    ensure_report_dir()
    timestamp = datetime.now().strftime('%Y%m%d')
    period_name = "周报" if period_type == 'week' else "月报"

    # 所有变化（不为0）
    meaningful_changes = changes_df[changes_df['rank_change'] != 0].copy()
    meaningful_changes['abs_change'] = meaningful_changes['rank_change'].abs()
    sorted_changes = meaningful_changes.sort_values('abs_change', ascending=False)

    # 保存完整数据
    full_file = os.path.join(REPORT_DIR, f"完整排名变化_{period_name}_{timestamp}.csv")
    sorted_changes[['domain', 'start_rank', 'end_rank', 'rank_change', 'change_percent']].to_csv(
        full_file, index=False, encoding='utf-8'
    )
    logging.info(f"保存完整排名变化文件：{full_file}")

    # 分区保存文件
    ranges = [(0, 100), (100, 500), (500, 1000)]
    for r_start, r_end in ranges:
        sub_df = sorted_changes.iloc[r_start:r_end]
        if not sub_df.empty:
            filename = f"排名变化_{period_name}_{timestamp}_{r_start}_{r_end}.csv"
            sub_file = os.path.join(REPORT_DIR, filename)
            sub_df[['domain', 'start_rank', 'end_rank', 'rank_change', 'change_percent']].to_csv(
                sub_file, index=False, encoding='utf-8'
            )
            logging.info(f"保存排名变化区间文件：{sub_file}")

    # 超过1000的部分
    if len(sorted_changes) > 1000:
        extra_df = sorted_changes.iloc[1000:]
        filename = f"排名变化_{period_name}_{timestamp}_1000_plus.csv"
        extra_file = os.path.join(REPORT_DIR, filename)
        extra_df[['domain', 'start_rank', 'end_rank', 'rank_change', 'change_percent']].to_csv(
            extra_file, index=False, encoding='utf-8'
        )
        logging.info(f"保存排名变化1000+文件：{extra_file}")

    # 保留原 Top 50 上升/下降 + 图表
    rising_domains = changes_df[changes_df['rank_change'] > 0].sort_values('rank_change', ascending=False).head(50)
    falling_domains = changes_df[changes_df['rank_change'] < 0].sort_values('rank_change', ascending=True).head(50)

    rising_file = os.path.join(REPORT_DIR, f"排名上升域名{period_name}_{timestamp}.csv")
    falling_file = os.path.join(REPORT_DIR, f"排名下降域名{period_name}_{timestamp}.csv")

    rising_domains[['domain', 'start_rank', 'end_rank', 'rank_change', 'change_percent']].to_csv(
        rising_file, index=False, encoding='utf-8'
    )
    falling_domains[['domain', 'start_rank', 'end_rank', 'rank_change', 'change_percent']].to_csv(
        falling_file, index=False, encoding='utf-8'
    )

    generate_visualization(rising_domains, falling_domains, period_type, timestamp)

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
