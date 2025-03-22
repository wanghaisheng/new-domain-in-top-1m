import csv
import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict
import json
import argparse

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 文件路径 - 修改为支持 GitHub Actions 环境
github_workspace = os.environ.get("GITHUB_WORKSPACE", ".")  # 获取 GitHub 工作目录，默认为当前目录
DOMAINS_RANKINGS_FILE = os.path.join(github_workspace, 'domains_rankings.csv')
REPORT_DIR = os.path.join(github_workspace, 'reports')
DOMAINS_FIRST_SEEN_FILE = os.path.join(github_workspace, 'domains_first_seen.csv')

def ensure_report_dir():
    """确保报告目录存在"""
    if not os.path.exists(REPORT_DIR):
        os.makedirs(REPORT_DIR)
        logging.info(f"创建报告目录: {REPORT_DIR}")

def load_rankings_data():
    """加载域名排名数据"""
    if not os.path.exists(DOMAINS_RANKINGS_FILE):
        logging.error(f"排名数据文件不存在: {DOMAINS_RANKINGS_FILE}")
        return None
    
    try:
        # 使用pandas读取CSV文件，更高效处理大数据
        df = pd.read_csv(DOMAINS_RANKINGS_FILE)
        logging.info(f"成功加载排名数据，共 {len(df)} 个域名")
        return df
    except Exception as e:
        logging.error(f"加载排名数据失败: {e}")
        return None

def get_date_range(period_type):
    """获取日期范围
    
    Args:
        period_type: 'week' 或 'month'
    
    Returns:
        tuple: (start_date, end_date) 格式为 'YYYY-MM-DD'
    """
    today = datetime.now()
    end_date = today.strftime('%Y-%m-%d')
    
    if period_type == 'week':
        # 计算过去7天的日期范围
        start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    elif period_type == 'month':
        # 计算过去30天的日期范围
        start_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    else:
        raise ValueError(f"不支持的周期类型: {period_type}")
    
    return start_date, end_date

def get_custom_date_range(start_date, end_date):
    """获取自定义日期范围
    
    Args:
        start_date: 开始日期，格式为 'YYYY-MM-DD'
        end_date: 结束日期，格式为 'YYYY-MM-DD'
    
    Returns:
        tuple: (start_date, end_date)
    """
    # 验证日期格式
    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        logging.error(f"日期格式错误，应为 YYYY-MM-DD")
        return None, None
    
    # 确保开始日期不晚于结束日期
    if start_date > end_date:
        logging.warning(f"开始日期 {start_date} 晚于结束日期 {end_date}，将交换两个日期")
        start_date, end_date = end_date, start_date
    
    return start_date, end_date

def calculate_rank_changes(df, start_date, end_date):
    """计算排名变化
    
    Args:
        df: 包含排名数据的DataFrame
        start_date: 开始日期
        end_date: 结束日期
    
    Returns:
        DataFrame: 包含排名变化的数据
    """
    # 检查日期列是否存在
    if start_date not in df.columns or end_date not in df.columns:
        available_dates = [col for col in df.columns if col != 'domain']
        available_dates.sort()
        
        # 如果指定的日期不存在，尝试找到最近的日期
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
    
    # 计算排名变化
    # 注意：排名为0表示域名不在排名中，我们需要特殊处理
    # 创建一个新的DataFrame来存储结果
    result = pd.DataFrame()
    result['domain'] = df['domain']
    
    # 复制开始和结束日期的排名
    result['start_rank'] = df[start_date]
    result['end_rank'] = df[end_date]
    
    # 计算排名变化（上升为正，下降为负）
    # 对于排名为0的情况需要特殊处理
    def calculate_change(row):
        start = row['start_rank']
        end = row['end_rank']
        
        # 如果开始时不在排名中，结束时在排名中，视为新进入
        if start == 0 and end > 0:
            return 1000000  # 使用一个大数表示新进入
        
        # 如果开始时在排名中，结束时不在排名中，视为退出
        if start > 0 and end == 0:
            return -1000000  # 使用一个大负数表示退出
        
        # 如果都不在排名中，变化为0
        if start == 0 and end == 0:
            return 0
        
        # 正常计算排名变化（注意排名数字越小表示排名越高）
        return start - end
    
    result['rank_change'] = result.apply(calculate_change, axis=1)
    
    # 计算排名变化百分比
    def calculate_percent(row):
        start = row['start_rank']
        end = row['end_rank']
        change = row['rank_change']
        
        # 特殊情况处理
        if change == 1000000:  # 新进入
            return "新进入"
        if change == -1000000:  # 退出
            return "退出排名"
        if start == 0 or end == 0:
            return "0%"
        
        # 计算百分比变化
        percent = (start - end) / start * 100
        return f"{percent:.2f}%"
    
    result['change_percent'] = result.apply(calculate_percent, axis=1)
    
    return result

def generate_report(changes_df, period_type, start_date, end_date):
    """生成报告
    
    Args:
        changes_df: 包含排名变化的DataFrame
        period_type: 'week' 或 'month'
        start_date: 开始日期
        end_date: 结束日期
    """
    if changes_df is None or len(changes_df) == 0:
        logging.error("没有数据可生成报告")
        return
    
    # 确保报告目录存在
    ensure_report_dir()
    
    # 生成报告文件名
    timestamp = datetime.now().strftime('%Y%m%d')
    period_name = "周报" if period_type == 'week' else "月报"
    
    # 使用 os.path.join 构建文件路径，与主脚本保持一致
    report_file = os.path.join(REPORT_DIR, f"域名排名变化{period_name}_{timestamp}.csv")
    
    # 筛选有意义的变化（排除变化为0的记录）
    meaningful_changes = changes_df[changes_df['rank_change'] != 0].copy()
    
    # 按排名变化绝对值排序（取前100名变化最大的）
    meaningful_changes['abs_change'] = meaningful_changes['rank_change'].abs()
    top_changes = meaningful_changes.sort_values('abs_change', ascending=False).head(100)
    
    # 分别获取上升最多和下降最多的域名
    rising_domains = changes_df[changes_df['rank_change'] > 0].sort_values('rank_change', ascending=False).head(50)
    falling_domains = changes_df[changes_df['rank_change'] < 0].sort_values('rank_change', ascending=True).head(50)
    
    # 保存CSV报告
    try:
        # 保存总体变化报告
        top_changes[['domain', 'start_rank', 'end_rank', 'rank_change', 'change_percent']].to_csv(
            report_file, index=False, encoding='utf-8'
        )
        logging.info(f"已保存排名变化报告: {report_file}")
        
        # 保存上升域名报告
        rising_file = os.path.join(REPORT_DIR, f"排名上升域名{period_name}_{timestamp}.csv")
        rising_domains[['domain', 'start_rank', 'end_rank', 'rank_change', 'change_percent']].to_csv(
            rising_file, index=False, encoding='utf-8'
        )
        logging.info(f"已保存排名上升域名报告: {rising_file}")
        
        # 保存下降域名报告
        falling_file = os.path.join(REPORT_DIR, f"排名下降域名{period_name}_{timestamp}.csv")
        falling_domains[['domain', 'start_rank', 'end_rank', 'rank_change', 'change_percent']].to_csv(
            falling_file, index=False, encoding='utf-8'
        )
        logging.info(f"已保存排名下降域名报告: {falling_file}")
        
        # 生成可视化图表
        generate_visualization(rising_domains, falling_domains, period_type, timestamp)
        
    except Exception as e:
        logging.error(f"保存报告失败: {e}")

def generate_visualization(rising_domains, falling_domains, period_type, timestamp):
    """生成可视化图表
    
    Args:
        rising_domains: 排名上升的域名DataFrame
        falling_domains: 排名下降的域名DataFrame
        period_type: 'week' 或 'month'
        timestamp: 时间戳字符串
    """
    try:
        period_name = "周报" if period_type == 'week' else "月报"
        
        # 创建图表目录
        charts_dir = os.path.join(REPORT_DIR, 'charts')
        if not os.path.exists(charts_dir):
            os.makedirs(charts_dir)
        
        # 绘制排名上升Top10域名图表
        plt.figure(figsize=(12, 8))
        top10_rising = rising_domains.head(10)
        plt.barh(top10_rising['domain'], top10_rising['rank_change'], color='green')
        plt.xlabel('排名提升')
        plt.ylabel('域名')
        plt.title(f'排名上升Top10域名 - {period_name}')
        plt.tight_layout()
        plt.savefig(os.path.join(charts_dir, f'top_rising_{period_type}_{timestamp}.png'))
        plt.close()
        
        # 绘制排名下降Top10域名图表
        plt.figure(figsize=(12, 8))
        top10_falling = falling_domains.head(10)
        plt.barh(top10_falling['domain'], top10_falling['rank_change'], color='red')
        plt.xlabel('排名下降')
        plt.ylabel('域名')
        plt.title(f'排名下降Top10域名 - {period_name}')
        plt.tight_layout()
        plt.savefig(os.path.join(charts_dir, f'top_falling_{period_type}_{timestamp}.png'))
        plt.close()
        
        logging.info(f"已生成可视化图表")
    except Exception as e:
        logging.error(f"生成可视化图表失败: {e}")

def main():
    """主函数"""
    logging.info("开始分析域名排名变化")
    
    # 加载排名数据
    df = load_rankings_data()
    if df is None:
        return
    
    # 生成周报
    start_date_week, end_date_week = get_date_range('week')
    logging.info(f"生成周报，时间范围: {start_date_week} 至 {end_date_week}")
    weekly_changes = calculate_rank_changes(df, start_date_week, end_date_week)
    generate_report(weekly_changes, 'week', start_date_week, end_date_week)
    
    # 生成月报
    start_date_month, end_date_month = get_date_range('month')
    logging.info(f"生成月报，时间范围: {start_date_month} 至 {end_date_month}")
    monthly_changes = calculate_rank_changes(df, start_date_month, end_date_month)
    generate_report(monthly_changes, 'month', start_date_month, end_date_month)
    
    logging.info("域名排名变化分析完成")

def check_and_run_scheduled_reports():
    """检查是否需要运行定期报告"""
    # 创建记录文件目录
    if not os.path.exists(REPORT_DIR):
        os.makedirs(REPORT_DIR)
    
    last_run_file = os.path.join(REPORT_DIR, 'last_run.json')
    today = datetime.now()
    
    # 默认上次运行时间
    last_run = {
        'weekly': None,
        'monthly': None
    }
    
    # 加载上次运行时间
    if os.path.exists(last_run_file):
        try:
            with open(last_run_file, 'r') as f:
                data = json.load(f)
                if 'weekly' in data:
                    last_run['weekly'] = datetime.strptime(data['weekly'], '%Y-%m-%d')
                if 'monthly' in data:
                    last_run['monthly'] = datetime.strptime(data['monthly'], '%Y-%m-%d')
        except Exception as e:
            logging.error(f"读取上次运行时间失败: {e}")
    
    # 检查是否需要运行周报
    run_weekly = False
    if last_run['weekly'] is None:
        run_weekly = True
    else:
        days_since_last_weekly = (today - last_run['weekly']).days
        if days_since_last_weekly >= 7:  # 每7天运行一次
            run_weekly = True
    
    # 检查是否需要运行月报
    run_monthly = False
    if last_run['monthly'] is None:
        run_monthly = True
    else:
        days_since_last_monthly = (today - last_run['monthly']).days
        if days_since_last_monthly >= 30:  # 每30天运行一次
            run_monthly = True
    
    # 加载数据
    df = None
    if run_weekly or run_monthly:
        df = load_rankings_data()
        if df is None:
            return
    
    # 运行周报
    if run_weekly:
        start_date_week, end_date_week = get_date_range('week')
        logging.info(f"定期生成周报，时间范围: {start_date_week} 至 {end_date_week}")
        weekly_changes = calculate_rank_changes(df, start_date_week, end_date_week)
        generate_report(weekly_changes, 'week', start_date_week, end_date_week)
        last_run['weekly'] = today
    
    # 运行月报
    if run_monthly:
        start_date_month, end_date_month = get_date_range('month')
        logging.info(f"定期生成月报，时间范围: {start_date_month} 至 {end_date_month}")
        monthly_changes = calculate_rank_changes(df, start_date_month, end_date_month)
        generate_report(monthly_changes, 'month', start_date_month, end_date_month)
        last_run['monthly'] = today
    
    # 保存运行时间
    if run_weekly or run_monthly:
        try:
            with open(last_run_file, 'w') as f:
                json.dump({
                    'weekly': last_run['weekly'].strftime('%Y-%m-%d') if last_run['weekly'] else None,
                    'monthly': last_run['monthly'].strftime('%Y-%m-%d') if last_run['monthly'] else None
                }, f)
        except Exception as e:
            logging.error(f"保存运行时间失败: {e}")

# 在 if __name__ == "__main__": 部分添加命令行参数处理
if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='分析域名排名变化并生成报告')
    parser.add_argument('--period', choices=['weekly', 'monthly', 'both', 'custom'], default='both',
                        help='指定生成报告的周期: weekly, monthly, custom, 或 both (默认)')
    parser.add_argument('--start-date', type=str, help='自定义分析的开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='自定义分析的结束日期 (YYYY-MM-DD)')
    parser.add_argument('--report-name', type=str, default='自定义报告', help='自定义报告名称')
    args = parser.parse_args()
    
    logging.info(f"开始分析域名排名变化，周期: {args.period}")
    
    # 加载排名数据
    df = load_rankings_data()
    if df is None:
        exit(1)
    
    # 根据指定的周期生成报告
    if args.period in ['weekly', 'both']:
        # 生成周报 - 分析过去7天的数据
        start_date_week, end_date_week = get_date_range('week')
        logging.info(f"生成周报，时间范围: {start_date_week} 至 {end_date_week}")
        weekly_changes = calculate_rank_changes(df, start_date_week, end_date_week)
        generate_report(weekly_changes, 'week', start_date_week, end_date_week)
    
    if args.period in ['monthly', 'both']:
        # 生成月报 - 分析过去30天的数据
        start_date_month, end_date_month = get_date_range('month')
        logging.info(f"生成月报，时间范围: {start_date_month} 至 {end_date_month}")
        monthly_changes = calculate_rank_changes(df, start_date_month, end_date_month)
        generate_report(monthly_changes, 'month', start_date_month, end_date_month)
    
    if args.period == 'custom':
        # 检查是否提供了自定义日期范围
        if not args.start_date or not args.end_date:
            logging.error("使用自定义周期时必须提供开始日期和结束日期")
            exit(1)
        
        # 获取自定义日期范围
        start_date, end_date = get_custom_date_range(args.start_date, args.end_date)
        if start_date is None or end_date is None:
            exit(1)
        
        logging.info(f"生成自定义报告，时间范围: {start_date} 至 {end_date}")
        custom_changes = calculate_rank_changes(df, start_date, end_date)
        
        # 使用自定义报告名称
        report_name = args.report_name
        
        # 生成自定义报告
        # 确保报告目录存在
        ensure_report_dir()
        
        # 生成报告文件名
        timestamp = datetime.now().strftime('%Y%m%d')
        
        # 使用 os.path.join 构建文件路径
        report_file = os.path.join(REPORT_DIR, f"{report_name}_{start_date}_to_{end_date}_{timestamp}.csv")
        
        # 筛选有意义的变化（排除变化为0的记录）
        meaningful_changes = custom_changes[custom_changes['rank_change'] != 0].copy()
        
        # 按排名变化绝对值排序（取前100名变化最大的）
        meaningful_changes['abs_change'] = meaningful_changes['rank_change'].abs()
        top_changes = meaningful_changes.sort_values('abs_change', ascending=False).head(100)
        
        # 分别获取上升最多和下降最多的域名
        rising_domains = custom_changes[custom_changes['rank_change'] > 0].sort_values('rank_change', ascending=False).head(50)
        falling_domains = custom_changes[custom_changes['rank_change'] < 0].sort_values('rank_change', ascending=True).head(50)
        
        # 保存CSV报告
        try:
            # 保存总体变化报告
            top_changes[['domain', 'start_rank', 'end_rank', 'rank_change', 'change_percent']].to_csv(
                report_file, index=False, encoding='utf-8'
            )
            logging.info(f"已保存排名变化报告: {report_file}")
            
            # 保存上升域名报告
            rising_file = os.path.join(REPORT_DIR, f"{report_name}_上升_{start_date}_to_{end_date}_{timestamp}.csv")
            rising_domains[['domain', 'start_rank', 'end_rank', 'rank_change', 'change_percent']].to_csv(
                rising_file, index=False, encoding='utf-8'
            )
            logging.info(f"已保存排名上升域名报告: {rising_file}")
            
            # 保存下降域名报告
            falling_file = os.path.join(REPORT_DIR, f"{report_name}_下降_{start_date}_to_{end_date}_{timestamp}.csv")
            falling_domains[['domain', 'start_rank', 'end_rank', 'rank_change', 'change_percent']].to_csv(
                falling_file, index=False, encoding='utf-8'
            )
            logging.info(f"已保存排名下降域名报告: {falling_file}")
            
            # 生成可视化图表
            charts_dir = os.path.join(REPORT_DIR, 'charts')
            if not os.path.exists(charts_dir):
                os.makedirs(charts_dir)
            
            # 绘制排名上升Top10域名图表
            plt.figure(figsize=(12, 8))
            top10_rising = rising_domains.head(10)
            plt.barh(top10_rising['domain'], top10_rising['rank_change'], color='green')
            plt.xlabel('排名提升')
            plt.ylabel('域名')
            plt.title(f'{report_name} - 排名上升Top10域名 ({start_date} 至 {end_date})')
            plt.tight_layout()
            plt.savefig(os.path.join(charts_dir, f'{report_name}_rising_{start_date}_to_{end_date}_{timestamp}.png'))
            plt.close()
            
            # 绘制排名下降Top10域名图表
            plt.figure(figsize=(12, 8))
            top10_falling = falling_domains.head(10)
            plt.barh(top10_falling['domain'], top10_falling['rank_change'], color='red')
            plt.xlabel('排名下降')
            plt.ylabel('域名')
            plt.title(f'{report_name} - 排名下降Top10域名 ({start_date} 至 {end_date})')
            plt.tight_layout()
            plt.savefig(os.path.join(charts_dir, f'{report_name}_falling_{start_date}_to_{end_date}_{timestamp}.png'))
            plt.close()
            
            logging.info(f"已生成自定义报告的可视化图表")
        except Exception as e:
            logging.error(f"保存自定义报告失败: {e}")
    
    logging.info("域名排名变化分析完成")