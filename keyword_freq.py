import csv
import re
from collections import Counter
import logging

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def analyze_domain_keywords(csv_file):
    """
    从CSV文件中读取域名，提取关键词并统计词频。
    处理大型CSV文件（1M+行）进行了优化。

    Args:
        csv_file (str): CSV文件的路径。  CSV文件第一列是排名，第二列是域名。

    Returns:
        Counter: 一个Counter对象，包含了关键词及其词频。
    """

    keywords = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader, None)  # 跳过标题行

            for i, row in enumerate(reader):
                if i % 10000 == 0:  # 每处理 10000 行记录一次日志
                    logging.info(f"Processed {i} rows")

                if len(row) < 2:
                    logging.warning(f"Skipping row {i} due to insufficient columns: {row}")
                    continue

                try:
                    domain = row[1].strip()
                    domain_name = domain.split('.')[0]  # remove .com etc.
                    words = re.findall(r"[a-zA-Z0-9-]+", domain_name)
                    keywords.extend(words)
                except IndexError:
                    logging.error(f"Error processing row {i}: {row}")
                    continue

    except FileNotFoundError:
        logging.error(f"File not found: {csv_file}")
        return Counter()  # 返回空的 Counter，避免程序崩溃
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return Counter()


    # 统计词频
    keyword_counts = Counter(keywords)
    return keyword_counts


if __name__ == "__main__":
    csv_file_path = 'top-1m.csv'  # 替换为你的CSV文件路径
    logging.info(f"Starting analysis of {csv_file_path}")
    keyword_counts = analyze_domain_keywords(csv_file_path)
    logging.info("Analysis complete.")

    # 打印词频统计结果 (只打印前20个，避免输出过多)
    print("Top 20 关键词词频统计:")
    for keyword, count in keyword_counts.most_common(20):
        print(f"{keyword}: {count}")

    # 保存到文件
    output_file = "keyword_counts.txt"
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            for keyword, count in keyword_counts.most_common():
                f.write(f"{keyword}: {count}\n")
        logging.info(f"关键词词频统计已保存到 {output_file}")
    except Exception as e:
        logging.error(f"Error saving results to {output_file}: {e}")
