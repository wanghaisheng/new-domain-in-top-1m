# new-domain-in-top-1m

use parquet to save rankings
获取历史数据
https://github.com/adysec/top_1m_domains



integration with 

https://github.com/karlhorky/github-actions-database-persistence


优化report  增加 borndate、serp count、google about描述和提取到关键词等

根据data/process_history.json中的日记，计算与当前日期的gap，罗列所有的日期，针对每个日期，run_chunked_import.py，获取对应的zip，完成数据插入
