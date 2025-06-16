import aiohttp
from typing import Callable, Optional
from datetime import datetime
import asyncio
import os
import pandas as pd
import logging
import glob

# === 配置参数 ===
BATCH_SIZE = 1000
RETRY = 3
SEMAPHORE_LIMIT = 100
RESULT_DIR = './borndate_results'
FAILED_SUFFIX = '_failed.txt'
LOG_FILE = 'borndate_revved.log'

# === 通用 born date 查询函数 ===
async def lookup_domain_borndate(
    domain: str,
    query_url_func: Callable[[str], str],
    parse_borndate_func: Callable[[dict], Optional[str]],
    proxy_url: Optional[str] = None,
    session: Optional[aiohttp.ClientSession] = None,
    timeout: int = 30,
) -> Optional[str]:
    url = query_url_func(domain)
    if session is None:
        raise ValueError("session must be provided and managed by the caller")
    try:
        async with session.get(url, proxy=proxy_url, timeout=timeout) as response:
            if response.status == 200:
                data = await response.json()
                return parse_borndate_func(data)
    except Exception as e:
        logging.warning(f"lookup_domain_borndate error for {domain}: {e}")
    return None

# === Revved 接口 ===
def revved_query_url(domain: str) -> str:
    return f'https://domains.revved.com/v1/whois?domains={domain}'

def revved_parse_borndate(data: dict) -> Optional[str]:
    if 'results' in data:
        for event in data.get('results', []):
            if 'createdDate' in event:
                return event['createdDate']
    return None

# === fetch 单个域名数据 ===
semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

async def fetch_borndate(domain, session):
    async with semaphore:
        for attempt in range(1, RETRY + 1):
            try:
                borndate = await lookup_domain_borndate(
                    domain, revved_query_url, revved_parse_borndate, session=session
                )
                if borndate:
                    logging.info(f"{domain} | borndate: {borndate}")
                return domain, borndate or 'not found'
            except Exception as e:
                logging.warning(f"Attempt {attempt} failed for {domain}: {e}")
                if attempt == RETRY:
                    return domain, f'error: {e}'
                await asyncio.sleep(1)

# === 分批执行 ===
async def process_batch(batch_domains):
    results = []
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_borndate(d, session) for d in batch_domains]
        results = await asyncio.gather(*tasks)
    return results

# === 主执行函数 ===
async def process_domains():
    os.makedirs(RESULT_DIR, exist_ok=True)
    date_str = datetime.now().strftime('%Y-%m-%d')
    txt_files = glob.glob(os.path.join("new_domains", "*.txt"))
    logging.basicConfig(
        filename=LOG_FILE, level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s'
    )
    for txt_file in txt_files:
        filename = os.path.splitext(os.path.basename(txt_file))[0]
        if filename != date_str:
            continue

        with open(txt_file, 'r', encoding='utf-8') as f:
            domains = [line.strip() for line in f if line.strip()]

        if not domains:
            continue

        logging.info(f"处理 {txt_file}，共 {len(domains)} 个域名")
        all_results, failed = [], []

        for batch_start in range(0, len(domains), BATCH_SIZE):
            batch = domains[batch_start:batch_start+BATCH_SIZE]
            batch_results = await process_batch(batch)
            all_results.extend(batch_results)
            failed.extend([d for d, b in batch_results if b.startswith('error:')])

        # 保存成功结果
        result_file = os.path.join(RESULT_DIR, f"{date_str}.csv")
        pd.DataFrame(all_results, columns=['domain', 'borndate']).to_csv(result_file, index=False)

        # 保存失败列表
        if failed:
            failed_file = os.path.join(RESULT_DIR, f"{date_str}{FAILED_SUFFIX}")
            with open(failed_file, 'w') as f:
                f.write('\n'.join(failed))

        logging.info(f"已保存结果到 {result_file}, 失败 {len(failed)} 个")

# === 入口点 ===
if __name__ == "__main__":
    asyncio.run(process_domains())
