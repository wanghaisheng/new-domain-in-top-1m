import aiohttp
from typing import Callable, Optional

async def lookup_domain_borndate(
    domain: str,
    query_url_func: Callable[[str], str],
    parse_borndate_func: Callable[[dict], Optional[str]],
    proxy_url: Optional[str] = None,
    session: Optional[aiohttp.ClientSession] = None,
    timeout: int = 30,
) -> Optional[str]:
    """
    通用的域名born date查询方法。
    - query_url_func: 传入domain返回查询url
    - parse_borndate_func: 传入json dict返回born date字符串
    - session: 必须由调用方创建和关闭
    """
    url = query_url_func(domain)
    if session is None:
        raise ValueError("session must be provided and managed by the caller")
    try:
        async with session.get(url, proxy=proxy_url, timeout=timeout) as response:
            if response.status == 200:
                data = await response.json()
                return parse_borndate_func(data)
    except Exception as e:
        print(f"lookup_domain_borndate error: {e}")
    return None

# Revved专用

def revved_query_url(domain: str) -> str:
    return f'https://domains.revved.com/v1/whois?domains={domain}'

def revved_parse_borndate(data: dict) -> Optional[str]:
    if 'results' in data:
        for event in data.get('results', []):
            if 'createdDate' in event:
                return event['createdDate']
    return None

# RDAP专用

def rdap_query_url(domain: str, rdap_url: str) -> str:
    return f'{rdap_url}domain/{domain}'

def rdap_parse_borndate(data: dict) -> Optional[str]:
    for event in data.get('events', []):
        if event.get('eventAction') == 'registration':
            return event.get('eventDate')
    return None 


import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from data_utils import cleandomain
from file_utils import ensure_dir_exists
from log_utils import setup_logging
import asyncio
import aiohttp
import logging

BATCH_SIZE = 10000
PROGRESS_FILE = 'borndate_revved_progress.txt'
RESULT_DIR = './results_borndate_revved'
LOG_FILE = 'borndate_revved.log'
ensure_dir_exists(RESULT_DIR)
setup_logging(LOG_FILE)

INPUT_CSV = os.getenv('input_csv', 'domains.csv')
DOMAIN_COL = os.getenv('domain_col', 'domain')
RETRY = 3

if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, 'r') as f:
        last_id = int(f.read().strip())
else:
    last_id = 0

df = pd.read_csv(INPUT_CSV)
domains = df[DOMAIN_COL].tolist()
total = len(domains)

async def fetch_borndate(domain, session):
    for attempt in range(1, RETRY+1):
        try:
            borndate = await lookup_domain_borndate(domain, revved_query_url, revved_parse_borndate, session=session)
            logging.info(f"{domain} | borndate: {borndate}")
            return domain, borndate
        except Exception as e:
            logging.warning(f"Attempt {attempt} failed for {domain}: {e}")
            if attempt == RETRY:
                return domain, f'error: {e}'
            await asyncio.sleep(1)

async def process_batch(batch_domains):
    results = []
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_borndate(d, session) for d in batch_domains]
        for r in await asyncio.gather(*tasks):
            results.append(r)
    return results

for batch_start in range(last_id, total, BATCH_SIZE):
    batch_end = min(batch_start + BATCH_SIZE, total)
    batch_domains = domains[batch_start:batch_end]
    logging.info(f'Processing {batch_start} - {batch_end}')
    batch_results = asyncio.run(process_batch(batch_domains))
    result_file = os.path.join(RESULT_DIR, f'borndate_revved_{batch_start}_{batch_end}.csv')
    pd.DataFrame(batch_results, columns=['domain', 'borndate']).to_csv(result_file, index=False)
    with open(PROGRESS_FILE, 'w') as f:
        f.write(str(batch_end))
    logging.info(f'Saved {result_file}, progress updated to {batch_end}') 
