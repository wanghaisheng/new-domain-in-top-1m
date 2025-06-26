import os
import pandas as pd
import asyncio
from datetime import datetime
from file_utils import ensure_dir_exists, write_lines, read_lines
from data_utils import cleandomain
from html_utils import extract_about_page_data
from aiohttp import ClientSession
import glob
import logging

BATCH_SIZE = 1000
PROGRESS_FILE = 'aboutdata_progress.txt'
RESULT_DIR = './results_aboutdata'
LOG_FILE = 'aboutdata.log'
NEW_DOMAINS_DIR = './new_domains'

ensure_dir_exists(RESULT_DIR)

async def fetch_aboutdata(session, domain):
    url = f"https://www.google.com/search?q=About+{domain}&tbm=ilp"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; aboutdata-bot/1.0)"}
    try:
        async with session.get(url, timeout=15, headers=headers) as resp:
            html = await resp.text()
            return extract_about_page_data(html)
    except Exception as e:
        logging.warning(f"Error fetching {domain}: {e}")
        return ''

async def process_batch(batch_domains):
    results = []
    async with ClientSession() as session:
        tasks = [fetch_aboutdata(session, d) for d in batch_domains]
        responses = await asyncio.gather(*tasks)
        for domain, aboutdata in zip(batch_domains, responses):
            results.append((domain, aboutdata))
    return results

def process_domains():
    txt_files = glob.glob(os.path.join(NEW_DOMAINS_DIR, "*.txt"))
    date_str = datetime.now().strftime('%Y-%m-%d')
    for txt_file in txt_files:
        if date_str != os.path.splitext(os.path.basename(txt_file))[0]:
            continue

        with open(txt_file, 'r', encoding='utf-8') as f:
            domains = [cleandomain(line.strip()) for line in f if line.strip()]
        if not domains:
            continue

        logging.info(f"Processing {txt_file}, total {len(domains)} domains")
        all_results = []
        for batch_start in range(0, len(domains), BATCH_SIZE):
            batch_domains = domains[batch_start:batch_start+BATCH_SIZE]
            batch_results = asyncio.run(process_batch(batch_domains))
            all_results.extend(batch_results)

        result_file = os.path.join(RESULT_DIR, f"{date_str}.csv")
        pd.DataFrame(all_results, columns=['domain', 'aboutdata']).to_csv(result_file, index=False)
        logging.info(f"Saved results to {result_file}")

if __name__ == "__main__":
    logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
    process_domains()
