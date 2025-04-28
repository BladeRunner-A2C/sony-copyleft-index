#!/usr/bin/env python3

import asyncio
import json
import sys
import aiohttp

CONN_TIMEOUT = aiohttp.ClientTimeout(total=30)
BASE_URL = 'https://developer.sony.com'
API_PATH = '/api/files'
API_PARAMS = 'tags=xperia-open-source-archives&type=file_download'
BATCH_SIZE = 100
MAX_CONCURRENT = 10
OUTPUT_FILE = 'open-source-archives.json'


def process_item(item):
    key = item.get('key', '')
    content = item.get('content', {})
    post_title = content.get('post_title', '')

    return {'title': post_title, 'url': f'{BASE_URL}{key}' if key else ''}


async def fetch_batch(session, url, batch_size, offset):
    full_url = f'{url}&limit={batch_size}&offset={offset}' if '?' in url else f'{url}?limit={batch_size}&offset={offset}'

    async with session.get(full_url) as response:
        if response.status != 200:
            raise Exception(f'API request failed with status {response.status}')
        data = await response.json()

    return [process_item(item) for item in data.get('filesList', [])], data.get('totalFiles', 0)


async def fetch_all_sony_archives(url, batch_size=BATCH_SIZE, max_concurrent=MAX_CONCURRENT):
    all_results = []

    async with aiohttp.ClientSession(timeout=CONN_TIMEOUT) as session:
        data, total_files = await fetch_batch(session, url, batch_size, 0)
        all_results.extend(data)

        if total_files > batch_size:
            semaphore = asyncio.Semaphore(max_concurrent)

            async def fetch_with_semaphore(offset):
                async with semaphore:
                    results, _ = await fetch_batch(session, url, batch_size, offset)
                    return results

            offsets = range(batch_size, total_files, batch_size)
            batch_results = await asyncio.gather(*[fetch_with_semaphore(offset) for offset in offsets])
            all_results.extend([result for results in batch_results for result in results])

    return all_results


async def main():
    url = f'{BASE_URL}{API_PATH}?{API_PARAMS}'

    try:
        result = await fetch_all_sony_archives(url)

        with open(OUTPUT_FILE, 'w') as f:
            json.dump(result, f, indent=2)

        print(f'Successfully created {OUTPUT_FILE} with {len(result)} records')
    except Exception as e:
        print(f'Error: {e}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
