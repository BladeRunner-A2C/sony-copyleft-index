#!/usr/bin/env python3

import asyncio
import json
import os
import sys
from functools import lru_cache

import aiohttp

CONN_TIMEOUT = aiohttp.ClientTimeout(total=30)
BASE_URL = 'https://developer.sony.com'
API_PATH = '/api/files'
API_PARAMS = 'tags=xperia-open-source-archives&type=file_download'
BATCH_SIZE = 100
MAX_CONCURRENT = 10
OUTPUT_FILE = 'open-source-archives.json'

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    print(
        'Error: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables must be set.',
        file=sys.stderr,
    )
    sys.exit(1)


def process_item(item):
    key = item.get('key', '')
    content = item.get('content', {})
    post_title = content.get('post_title', '')

    full_url = f'{BASE_URL}{key}' if key else ''

    return {'title': post_title, 'url': full_url}


@lru_cache(maxsize=128)
def build_url(base_url, limit, offset):
    if '?' in base_url:
        return f'{base_url}&limit={limit}&offset={offset}'
    return f'{base_url}?limit={limit}&offset={offset}'


async def fetch_batch(session, url, batch_size, offset):
    full_url = build_url(url, batch_size, offset)

    async with session.get(full_url) as response:
        if response.status != 200:
            print(
                f'Error: API request failed with status {response.status}',
                file=sys.stderr,
            )
            raise Exception(f'API request failed with status {response.status}')
        data = await response.json()

    return [process_item(item) for item in data.get('filesList', [])]


async def fetch_all_sony_archives(
    url, batch_size=BATCH_SIZE, max_concurrent=MAX_CONCURRENT
):
    all_results = []

    async with aiohttp.ClientSession(timeout=CONN_TIMEOUT) as session:
        data_url = build_url(url, batch_size, 0)
        async with session.get(data_url) as response:
            if response.status != 200:
                print(
                    f'Error: API request failed with status {response.status}',
                    file=sys.stderr,
                )
                raise Exception(
                    f'API request failed with status {response.status}'
                )
            data = await response.json()
        total_files = data.get('totalFiles', 0)

        first_batch = await fetch_batch(session, url, batch_size, 0)
        all_results.extend(first_batch)

        remaining_offsets = list(range(batch_size, total_files, batch_size))

        if remaining_offsets:
            semaphore = asyncio.Semaphore(max_concurrent)

            async def fetch_with_semaphore(offset):
                async with semaphore:
                    return await fetch_batch(session, url, batch_size, offset)

            tasks = [
                fetch_with_semaphore(offset) for offset in remaining_offsets
            ]

            batch_results = await asyncio.gather(*tasks)

            for results in batch_results:
                all_results.extend(results)

    return all_results


def format_message(post):
    return f'<b>{post["title"]}</b>\n{post["url"]}'


async def send_telegram_message(session, bot_token, chat_id, text):
    send_url = f'https://api.telegram.org/bot{bot_token}/sendMessage'
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'HTML',
        'disable_web_page_preview': False,
    }
    async with session.post(send_url, data=payload) as resp:
        if resp.status != 200:
            text_resp = await resp.text()
            print(
                f'Error: Telegram API error {resp.status}: {text_resp}',
                file=sys.stderr,
            )
            raise Exception(f'Telegram API error {resp.status}: {text_resp}')
        return await resp.json()


async def main():
    url = f'{BASE_URL}{API_PATH}?{API_PARAMS}'

    try:
        new_result = await fetch_all_sony_archives(url)

        old_result = []
        if os.path.exists(OUTPUT_FILE):
            with open(OUTPUT_FILE, 'r') as f:
                old_result = json.load(f)

        old_urls = {item.get('url') for item in old_result}
        new_posts = [
            post for post in new_result if post.get('url') not in old_urls
        ]

        if not new_posts:
            print('No new posts found.')
        else:
            async with aiohttp.ClientSession() as session:
                for post in new_posts:
                    message = format_message(post)
                    try:
                        await send_telegram_message(
                            session,
                            TELEGRAM_BOT_TOKEN,
                            TELEGRAM_CHAT_ID,
                            message,
                        )
                        print(f'Sent Telegram message for {post["title"]}')
                    except Exception as e:
                        print(
                            f'Failed to send message for {post["title"]}: {e}',
                            file=sys.stderr,
                        )
                    await asyncio.sleep(30)  # 30 seconds delay between messages

        with open(OUTPUT_FILE, 'w') as f:
            json.dump(new_result, f, indent=2)

        print(f'Updated {OUTPUT_FILE} with {len(new_result)} records')

    except Exception as e:
        print(f'Error: {e}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
