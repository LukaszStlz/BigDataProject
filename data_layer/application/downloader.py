import os
from datetime import datetime

import asyncio
import aiohttp
import aiofile

from .consts import DATALAKE_PATH

def header_body_split(text):
    START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK"
    END_MARKER = "*** END OF THE PROJECT GUTENBERG EBOOK"
    
    header, _ = text.split(START_MARKER, 1)
    body, _ = text.split(END_MARKER, 1)

    return header, body

def download_books(urls, out_dir=DATALAKE_PATH):
    sema = asyncio.BoundedSemaphore(5)

    async def fetch_file(session, url):
        fname = url.split("/")[-1]
        book_id = fname.split('.')[0]

        # Check if book already exists
        h_path = os.path.join(out_dir, f'header_{book_id}.txt')
        b_path = os.path.join(out_dir, f'body_{book_id}.txt')

        if os.path.exists(h_path) and os.path.exists(b_path):
            print(f'Book {book_id} already downloaded, skipping')
            return

        print(f'Downloading book {book_id}...')
        async with sema:
            async with session.get(url) as resp:
                assert resp.status == 200
                data = await resp.read()

        os.makedirs(out_dir, exist_ok=True)

        header, body = header_body_split(data.decode('utf-8'))

        async with aiofile.async_open(h_path, "wb") as h_file, aiofile.async_open(b_path, "wb") as b_file:
            await h_file.write(header.encode('utf-8'))
            await b_file.write(body.encode('utf-8')) 

    async def main():
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_file(session, url) for url in urls]
            await asyncio.gather(*tasks)

    asyncio.run(main())
