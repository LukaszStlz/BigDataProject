import os
from datetime import datetime

import asyncio
import aiohttp
import aiofile


DATALAKE_PATH = "/app/datalake"

def header_body_split(text):
    START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK"
    END_MARKER = "*** END OF THE PROJECT GUTENBERG EBOOK"
    
    header, _ = text.split(START_MARKER, 1)
    body, _ = text.split(END_MARKER, 1)

    return header, body

def download_books(urls):
    sema = asyncio.BoundedSemaphore(5)

    async def fetch_file(session, url):
        fname = url.split("/")[-1]
        async with sema:
            async with session.get(url) as resp:
                assert resp.status == 200
                data = await resp.read()

        main_dir_path = os.path.join(DATALAKE_PATH, f"{datetime.now().year}{datetime.now().strftime('%m')}{datetime.now().strftime('%d')}")
        sub_dir_path = os.path.join(main_dir_path, f'{datetime.now().strftime('%I')}') 
        
        os.makedirs(main_dir_path, exist_ok=True) # blocking op for now
        os.makedirs(sub_dir_path, exist_ok=True) # blocking op for now

        h_path = os.path.join(sub_dir_path, f'{fname.split('.')[0]}_header.txt')
        b_path = os.path.join(sub_dir_path, f'{fname.split('.')[0]}_body.txt')

        header, body = header_body_split(data.decode('utf-8'))

        async with aiofile.async_open(h_path, "wb") as h_file, aiofile.async_open(b_path, "wb") as b_file:
            await h_file.write(header.encode('utf-8'))
            await b_file.write(body.encode('utf-8')) 

    async def main():
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_file(session, url) for url in urls]
            await asyncio.gather(*tasks)

    asyncio.run(main())

urls = [
        "https://www.gutenberg.org/cache/epub/1342/pg1342.txt",
        ]

download_books(urls)
