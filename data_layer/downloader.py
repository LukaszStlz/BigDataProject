import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import asyncio
import aiohttp
import aiofile

from consts import (
    DATALAKE_PATH,
    CONTROL_PATH,
    DOWNLOADED_BOOKS_FILE,
)

START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK"
END_MARKER   = "*** END OF THE PROJECT GUTENBERG EBOOK"

def _timestamped_dir(base: str) -> str:
    ts = datetime.now(timezone.utc)
    day = ts.strftime("%Y%m%d")
    hour = ts.strftime("%H")
    target = Path(base) / day / hour
    target.mkdir(parents=True, exist_ok=True)
    return str(target)

def _extract_book_id(url: str) -> str:
    m = re.search(r"/pg(\d+)\.txt$", url)
    if m:
        return m.group(1)
    tail = url.rstrip("/").split("/")[-1]
    return os.path.splitext(tail)[0].lstrip("pg")

def _split_header_body(text: str):
    if START_MARKER not in text or END_MARKER not in text:
        raise ValueError("Gutenberg markers not found in text")
    header, body_and_footer = text.split(START_MARKER, 1)
    body, _ = body_and_footer.split(END_MARKER, 1)
    return header.strip(), body.strip()

def _load_downloaded_set() -> set:
    p = Path(DOWNLOADED_BOOKS_FILE)
    if not p.exists():
        return set()
    return set(p.read_text(encoding="utf-8").splitlines())

async def _append_downloaded(book_id: str, downloaded_set: set):
    if book_id in downloaded_set:
        return
    Path(CONTROL_PATH).mkdir(parents=True, exist_ok=True)
    async with aiofile.async_open(DOWNLOADED_BOOKS_FILE, "a") as f:
        await f.write(f"{book_id}\n")
    downloaded_set.add(book_id)

def download_books(urls: Iterable[str], out_dir: str = DATALAKE_PATH):
    sema = asyncio.BoundedSemaphore(5)
    target_dir = _timestamped_dir(out_dir)
    downloaded_set = _load_downloaded_set()

    async def fetch_file(session: aiohttp.ClientSession, url: str):
        book_id = _extract_book_id(url)
        h_path = os.path.join(target_dir, f"{book_id}.header.txt")
        b_path = os.path.join(target_dir, f"{book_id}.body.txt")

        
        if os.path.exists(h_path) and os.path.exists(b_path):
            print(f"[DOWNLOADER] {book_id} already saved in {target_dir}, skipping")
            await _append_downloaded(book_id, downloaded_set)  
            return

        print(f"[DOWNLOADER] Downloading {book_id}...")
        async with sema:
            async with session.get(url) as resp:
                if resp.status != 200:
                    print(f"[DOWNLOADER] {book_id} failed: HTTP {resp.status}")
                    return
                data = await resp.text(encoding="utf-8", errors="ignore")

        try:
            header, body = _split_header_body(data)
        except Exception as e:
            print(f"[DOWNLOADER] {book_id} split error: {e}")
            return

        async with aiofile.async_open(h_path, "w") as hf:
            await hf.write(header)
        async with aiofile.async_open(b_path, "w") as bf:
            await bf.write(body)

        await _append_downloaded(book_id, downloaded_set)
        print(f"[DOWNLOADER] Saved {book_id} to {target_dir}")

    async def main():
        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*(fetch_file(session, u) for u in urls))

    asyncio.run(main())
