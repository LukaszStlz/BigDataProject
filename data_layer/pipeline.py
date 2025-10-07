import os
import re
import time
from pathlib import Path

from consts import DATALAKE_PATH
import downloader
from indexer import Indexer


from metadata_sqlite import (
    init_db as sqlite_init,
    bulk_upsert as sqlite_bulk_upsert,
    find_by_author as sqlite_find_by_author,
)
from metadata_mysql import (
    init_db as mysql_init,
    bulk_upsert as mysql_bulk_upsert,
    find_by_author as mysql_find_by_author,
)
from metadata_redis import (
    init_db as redis_init,
    bulk_upsert as redis_bulk_upsert,
    find_by_author as redis_find_by_author,
)

GUTENBERG_URLS = [
    "https://www.gutenberg.org/cache/epub/1342/pg1342.txt",
    "https://www.gutenberg.org/cache/epub/84/pg84.txt",
    "https://www.gutenberg.org/cache/epub/11/pg11.txt",
    "https://www.gutenberg.org/cache/epub/74/pg74.txt",
    "https://www.gutenberg.org/cache/epub/1080/pg1080.txt",
]



def _header_files() -> list[Path]:
    root = Path(DATALAKE_PATH)
    return sorted(root.rglob("*.header.txt"))

def _book_id_from_header_path(p: Path) -> str:
    # file is something like .../1342.header.txt
    stem = p.stem  # '1342.header'
    bid = stem.split(".")[0]
    return bid

def _parse_header_simple(txt: str) -> tuple[str, str, str]:
    """
    Extract (title, author, language) with simple regex. Missing -> "".
    """
    def grab(key: str) -> str:
        m = re.search(rf"^\s*{key}\s*:\s*(.+)$", txt, flags=re.I | re.M)
        return m.group(1).strip() if m else ""

    title = grab("Title")
    author = grab("Author")
    language = grab("Language")

    
    lang_map = {
        "english": "en",
        "en": "en",
        "fr": "fr",
        "french": "fr",
        "de": "de",
        "german": "de",
        "es": "es",
        "spanish": "es",
    }
    if language:
        key = language.strip().lower()
        language = lang_map.get(key, key[:2] if len(key) >= 2 else key)

    return title, author, language or ""

def _build_rows_from_headers(headers: list[Path]) -> list[dict]:
    rows: list[dict] = []
    for h in headers:
        bid = _book_id_from_header_path(h)
        body = h.with_name(f"{bid}.body.txt")
        if not body.exists():
            continue
        try:
            header_txt = h.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            header_txt = ""
        title, author, language = _parse_header_simple(header_txt)
        rows.append({
            "book_id": bid,
            "title": title,
            "author": author,
            "language": language,
            "header_path": str(h),
            "body_path": str(body),
        })
    return rows



def _bench_metadata(rows: list[dict]) -> None:
    print("[METADATA] Initializing SQLite, MySQL, Redis...")
    sqlite_init()
    mysql_init()
    redis_init()

 
    t0 = time.perf_counter()
    sqlite_bulk_upsert(rows)
    t1 = time.perf_counter()
    print(f"[BENCH] SQLite bulk upsert: {1000*(t1-t0):.1f} ms for {len(rows)} rows")

    t0 = time.perf_counter()
    mysql_bulk_upsert(rows)
    t1 = time.perf_counter()
    print(f"[BENCH] MySQL  bulk upsert: {1000*(t1-t0):.1f} ms for {len(rows)} rows")

    t0 = time.perf_counter()
    redis_bulk_upsert(rows)
    t1 = time.perf_counter()
    print(f"[BENCH] Redis  bulk upsert: {1000*(t1-t0):.1f} ms for {len(rows)} rows")

   
    authors = ["Jane Austen", "Lewis Carroll", "Mark Twain"]
    for a in authors:
        t0 = time.perf_counter()
        r_sqlite = sqlite_find_by_author(a)
        t1 = time.perf_counter()
        t_sqlite = 1000*(t1-t0)

        t0 = time.perf_counter()
        r_mysql = mysql_find_by_author(a)
        t1 = time.perf_counter()
        t_mysql = 1000*(t1-t0)

        t0 = time.perf_counter()
        r_redis = redis_find_by_author(a)
        t1 = time.perf_counter()
        t_redis = 1000*(t1-t0)

        print(
            f"[QBENCH] author='{a}' | "
            f"SQLite: {t_sqlite:.2f} ms (rows={len(r_sqlite)}) | "
            f"MySQL: {t_mysql:.2f} ms (rows={len(r_mysql)}) | "
            f"Redis: {t_redis:.2f} ms (rows={len(r_redis)})"
        )

def main():
    print("Starting pipeline...")

  
    try:
        downloader.download_books(GUTENBERG_URLS)
        print(f"[DOWNLOADER] called downloader.download_books(urls) with {len(GUTENBERG_URLS)} URLs")
    except Exception as e:
        print(f"[DOWNLOADER] Skipped due to error: {e}")

    
    headers = _header_files()
    print(f"[DEBUG] Header files found under {DATALAKE_PATH}: {len(headers)}")
    for p in headers[:10]:
        print(f"[DEBUG]  - {p}")
    rows = _build_rows_from_headers(headers)
    if not rows:
        print("[METADATA] No headers found; skipping metadata benches.")
    else:
       
        _bench_metadata(rows)

   
    print("Indexing books (skips already indexed)")
    try:
        Indexer().index_all_books()
        print("[INDEXER] Indexer().index_all_books() ran.")
    except Exception as e:
        print(f"[INDEXER] Could not run Indexer.index_all_books(): {e}")

    
    try:
        stats = Indexer().get_stats()
        print("\nPipeline complete!")
        print(f"Total books indexed: {stats.get('indexed_books', 0)}")
        print(f"Unique words: {stats.get('unique_words', 0)}")
    except Exception:
        print("\nPipeline complete!")

if __name__ == "__main__":
    main()
