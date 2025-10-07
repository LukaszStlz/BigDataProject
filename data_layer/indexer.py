import re
from pathlib import Path
from typing import Set, Dict, List, Iterable

import redis
from consts import DATALAKE_PATH


class Indexer:
    """
    Builds a simple inverted index in Redis.
    Keys:
      - book:{id}:metadata -> hash(title, word_count, unique_words)
      - word:{term}        -> set(book_id, ...)
      - title_word:{term}  -> set(book_id, ...)
      - stats:all_words    -> set(all unique terms)
      - stats:indexed_books-> set(book_id, ...)
    """

    def __init__(self, redis_host: str = "redis", redis_port: int = 6379):
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.datalake = Path(DATALAKE_PATH)

    # -----------------------
    # Helpers
    # -----------------------
    def tokenize_text(self, text: str) -> Set[str]:
        # keep only alphabetic tokens, lowercase, length > 2
        words = re.findall(r"\b[a-zA-Z]+\b", text.lower())
        return {w for w in words if len(w) > 2}

    def _iter_books(self) -> Iterable[tuple[str, Path, Path]]:
        """
        Yield unique (book_id, header_path, body_path) triples from the datalake.
        We:
          - find all *.header.txt files
          - parse book_id strictly from the filename (NNN.header.txt)
          - pick only one (latest) occurrence per book_id if there are duplicates across hours
          - require that the matching body file exists
        """
        headers = sorted(
            self.datalake.glob("**/*.header.txt"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,  # latest first
        )
        seen: Set[str] = set()

        for header in headers:
            m = re.match(r"^(\d+)\.header\.txt$", header.name)
            if not m:
                continue
            bid = m.group(1)
            if bid in seen:
                continue
            body = header.with_name(f"{bid}.body.txt")
            if not body.exists():
                continue
            seen.add(bid)
            yield bid, header, body

    def get_indexed_books(self) -> Set[str]:
        # Redis KEYS scan for already indexed metadata
        keys = self.redis.keys("book:*:metadata")
        return {k.split(":")[1] for k in keys}

    def is_book_indexed(self, book_id: str) -> bool:
        return self.redis.exists(f"book:{book_id}:metadata") > 0

    # -----------------------
    # Core
    # -----------------------
    def process_book(self, book_id: str, header_file: Path, body_file: Path) -> Dict:
        header_content = header_file.read_text(encoding="utf-8", errors="ignore").strip()
        body_content = body_file.read_text(encoding="utf-8", errors="ignore")

        all_words = self.tokenize_text(header_content + " " + body_content)
        title_words = self.tokenize_text(header_content)

        return {
            "book_id": book_id,
            # Stage 1: we keep whole header as title placeholder; Stage 2 can parse true Title/Author
            "title": header_content,
            "all_words": all_words,
            "title_words": title_words,
            "word_count": len(body_content.split()),
        }

    def index_book(self, book_data: Dict) -> None:
        bid = book_data["book_id"]
        pipe = self.redis.pipeline()

        
        pipe.hset(
            f"book:{bid}:metadata",
            mapping={
                "title": book_data["title"],
                "word_count": str(book_data["word_count"]),
                "unique_words": str(len(book_data["all_words"])),
            },
        )

      
        for w in book_data["all_words"]:
            pipe.sadd(f"word:{w}", bid)
        for w in book_data["title_words"]:
            pipe.sadd(f"title_word:{w}", bid)

       
        if book_data["all_words"]:
            pipe.sadd("stats:all_words", *list(book_data["all_words"]))
        pipe.sadd("stats:indexed_books", bid)

        pipe.execute()

    def index_all_books(self, force_reindex: bool = False) -> None:
        indexed = self.get_indexed_books()
        to_index: List[tuple[str, Path, Path]] = []

        for bid, h, b in self._iter_books():
            if force_reindex or (bid not in indexed):
                to_index.append((bid, h, b))

        if not to_index:
            print("[INDEXER] No new books to index.")
            return

        print(f"[INDEXER] Indexing {len(to_index)} book(s)")
        for i, (bid, h, b) in enumerate(to_index, 1):
            try:
                data = self.process_book(bid, h, b)
                self.index_book(data)
                print(f"[INDEXER] {i}/{len(to_index)} indexed: {bid}")
            except Exception as e:
                print(f"[INDEXER] Error on {bid}: {e}")

    
    def search_books(self, query: str) -> List[str]:
        words = self.tokenize_text(query)
        if not words:
            return []
        sets = [self.redis.smembers(f"word:{w}") for w in words]
        if not sets:
            return []
        res = set(sets[0])
        for s in sets[1:]:
            res &= s
        return sorted(res)

    def get_book_info(self, book_id: str) -> Dict:
        return self.redis.hgetall(f"book:{book_id}:metadata")

    def get_stats(self) -> Dict:
        
        return {
            "total_books": self.redis.scard("stats:indexed_books"),
            "unique_words": self.redis.scard("stats:all_words"),
            "indexed_books": len(self.get_indexed_books()),
        }
