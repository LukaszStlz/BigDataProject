from typing import Iterable, List, Tuple, Dict, Union
import redis

REDIS_HOST = "redis"
REDIS_PORT = 6379

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

RowType = Union[
    Tuple[str, str, str, str, str, str],  
    Dict[str, str],
]

def _row_to_dict(row: RowType) -> Dict[str, str]:
    if isinstance(row, dict):
        return {
            "book_id": row["book_id"],
            "title": row.get("title", "") or "",
            "author": row.get("author", "") or "",
            "language": row.get("language", "") or "",
            "header_path": row.get("header_path", "") or "",
            "body_path": row.get("body_path", "") or "",
        }
    # tuple
    return {
        "book_id": row[0],
        "title": row[1],
        "author": row[2],
        "language": row[3],
        "header_path": row[4],
        "body_path": row[5],
    }

def init_db() -> None:
    
    r.setnx("md:initialized", "1")

def bulk_upsert(rows: Iterable[RowType]) -> None:
    pipe = r.pipeline()
    for row in rows:
        d = _row_to_dict(row)
        bid = d["book_id"]
        ak = f"md:author:{d['author']}"
        hk = f"md:book:{bid}"
       
        pipe.hset(hk, mapping=d)
        
        pipe.sadd(ak, bid)
    pipe.execute()

def find_by_author(author: str) -> List[Tuple[str, str, str, str, str, str]]:
    """
    Return shape compatible with sqlite/mysql: list of tuples:
    (book_id, title, author, language, header_path, body_path)
    """
    ids = list(r.smembers(f"md:author:{author}"))
    out: List[Tuple[str, str, str, str, str, str]] = []
    if not ids:
        return out
    pipe = r.pipeline()
    for bid in ids:
        pipe.hgetall(f"md:book:{bid}")
    rows = pipe.execute()
    for d in rows:
        if not d:
            continue
        out.append((
            d.get("book_id", ""),
            d.get("title", ""),
            d.get("author", ""),
            d.get("language", ""),
            d.get("header_path", ""),
            d.get("body_path", ""),
        ))
    return out
