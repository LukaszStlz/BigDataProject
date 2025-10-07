
import redis, time
from collections import defaultdict

r = redis.Redis(host="redis", port=6379, decode_responses=True)

def build_index(docs: dict[str, list[str]]):
    start = time.perf_counter()
    pipe = r.pipeline(transaction=False)
    for book_id, tokens in docs.items():
        for t in tokens:
            pipe.sadd(f"term:{t}", book_id)
    pipe.execute()
    elapsed = (time.perf_counter() - start) * 1000
    print(f"[BENCH] Redis index built in {elapsed:.1f} ms ({len(docs)} docs)")

def query(term: str):
    return list(r.smembers(f"term:{term}"))
