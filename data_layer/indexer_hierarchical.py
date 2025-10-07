
from pathlib import Path
import os, json, time
from collections import defaultdict

INDEX_DIR = Path("/app/datamarts/inverted_index")

def build_index(docs: dict[str, list[str]]):
    """
    docs: { book_id: list of normalized tokens }
    builds per-term .txt files grouped by first letter
    """
    start = time.perf_counter()
    INDEX_DIR.mkdir(parents=True, exist_ok=True)

    
    postings = defaultdict(set)
    for book_id, tokens in docs.items():
        for t in tokens:
            postings[t].add(book_id)

    for term, docset in postings.items():
        first = term[0].upper()
        folder = INDEX_DIR / first
        folder.mkdir(parents=True, exist_ok=True)
        with open(folder / f"{term}.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(docset)))

    elapsed = (time.perf_counter() - start) * 1000
    print(f"[BENCH] Hierarchical index built in {elapsed:.1f} ms ({len(postings)} terms)")

def query(term: str):
    """return postings list for a term"""
    first = term[0].upper()
    path = INDEX_DIR / first / f"{term}.txt"
    if not path.exists():
        return []
    return path.read_text().splitlines()
