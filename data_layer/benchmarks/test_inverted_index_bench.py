import pytest
import random
from data_layer.indexer import Indexer
from data_layer.indexer_hierarchical import IndexerHierarchical

# --- FIXTURES ---
@pytest.fixture(scope="module")
def flat_indexer():
    return Indexer(redis_host="redis", redis_port=6379)

@pytest.fixture(scope="module")
def hier_indexer():
    return IndexerHierarchical(redis_host="redis", redis_port=6379)

# --- BUILD INDEX (różne liczby plików) ---
@pytest.mark.parametrize("limit", [100, 500, 1000])
@pytest.mark.benchmark(group="index_build")
def test_build_index(flat_indexer, hier_indexer, benchmark, limit):
    for idx_type, instance in [
        ("flat", flat_indexer),
        ("hierarchical", hier_indexer)
    ]:
        benchmark.extra_info = {"type": idx_type, "limit": limit}
        benchmark.group = f"index_build_{idx_type}"
        benchmark(lambda: instance.index_all_books(limit=limit))

# --- QUERY SINGLE WORD ---
@pytest.mark.parametrize("word", ["adventure", "sea", "island", "ship"])
@pytest.mark.benchmark(group="query_single")
def test_query_word(flat_indexer, hier_indexer, benchmark, word):
    for idx_type, instance in [
        ("flat", flat_indexer),
        ("hierarchical", hier_indexer)
    ]:
        benchmark.extra_info = {"type": idx_type, "word": word}
        benchmark.group = f"query_single_{idx_type}"
        benchmark(lambda: instance.search_books(word))

# --- QUERY MULTI-WORD ---
QUERIES = ["island shipwreck", "love death", "monster creation"]

@pytest.mark.parametrize("phrase", QUERIES)
@pytest.mark.benchmark(group="query_multi")
def test_query_phrase(flat_indexer, hier_indexer, benchmark, phrase):
    for idx_type, instance in [
        ("flat", flat_indexer),
        ("hierarchical", hier_indexer)
    ]:
        benchmark.extra_info = {"type": idx_type, "query": phrase}
        benchmark.group = f"query_multi_{idx_type}"
        benchmark(lambda: instance.search_books(phrase))
