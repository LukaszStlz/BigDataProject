import pytest
import random
from data_layer.metadata_sqlite import init_db as sqlite_init, bulk_upsert as sqlite_upsert, find_by_author as sqlite_query
from data_layer.metadata_mysql import init_db as mysql_init, bulk_upsert as mysql_upsert, find_by_author as mysql_query
from data_layer.metadata_redis import init_db as redis_init, bulk_upsert as redis_upsert, find_by_author as redis_query

# --- Dane testowe o różnej skali ---
DATASETS = {
    "small": 200,
    "medium": 1000,
    "large": 5000
}

def generate_rows(n):
    return [
        (f"book_{i}", f"Title {i}", f"Author {i % 20}", "en",
         f"/path/to/book_{i}.header.txt", f"/path/to/book_{i}.body.txt")
        for i in range(n)
    ]

@pytest.fixture(scope="session", autouse=True)
def setup_dbs():
    sqlite_init()
    mysql_init()
    redis_init()

# --- Bulk insert ---
@pytest.mark.parametrize("size", list(DATASETS.keys()))
@pytest.mark.benchmark(group="bulk_upsert")
def test_bulk_upsert_all(size, benchmark):
    n = DATASETS[size]
    rows = generate_rows(n)
    for db_name, func in [
        ("SQLite", sqlite_upsert),
        ("MySQL", mysql_upsert),
        ("Redis", redis_upsert)
    ]:
        benchmark.extra_info = {"db": db_name, "size": size, "rows": n}
        benchmark.group = f"bulk_upsert_{size}"
        benchmark(lambda: func(rows))

# --- Query performance ---
AUTHORS = [f"Author {i}" for i in range(1, 6)]

@pytest.mark.parametrize("author", AUTHORS)
@pytest.mark.benchmark(group="query")
def test_query_all(author, benchmark):
    for db_name, func in [
        ("SQLite", sqlite_query),
        ("MySQL", mysql_query),
        ("Redis", redis_query)
    ]:
        benchmark.extra_info = {"db": db_name, "author": author}
        benchmark.group = f"query_{db_name}"
        benchmark(lambda: func(author))
