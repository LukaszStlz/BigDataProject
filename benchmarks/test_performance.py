"""
Performance Benchmarks - Speed and Resource Monitoring
Purpose: Measure system performance and resource usage
Run: pytest benchmarks/ --benchmark-only -v
"""

import pytest
import sys
import os
import psutil
import time
sys.path.append('/app')

from storage_backends import RedisBackend, PostgreSQLBackend
from indexer import Indexer


@pytest.fixture
def benchmark_book_data():
    """Larger book data for performance testing"""
    return {
        'book_id': 'bench_001',
        'title': 'Performance Benchmark Book',
        'author': 'Benchmark Author',
        'language': 'en',
        'all_words': {f'word_{i}' for i in range(1000)},  # 1000 unique words
        'word_count': 5000
    }


@pytest.fixture
def multiple_books_data():
    """Multiple books for load testing"""
    books = []
    for i in range(10):
        books.append({
            'book_id': f'bench_book_{i:03d}',
            'title': f'Benchmark Book {i}',
            'author': f'Author {i}',
            'language': 'en',
            'all_words': {f'word_{j}' for j in range(i*10, i*10 + 100)},
            'word_count': 1000 + i*100
        })
    return books


@pytest.mark.benchmark(group="indexing")
def test_redis_indexing_speed(benchmark, benchmark_book_data):
    """Benchmark Redis indexing performance"""
    backend = RedisBackend()
    indexer = Indexer(backend)

    def index_operation():
        return indexer.index_book(benchmark_book_data)

    result = benchmark(index_operation)
    return result


@pytest.mark.benchmark(group="indexing")
def test_postgres_indexing_speed(benchmark, benchmark_book_data):
    """Benchmark PostgreSQL indexing performance"""
    backend = PostgreSQLBackend()
    indexer = Indexer(backend)

    def index_operation():
        return indexer.index_book(benchmark_book_data)

    result = benchmark(index_operation)
    return result


@pytest.mark.benchmark(group="search")
def test_redis_search_speed(benchmark):
    """Benchmark Redis search performance"""
    backend = RedisBackend()
    indexer = Indexer(backend)

    setup_data = {
        'book_id': 'search_test_001',
        'title': 'Search Performance Test',
        'author': 'Search Author',
        'language': 'en',
        'all_words': {'performance', 'search', 'test', 'benchmark'},
        'word_count': 200
    }
    indexer.index_book(setup_data)

    def search_operation():
        return indexer.search_books("performance test")

    result = benchmark(search_operation)
    return result


@pytest.mark.benchmark(group="search")
def test_postgres_search_speed(benchmark):
    """Benchmark PostgreSQL search performance"""
    backend = PostgreSQLBackend()
    indexer = Indexer(backend)

    setup_data = {
        'book_id': 'search_test_002',
        'title': 'Search Performance Test',
        'author': 'Search Author',
        'language': 'en',
        'all_words': {'performance', 'search', 'test', 'benchmark'},
        'word_count': 200
    }
    indexer.index_book(setup_data)

    def search_operation():
        return indexer.search_books("performance test")

    result = benchmark(search_operation)
    return result


def test_multiple_books_indexing_redis(multiple_books_data):
    """Test Redis performance with multiple books"""
    backend = RedisBackend()
    indexer = Indexer(backend)

    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB

    for book_data in multiple_books_data:
        indexer.index_book(book_data)

    end_time = time.time()
    end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB

    indexing_time = end_time - start_time
    memory_used = end_memory - start_memory

    print(f"\nRedis Load Test Results:")
    print(f"  Books indexed: {len(multiple_books_data)}")
    print(f"  Total time: {indexing_time:.2f} seconds")
    print(f"  Time per book: {indexing_time/len(multiple_books_data):.3f} seconds")
    print(f"  Memory used: {memory_used:.2f} MB")

    assert indexing_time < 10.0, "Indexing too slow"
    assert memory_used < 100.0, "Memory usage too high"


def test_multiple_books_indexing_postgres(multiple_books_data):
    """Test PostgreSQL performance with multiple books"""
    backend = PostgreSQLBackend()
    indexer = Indexer(backend)

    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB

    for book_data in multiple_books_data:
        indexer.index_book(book_data)

    end_time = time.time()
    end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB

    indexing_time = end_time - start_time
    memory_used = end_memory - start_memory

    print(f"\nPostgreSQL Load Test Results:")
    print(f"  Books indexed: {len(multiple_books_data)}")
    print(f"  Total time: {indexing_time:.2f} seconds")
    print(f"  Time per book: {indexing_time/len(multiple_books_data):.3f} seconds")
    print(f"  Memory used: {memory_used:.2f} MB")

    assert indexing_time < 15.0, "Indexing too slow"
    assert memory_used < 100.0, "Memory usage too high"


def test_memory_usage_monitoring():
    """Monitor memory usage during operations"""
    process = psutil.Process()
    initial_memory = process.memory_info().rss / 1024 / 1024  # MB

    backend = RedisBackend()
    indexer = Indexer(backend)

    large_book = {
        'book_id': 'memory_test_001',
        'title': 'Memory Test Book',
        'author': 'Memory Author',
        'language': 'en',
        'all_words': {f'memword_{i}' for i in range(5000)},  # 5000 words
        'word_count': 20000
    }

    indexer.index_book(large_book)

    final_memory = process.memory_info().rss / 1024 / 1024  # MB
    memory_increase = final_memory - initial_memory

    print(f"\nMemory Usage Monitoring:")
    print(f"  Initial memory: {initial_memory:.2f} MB")
    print(f"  Final memory: {final_memory:.2f} MB")
    print(f"  Memory increase: {memory_increase:.2f} MB")

    assert memory_increase < 50.0, "Memory leak detected"


def test_your_performance_benchmark_template():
    """Kacper, dodaj inne benchmarki jak chcesz a jak nie przygotuj tylko wykresy na podstawie tego co jest u gory"""
