"""
Functional Tests - Unit and Integration Testing
Purpose: Verify system works correctly
Run: pytest tests/ -v
"""

import pytest
import sys
import os
sys.path.append('/app')
sys.path.append('/app/application')

from storage_backends import RedisBackend, PostgreSQLBackend
from indexer import Indexer


@pytest.fixture
def sample_book_data():
    """Sample book data for testing"""
    return {
        'book_id': 'test_func_001',
        'title': 'Functional Test Book',
        'author': 'Test Author',
        'language': 'en',
        'all_words': {'functional', 'test', 'book', 'pytest'},
        'word_count': 100
    }


@pytest.mark.parametrize("backend_class", [RedisBackend, PostgreSQLBackend])
def test_backend_connection(backend_class):
    """Test backend connections work"""
    backend = backend_class()
    assert backend.test_connection(), f"{backend_class.__name__} connection failed"


def test_metadata_storage_retrieval():
    """Test metadata is stored and retrieved correctly"""
    backend = RedisBackend()
    test_metadata = {
        'title': 'Test Title',
        'author': 'Test Author',
        'language': 'en',
        'word_count': 100,
        'unique_words': 50
    }

    backend.store_book_metadata('test_meta_001', test_metadata)
    retrieved = backend.get_book_metadata('test_meta_001')

    assert retrieved['title'] == 'Test Title'
    assert retrieved['author'] == 'Test Author'


def test_word_index_functionality():
    """Test word indexing and search works"""
    backend = RedisBackend()

    backend.add_word_to_index('python', 'book_001')
    backend.add_word_to_index('python', 'book_002')
    backend.add_word_to_index('java', 'book_001')

    python_books = backend.search_word('python')
    java_books = backend.search_word('java')

    assert 'book_001' in python_books
    assert 'book_002' in python_books
    assert 'book_001' in java_books
    assert 'book_002' not in java_books


def test_cross_backend_consistency(sample_book_data):
    """Test both backends produce same results"""
    redis_backend = RedisBackend()
    postgres_backend = PostgreSQLBackend()

    redis_indexer = Indexer(redis_backend)
    postgres_indexer = Indexer(postgres_backend)

    redis_indexer.index_book(sample_book_data)
    postgres_indexer.index_book(sample_book_data)

    redis_search = redis_indexer.search_books("functional test")
    postgres_search = postgres_indexer.search_books("functional test")

    assert set(redis_search) == set(postgres_search)


def test_missing_book_metadata():
    """Test handling of non-existent book metadata"""
    backend = RedisBackend()
    result = backend.get_book_metadata('nonexistent_book')
    assert result == {} or result is None


def test_empty_search_query():
    """Test handling of empty search queries"""
    backend = RedisBackend()
    indexer = Indexer(backend)
    results = indexer.search_books("")
    assert results == []



def test_your_functional_test_template():
    """Patrycja, na wzor napisalem kilka testow, dodaj cos jak masz ochote zgodnie mniej wiecej z tym stylem co te u gory."""
