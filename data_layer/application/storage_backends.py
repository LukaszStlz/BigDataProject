from abc import ABC, abstractmethod
from typing import Dict, List, Set
import redis
import psycopg2
from psycopg2.extras import DictCursor
import json
import time
from pathlib import Path

class StorageBackend(ABC):

    @abstractmethod
    def store_book_metadata(self, book_id: str, metadata: Dict) -> None:
        pass

    @abstractmethod
    def get_book_metadata(self, book_id: str) -> Dict:
        pass

    @abstractmethod
    def is_book_indexed(self, book_id: str) -> bool:
        pass

    @abstractmethod
    def get_indexed_books(self) -> Set[str]:
        pass

    @abstractmethod
    def add_word_to_index(self, word: str, book_id: str) -> None:
        pass

    @abstractmethod
    def search_word(self, word: str) -> Set[str]:
        pass

    @abstractmethod
    def get_stats(self) -> Dict:
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        pass

class RedisBackend(StorageBackend):
    def __init__(self, host='redis', port=6379):
        self.redis_client = redis.Redis(host=host, port=port, decode_responses=True)

    def store_book_metadata(self, book_id: str, metadata: Dict) -> None:
        pipe = self.redis_client.pipeline()
        pipe.hset(f'book:{book_id}:metadata', mapping={
            'title': metadata.get('title', ''),
            'author': metadata.get('author', ''),
            'language': metadata.get('language', ''),
            'word_count': str(metadata.get('word_count', 0)),
            'unique_words': str(metadata.get('unique_words', 0)),
            'indexed_at': str(int(time.time()))
        })
        pipe.incr('stats:total_books')
        pipe.execute()

    def get_book_metadata(self, book_id: str) -> Dict:
        return self.redis_client.hgetall(f'book:{book_id}:metadata')

    def is_book_indexed(self, book_id: str) -> bool:
        return self.redis_client.exists(f'book:{book_id}:metadata') > 0

    def get_indexed_books(self) -> Set[str]:
        pattern = 'book:*:metadata'
        keys = self.redis_client.keys(pattern)
        return {key.split(':')[1] for key in keys}

    def add_word_to_index(self, word: str, book_id: str) -> None:
        self.redis_client.sadd(f'word:{word}', book_id)
        self.redis_client.sadd('stats:all_words', word)

    def search_word(self, word: str) -> Set[str]:
        return self.redis_client.smembers(f'word:{word}')

    def get_stats(self) -> Dict:
        return {
            'total_books': int(self.redis_client.get('stats:total_books') or 0),
            'unique_words': self.redis_client.scard('stats:all_words'),
            'indexed_books': len(self.get_indexed_books())
        }

    def test_connection(self) -> bool:
        try:
            self.redis_client.ping()
            return True
        except:
            return False

class PostgreSQLBackend(StorageBackend):
    def __init__(self, host='postgres_db', port=5432, user='user', password='password', database='datamart_db'):
        self.conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, dbname=database
        )
        self._initialize_db()

    def _initialize_db(self):
        with self.conn.cursor() as cur:
            cur.execute('''
                CREATE TABLE IF NOT EXISTS books (
                    book_id VARCHAR PRIMARY KEY,
                    title TEXT,
                    author TEXT,
                    language VARCHAR(10),
                    word_count INTEGER,
                    unique_words INTEGER,
                    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS word_index (
                    word VARCHAR,
                    book_id VARCHAR,
                    PRIMARY KEY (word, book_id)
                )
            ''')
            cur.execute('''
                CREATE INDEX IF NOT EXISTS idx_word_index_word ON word_index(word)
            ''')
        self.conn.commit()

    def store_book_metadata(self, book_id: str, metadata: Dict) -> None:
        with self.conn.cursor() as cur:
            cur.execute('''
                INSERT INTO books (book_id, title, author, language, word_count, unique_words)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (book_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    author = EXCLUDED.author,
                    language = EXCLUDED.language,
                    word_count = EXCLUDED.word_count,
                    unique_words = EXCLUDED.unique_words,
                    indexed_at = CURRENT_TIMESTAMP
            ''', (
                book_id,
                metadata.get('title', ''),
                metadata.get('author', ''),
                metadata.get('language', ''),
                metadata.get('word_count', 0),
                metadata.get('unique_words', 0)
            ))
        self.conn.commit()

    def get_book_metadata(self, book_id: str) -> Dict:
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute('SELECT * FROM books WHERE book_id = %s', (book_id,))
            row = cur.fetchone()
            return dict(row) if row else {}

    def is_book_indexed(self, book_id: str) -> bool:
        with self.conn.cursor() as cur:
            cur.execute('SELECT EXISTS(SELECT 1 FROM books WHERE book_id = %s)', (book_id,))
            return cur.fetchone()[0]

    def get_indexed_books(self) -> Set[str]:
        with self.conn.cursor() as cur:
            cur.execute('SELECT book_id FROM books')
            return {r[0] for r in cur.fetchall()}

    def add_word_to_index(self, word: str, book_id: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute('''
                INSERT INTO word_index (word, book_id) VALUES (%s, %s)
                ON CONFLICT (word, book_id) DO NOTHING
            ''', (word, book_id))
        self.conn.commit()

    def search_word(self, word: str) -> Set[str]:
        with self.conn.cursor() as cur:
            cur.execute('SELECT book_id FROM word_index WHERE word = %s', (word,))
            return {r[0] for r in cur.fetchall()}

    def get_stats(self) -> Dict:
        with self.conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM books')
            total_books = cur.fetchone()[0]
            cur.execute('SELECT COUNT(DISTINCT word) FROM word_index')
            unique_words = cur.fetchone()[0]
        return {
            'total_books': total_books,
            'unique_words': unique_words,
            'indexed_books': total_books
        }

    def test_connection(self) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute('SELECT 1')
                cur.fetchone()
            return True
        except:
            return False
