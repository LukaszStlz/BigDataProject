import redis
import re
import os
import time
from typing import Set, Dict, List
from pathlib import Path

class Indexer:
    def __init__(self, redis_host='redis', redis_port=6379):
        self.redis_client = redis.Redis(host=redis_host, decode_responses=True)
        self.datalake_path = Path('/app/datalake')

    def tokenize_text(self, text: str) -> Set[str]:
        '''extract words, normalize to lowercase, remove punctuaction'''
        words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
        return set(word for word in words if len(word) > 2)

    def is_book_indexed(self, book_id: str) -> bool:
        '''check if book is already indexed'''
        return self.redis_client.exists(f'book:{book_id}:metadata') > 0

    def get_indexed_books(self) -> Set[str]:
        '''get all indexed books IDs'''
        pattern = 'book:*:metadata'
        keys = self.redis_client.keys(pattern)
        return {key.split(':')[1] for key in keys}

    def process_book(self, book_id: str) -> Dict:
        '''pricess single book and return indexing data'''
        header_file = self.datalake_path / f'header_{book_id}.txt'
        body_file = self.datalake_path / f'body_{book_id}.txt'

        if not header_file.exists() or not body_file.exists():
            raise FileNotFoundError(f'Missing files for book {book_id}')

        with open(header_file, 'r', encoding='utf-8') as f:
            header_content = f.read().strip()

        with open(body_file, 'r', encoding='utf-8') as f:
            body_content = f.read()

        all_words = self.tokenize_text(header_content + ' ' + body_content)
        title_words = self.tokenize_text(header_content)

        return {
            'book_id': book_id,
            'title': header_content,
            'all_words': all_words,
            'title_words': title_words,
            'word_count': len(body_content.split())
        }

    def index_book(self, book_data: Dict):
        '''index a single book, puts to redis - inverted index creation'''
        pipe = self.redis_client.pipeline()
        book_id = book_data['book_id']

        pipe.hset(f'book:{book_id}:metadata', mapping={
            'title': book_data['title'],
            'word_count': str(book_data['word_count']),
            'unique_words': str(len(book_data['all_words'])),
            'indexed_at': str(int(time.time()))
        })

        for word in book_data['all_words']:
            pipe.sadd(f'word:{word}', book_id)

        for word in book_data['title_words']:
            pipe.sadd(f'title_word:{word}', book_id)

        pipe.incr('stats:total_books')
        pipe.sadd('stats:all_words', *book_data['all_words'])

        pipe.execute()

    def index_all_books(self, force_reindex: bool = False):
        '''index all books, reindex if specified'''
        book_files = list(self.datalake_path.glob('header_*.txt'))
        book_ids = [f.stem.replace('header_', '') for f in book_files]

        if not force_reindex:
            indexed_books = self.get_indexed_books()
            books_to_index = [bid for bid in book_ids if bid not in indexed_books]
            skipped_count = len(book_ids) - len(books_to_index)

            print(f'Found {len(book_ids)} books total')
            print(f'Skipping {skipped_count} already indexed')

            print(f'Indexing {len(books_to_index)} new books')
        else:
            books_to_index = book_ids
            print(f'Force reindexing all {len(book_ids)} books')

        if not books_to_index:
            print(f'No new books to index!!')
            return

        for i, book_id in enumerate(books_to_index, 1):
            try:
                book_data = self.process_book(book_id)
                self.index_book(book_data)
                print(f'Indexed book {i}/{len(books_to_index)}: {book_id}')
            except Exception as e:
                print(f'Error indexing book {book_id}: {e}')

        print('Indexing complete!')

    def search_books(self, query: str) -> List[str]:
        '''search for books containing query'''
        words = self.tokenize_text(query)
        if not words:
            return []

        book_sets = [self.redis_client.smembers(f'word:{word}') for word in words]
        if not book_sets:
            return []

        result_books = set(book_sets[0])
        for book_set in book_sets[1:]:
            result_books &= book_set

        return list(result_books)

    def get_book_info(self, book_id: str) -> Dict:
        '''gives book metadata'''
        return self.redis_client.hgetall(f'book:{book_id}:metadata')

    def get_stats(self) -> Dict:
        '''gives indexing statistics'''
        return {
            'total_books': self.redis_client.get('stats:total_books') or 0,
            'unique_words': self.redis_client.scard('stats:all_words'),
            'indexed_books': len(self.get_indexed_books())
        }

    # to be moved elsewhere
    def test_redis_connection(self):
        try:
            self.redis_client.ping()
            print(f'Redis connection: OK')
            return True
        except:
            print('Redis connection: FAILED')
            return False
