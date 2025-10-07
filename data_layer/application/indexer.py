import re
import os
import time
from typing import Set, Dict, List
from pathlib import Path
from .storage_backends import StorageBackend

class Indexer:
    def __init__(self, backend: StorageBackend):
        self.backend = backend
        self.datalake_path = Path('/app/datalake')

    def tokenize_text(self, text: str) -> Set[str]:
        '''extract words, normalize to lowercase, remove punctuaction'''
        words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
        return set(word for word in words if len(word) > 2)

    def is_book_indexed(self, book_id: str) -> bool:
        '''check if book is already indexed'''
        return self.backend.is_book_indexed(book_id)

    def get_indexed_books(self) -> Set[str]:
        '''get all indexed books IDs'''
        return self.backend.get_indexed_books()

    def extract_metadata_from_header(self, header_content: str) -> Dict:
        '''extract metadata from header using regex'''
        import re

        metadata = {'title': '', 'author': '', 'language': 'en'}

        title_match = re.search(r'Title:\s*(.+)', header_content, re.IGNORECASE)
        if title_match:
            metadata['title'] = title_match.group(1).strip()

        author_match = re.search(r'Author:\s*(.+)', header_content, re.IGNORECASE)
        if author_match:
            metadata['author'] = author_match.group(1).strip()

        lang_match = re.search(r'Language:\s*(.+)', header_content, re.IGNORECASE)
        if lang_match:
            metadata['language'] = lang_match.group(1).strip()

        return metadata

    def process_book(self, book_id: str) -> Dict:
        '''process single book and return indexing data'''
        header_file = self.datalake_path / f'header_{book_id}.txt'
        body_file = self.datalake_path / f'body_{book_id}.txt'

        if not header_file.exists() or not body_file.exists():
            raise FileNotFoundError(f'Missing files for book {book_id}')

        with open(header_file, 'r', encoding='utf-8') as f:
            header_content = f.read().strip()

        with open(body_file, 'r', encoding='utf-8') as f:
            body_content = f.read()

        metadata = self.extract_metadata_from_header(header_content)

        all_words = self.tokenize_text(body_content)
        title_words = self.tokenize_text(metadata['title'])

        return {
            'book_id': book_id,
            'title': metadata['title'],
            'author': metadata['author'],
            'language': metadata['language'],
            'all_words': all_words,
            'title_words': title_words,
            'word_count': len(body_content.split())
        }

    def index_book(self, book_data: Dict):
        '''index a single book using backend interface'''
        book_id = book_data['book_id']

        metadata = {
            'title': book_data['title'],
            'author': book_data.get('author', ''),
            'language': book_data.get('language', ''),
            'word_count': book_data['word_count'],
            'unique_words': len(book_data['all_words'])
        }
        self.backend.store_book_metadata(book_id, metadata)

        for word in book_data['all_words']:
            self.backend.add_word_to_index(word, book_id)

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

        book_sets = [self.backend.search_word(word) for word in words]
        if not book_sets:
            return []

        result_books = book_sets[0]
        for book_set in book_sets[1:]:
            result_books &= book_set

        return list(result_books)

    def get_book_info(self, book_id: str) -> Dict:
        '''gives book metadata'''
        return self.backend.get_book_metadata(book_id)

    def get_stats(self) -> Dict:
        '''gives indexing statistics'''
        return self.backend.get_stats()

    def test_backend_connection(self):
        '''test backend connection'''
        if self.backend.test_connection():
            print('Backend connection: OK')
            return True
        else:
            print('Backend connection: FAILED')
            return False
