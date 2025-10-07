from .downloader import download_books
from .indexer import Indexer
from .storage_backends import RedisBackend, PostgreSQLBackend
import sys
import os

def run_pipeline(backend_name='redis'):
    """Run the data pipeline with specified backend"""

    urls = [
       "https://www.gutenberg.org/cache/epub/1342/pg1342.txt",
       "https://www.gutenberg.org/cache/epub/84/pg84.txt",
       "https://www.gutenberg.org/cache/epub/11/pg11.txt",
       "https://www.gutenberg.org/cache/epub/74/pg74.txt",
       "https://www.gutenberg.org/cache/epub/1080/pg1080.txt"
    ]

    print(f'Starting pipeline with {backend_name} backend...')

    download_books(urls)
    print('Book database updated.')

    if backend_name.lower() == 'redis':
        backend = RedisBackend()
        print('Using Redis datamart')
    elif backend_name.lower() == 'postgres':
        backend = PostgreSQLBackend()
        print('Using PostgreSQL datamart')
    else:
        print(f'Unknown backend: {backend_name}. Using Redis as default.')
        backend = RedisBackend()

    indexer = Indexer(backend)
    if not indexer.test_backend_connection():
        print(f'Failed to connect to {backend_name} backend!')
        return False

    print('Indexing books (skips already indexed)')
    indexer.index_all_books()

    stats = indexer.get_stats()
    print(f'\nPipeline complete!')
    print(f'Backend: {backend_name}')
    print(f'Total books indexed: {stats["total_books"]}')
    print(f'Unique words: {stats["unique_words"]}')

if __name__ == '__main__':
    backend = 'redis'
    if len(sys.argv) > 1:
        backend = sys.argv[1]

    backend = os.getenv('BACKEND_TYPE', backend)

    print(f'Running pipeline with backend: {backend}')
    run_pipeline(backend)
