from downloader import download_books
from indexer import Indexer

if __name__ == '__main__':
    urls = [
       "https://www.gutenberg.org/cache/epub/1342/pg1342.txt",
       "https://www.gutenberg.org/cache/epub/84/pg84.txt",
       "https://www.gutenberg.org/cache/epub/11/pg11.txt",
       "https://www.gutenberg.org/cache/epub/74/pg74.txt",
       "https://www.gutenberg.org/cache/epub/1080/pg1080.txt"
    ]

    print('Starting pipeline...')
    download_books(urls)
    print('Book database updated.')
    print('Indexing books (skips already indexed)')
    indexer = Indexer()
    indexer.index_all_books()

    stats = indexer.get_stats()
    print(f'\nPipeline complete!')
    print(f'Total books indexed: {stats["total_books"]}')
    print(f'Unique words: {stats["unique_words"]}')
