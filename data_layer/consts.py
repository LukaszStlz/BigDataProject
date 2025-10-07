
from pathlib import Path


DATALAKE_PATH   = "/app/datalake"
DATAMART_PATH   = "/app/datamarts"  
CONTROL_PATH    = "/app/control"

Path(DATALAKE_PATH).mkdir(parents=True, exist_ok=True)
Path(DATAMART_PATH).mkdir(parents=True, exist_ok=True)
Path(CONTROL_PATH).mkdir(parents=True, exist_ok=True)

DOWNLOADED_BOOKS_FILE = str(Path(CONTROL_PATH) / "downloaded_books.txt")
INDEXED_BOOKS_FILE    = str(Path(CONTROL_PATH) / "indexed_books.txt")
