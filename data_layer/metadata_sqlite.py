from typing import Iterable, List, Tuple, Dict, Union
import sqlite3
from pathlib import Path

SQLITE_PATH = Path("/app/datamarts/metadata.sqlite3")
SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS books (
    book_id     TEXT PRIMARY KEY,
    title       TEXT,
    author      TEXT,
    language    TEXT,
    header_path TEXT,
    body_path   TEXT
);
"""

RowType = Union[
    Tuple[str, str, str, str, str, str],  # (book_id, title, author, language, header_path, body_path)
    Dict[str, str],
]

def _conn():
    return sqlite3.connect(str(SQLITE_PATH))

def _row_to_tuple(row: RowType) -> Tuple[str, str, str, str, str, str]:
    if isinstance(row, dict):
        return (
            row["book_id"],
            row.get("title", "") or "",
            row.get("author", "") or "",
            row.get("language", "") or "",
            row.get("header_path", "") or "",
            row.get("body_path", "") or "",
        )
    return (
        row[0], row[1], row[2], row[3], row[4], row[5]
    )

def init_db() -> None:
    con = _conn()
    try:
        cur = con.cursor()
        cur.execute(SCHEMA_SQL)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_author   ON books(author)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_language ON books(language)")
        con.commit()
    finally:
        con.close()

def bulk_upsert(rows: Iterable[RowType]) -> None:
    data = [_row_to_tuple(r) for r in rows]
    if not data:
        return
    con = _conn()
    try:
        cur = con.cursor()
        cur.executemany(
            """
            INSERT INTO books (book_id, title, author, language, header_path, body_path)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(book_id) DO UPDATE SET
                title=excluded.title,
                author=excluded.author,
                language=excluded.language,
                header_path=excluded.header_path,
                body_path=excluded.body_path
            """,
            data,
        )
        con.commit()
    finally:
        con.close()

def find_by_author(author: str) -> List[Tuple[str, str, str, str, str, str]]:
    con = _conn()
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT book_id, title, author, language, header_path, body_path FROM books WHERE author=?",
            (author,),
        )
        return list(cur.fetchall())
    finally:
        con.close()
