from typing import Iterable, List, Tuple, Dict, Union
import pymysql

MYSQL_HOST = "mysql"
MYSQL_USER = "appuser"
MYSQL_PASSWORD = "apppass"
MYSQL_DB = "bigdata_stage1"
MYSQL_PORT = 3306

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS books (
    book_id     VARCHAR(32) PRIMARY KEY,
    title       TEXT,
    author      VARCHAR(255),
    language    VARCHAR(16),
    header_path TEXT,
    body_path   TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

def _conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        port=MYSQL_PORT,
        autocommit=True,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,
    )

def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s
        """,
        (MYSQL_DB, table, column),
    )
    return cur.fetchone() is not None

def _ensure_column(cur, table: str, column: str, ddl_tail: str) -> None:
    if not _column_exists(cur, table, column):
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {ddl_tail}")

def _ensure_index(cur, table: str, index_name: str, column_expr: str) -> None:
    cur.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND INDEX_NAME=%s
        """,
        (MYSQL_DB, table, index_name),
    )
    if cur.fetchone():
        return
    cur.execute(f"CREATE INDEX {index_name} ON {table}({column_expr})")

RowType = Union[
    Tuple[str, str, str, str, str, str],
    Dict[str, str],
]

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
        with con.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            _ensure_column(cur, "books", "header_path", "header_path TEXT")
            _ensure_column(cur, "books", "body_path",   "body_path   TEXT")
            _ensure_index(cur, "books", "idx_author",   "author")
            _ensure_index(cur, "books", "idx_language", "language")
            _ensure_index(cur, "books", "idx_title",    "title(255)")
    finally:
        con.close()

def bulk_upsert(rows: Iterable[RowType]) -> None:
    data = [_row_to_tuple(r) for r in rows]
    if not data:
        return
    con = _conn()
    try:
        with con.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO books (book_id, title, author, language, header_path, body_path)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    title=VALUES(title),
                    author=VALUES(author),
                    language=VALUES(language),
                    header_path=VALUES(header_path),
                    body_path=VALUES(body_path)
                """,
                data,
            )
    finally:
        con.close()

def find_by_author(author: str) -> List[Tuple[str, str, str, str, str, str]]:
    con = _conn()
    try:
        with con.cursor() as cur:
            cur.execute(
                "SELECT book_id, title, author, language, header_path, body_path FROM books WHERE author=%s",
                (author,),
            )
            return list(cur.fetchall())
    finally:
        con.close()
