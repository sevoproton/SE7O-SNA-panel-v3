"""
Database abstraction layer — supports SQLite (default) and PostgreSQL.
Uses aiosqlite for async SQLite and asyncpg for PostgreSQL.
"""
import asyncio
import os
from typing import Any, Optional

import aiosqlite
from app.config import settings

try:
    import asyncpg
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False

_db_conn: Optional[aiosqlite.Connection] = None
_db_lock = asyncio.Lock()
_pg_pool: Optional[Any] = None

if settings.database_url and HAS_POSTGRES:
    DB_BACKEND = "postgresql"
else:
    DB_BACKEND = "sqlite"

INTEGRITY_ERRORS = (aiosqlite.IntegrityError,)
if HAS_POSTGRES and DB_BACKEND == "postgresql":
    INTEGRITY_ERRORS = (aiosqlite.IntegrityError, asyncpg.exceptions.UniqueViolationError)


async def init_db():
    global _db_conn, _pg_pool
    if DB_BACKEND == "postgresql":
        _pg_pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=10)
        async with _pg_pool.acquire() as conn:
            await conn.execute(SCHEMA_POSTGRES)
            for col, coltype in LINKS_MIGRATIONS:
                try:
                    await conn.execute(f"ALTER TABLE links ADD COLUMN IF NOT EXISTS {col} {coltype}")
                except Exception:
                    pass
    else:
        db_path = settings.db_path
        try:
            test_file = os.path.join(os.path.dirname(db_path), ".write_test")
            with open(test_file, "w") as f:
                f.write("ok")
            os.remove(test_file)
        except Exception:
            db_path = "/tmp/panel.db"
        _db_conn = await aiosqlite.connect(db_path)
        _db_conn.row_factory = aiosqlite.Row
        await _db_conn.execute("PRAGMA journal_mode=WAL")
        await _db_conn.executescript(SCHEMA_SQLITE)
        await _db_conn.commit()

        cur = await _db_conn.execute("PRAGMA table_info(links)")
        existing_cols = {row[1] for row in await cur.fetchall()}
        for col, coltype in LINKS_MIGRATIONS:
            if col not in existing_cols:
                try:
                    await _db_conn.execute(f"ALTER TABLE links ADD COLUMN {col} {coltype}")
                    await _db_conn.commit()
                except Exception:
                    pass


async def close_db():
    global _db_conn
    if DB_BACKEND == "sqlite" and _db_conn:
        await _db_conn.close()


async def execute(sqlite_q, pg_q="", params=()):
    if DB_BACKEND == "postgresql":
        async with _pg_pool.acquire() as conn:
            await conn.execute(pg_q or sqlite_q, *params)
    else:
        async with _db_lock:
            await _db_conn.execute(sqlite_q, params)
            await _db_conn.commit()


async def fetchall(sqlite_q, pg_q="", params=()):
    if DB_BACKEND == "postgresql":
        async with _pg_pool.acquire() as conn:
            rows = await conn.fetch(pg_q or sqlite_q, *params)
            return [dict(r) for r in rows]
    else:
        async with _db_lock:
            cur = await _db_conn.execute(sqlite_q, params)
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def fetchone(sqlite_q, pg_q="", params=()):
    if DB_BACKEND == "postgresql":
        async with _pg_pool.acquire() as conn:
            row = await conn.fetchrow(pg_q or sqlite_q, *params)
            return dict(row) if row else None
    else:
        async with _db_lock:
            cur = await _db_conn.execute(sqlite_q, params)
            row = await cur.fetchone()
            return dict(row) if row else None


LINKS_COLS = (
    "uid TEXT PRIMARY KEY, label TEXT NOT NULL, "
    "limit_bytes BIGINT DEFAULT 0, used_bytes BIGINT DEFAULT 0, "
    "max_connections INT DEFAULT 0, created_at TEXT NOT NULL, "
    "active BOOLEAN DEFAULT TRUE, expires_at TEXT, "
    "custom_path TEXT DEFAULT '', custom_sni TEXT DEFAULT '', "
    "custom_host TEXT DEFAULT '', custom_fp TEXT DEFAULT 'chrome', "
    "color TEXT DEFAULT '#39ff14', flag TEXT DEFAULT '', fragment TEXT DEFAULT '', "
    "transport TEXT DEFAULT 'ws'"
)

# Columns added after the initial release — applied via ALTER TABLE for
# databases created before the column existed. (uid TEXT PRIMARY KEY, so a
# fresh install already gets these via LINKS_COLS / CREATE TABLE.)
LINKS_MIGRATIONS = [
    ("transport", "TEXT DEFAULT 'ws'"),
]

SCHEMA_SQLITE = (
    "CREATE TABLE IF NOT EXISTS links (" + LINKS_COLS + ");"
    "CREATE TABLE IF NOT EXISTS hourly_traffic (hour TEXT PRIMARY KEY, bytes INTEGER DEFAULT 0);"
    "CREATE TABLE IF NOT EXISTS daily_traffic (day TEXT PRIMARY KEY, bytes INTEGER DEFAULT 0);"
    "CREATE TABLE IF NOT EXISTS custom_addresses (id INTEGER PRIMARY KEY AUTOINCREMENT, address TEXT NOT NULL UNIQUE);"
    "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);"
    "CREATE TABLE IF NOT EXISTS login_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL, ip TEXT, success INTEGER DEFAULT 1, user_agent TEXT DEFAULT '', path TEXT DEFAULT '');"
)

SCHEMA_POSTGRES = (
    "CREATE TABLE IF NOT EXISTS links (" + LINKS_COLS + ");"
    "CREATE TABLE IF NOT EXISTS hourly_traffic (hour TEXT PRIMARY KEY, bytes BIGINT DEFAULT 0);"
    "CREATE TABLE IF NOT EXISTS daily_traffic (day TEXT PRIMARY KEY, bytes BIGINT DEFAULT 0);"
    "CREATE TABLE IF NOT EXISTS custom_addresses (id SERIAL PRIMARY KEY, address TEXT NOT NULL UNIQUE);"
    "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);"
    "CREATE TABLE IF NOT EXISTS login_logs (id SERIAL PRIMARY KEY, timestamp TEXT NOT NULL, ip TEXT, success BOOLEAN DEFAULT TRUE, user_agent TEXT DEFAULT '', path TEXT DEFAULT '');"
)
