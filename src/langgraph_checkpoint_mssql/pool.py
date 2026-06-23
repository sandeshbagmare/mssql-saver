"""Thread-safe pyodbc connection pool."""
from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator

import pyodbc


class ConnectionPool:
    """A simple, thread-safe pool of pyodbc connections.

    Connections are acquired via the `connection()` context manager.
    Successful exit commits; any exception rolls back and discards the connection.
    """

    def __init__(self, conn_str: str, pool_size: int = 10) -> None:
        self._conn_str = conn_str
        self._pool_size = pool_size
        self._pool: list[pyodbc.Connection] = []
        self._lock = threading.Lock()
        self._semaphore = threading.Semaphore(pool_size)

    @contextmanager
    def connection(self) -> Iterator[pyodbc.Connection]:
        """Yield a connection, commit on clean exit, rollback + discard on error."""
        self._semaphore.acquire()
        conn: pyodbc.Connection | None = None
        try:
            with self._lock:
                conn = self._pool.pop() if self._pool else None
            if conn is None:
                # MARS=yes allows multiple active result sets on one connection,
                # which is required when _row_to_tuple opens sub-cursors
                # (for blob/write lookups) while the main cursor is still open.
                conn_str = self._conn_str
                if "MARS_Connection" not in conn_str and "mars_connection" not in conn_str.lower():
                    conn_str = conn_str.rstrip(";") + ";MARS_Connection=yes;"
                conn = pyodbc.connect(conn_str, autocommit=False)
            yield conn
            conn.commit()
        except Exception:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
                conn = None
            raise
        finally:
            if conn is not None:
                with self._lock:
                    if len(self._pool) < self._pool_size:
                        self._pool.append(conn)
                    else:
                        conn.close()
            self._semaphore.release()

    def close(self) -> None:
        """Close all pooled connections."""
        with self._lock:
            for conn in self._pool:
                try:
                    conn.close()
                except Exception:
                    pass
            self._pool.clear()

    @classmethod
    def from_conn_string(cls, conn_str: str, pool_size: int = 10) -> "ConnectionPool":
        return cls(conn_str, pool_size)
