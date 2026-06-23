"""SQLAlchemy engine + session factories for Postgres and MSSQL."""
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from app.core.config import settings

# Postgres engine — use psycopg v3 driver (postgresql+psycopg://...)
_pg_url = settings.pg_dsn.replace("postgresql://", "postgresql+psycopg://", 1)
pg_engine = create_engine(_pg_url, pool_size=settings.pool_size, future=True)
PgSession = sessionmaker(bind=pg_engine, expire_on_commit=False)

# MSSQL engine — URL-encode the ODBC connection string
_odbc = urllib.parse.quote_plus(settings.mssql_conn_str)
mssql_engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={_odbc}",
    pool_size=settings.pool_size,
    future=True,
)
MssqlSession = sessionmaker(bind=mssql_engine, expire_on_commit=False)


def get_pg_session() -> Session:
    return PgSession()


def get_mssql_session() -> Session:
    return MssqlSession()
