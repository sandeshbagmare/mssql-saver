"""FastAPI dependency injection helpers."""
from collections.abc import Generator

from sqlalchemy.orm import Session

from app.db.session import PgSession


def get_db() -> Generator[Session, None, None]:
    """Yield a Postgres SQLAlchemy session; commit handled by manager layer."""
    db = PgSession()
    try:
        yield db
    finally:
        db.close()
