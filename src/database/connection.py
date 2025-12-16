import logging
from contextlib import contextmanager
from typing import Generator
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

from src.config import Config

logger = logging.getLogger(__name__)

# Global engine instance
_engine: Engine | None = None
_SessionLocal: sessionmaker | None = None


def get_db_engine() -> Engine:
    """Get or create SQLAlchemy engine with connection pooling.

    Returns:
        SQLAlchemy Engine instance
    """
    global _engine

    if _engine is None:
        database_url = Config.get_database_url()
        logger.info(f"Creating database engine: {Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}")

        _engine = create_engine(
            database_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False
        )

    return _engine


def get_session_maker() -> sessionmaker:
    """Get or create SQLAlchemy session maker.

    Returns:
        SQLAlchemy sessionmaker
    """
    global _SessionLocal

    if _SessionLocal is None:
        engine = get_db_engine()
        _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    return _SessionLocal


@contextmanager
def get_db_connection() -> Generator[Session, None, None]:
    """Context manager for database sessions.

    Yields:
        SQLAlchemy Session

    Example:
        with get_db_connection() as db:
            db.execute("SELECT * FROM raw_players")
            db.commit()
    """
    SessionLocal = get_session_maker()
    session = SessionLocal()

    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        session.close()


def test_connection() -> bool:
    """Test database connection.

    Returns:
        True if connection successful, False otherwise
    """
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            result = conn.execute("SELECT 1")
            logger.info("Database connection successful")
            return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False
