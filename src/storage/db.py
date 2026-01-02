"""
Database connection and session management using SQLAlchemy 2.0
"""
import os
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool

log = logging.getLogger(__name__)

# Read DATABASE_URL from environment or settings
# Try to get from settings, fallback to environment variable
DATABASE_URL = os.getenv("DATABASE_URL", None)

if not DATABASE_URL:
    try:
        # Try to import from onboarding config (if available)
        from agents.onboarding.config import settings
        DATABASE_URL = getattr(settings, 'DATABASE_URL', None)
    except (ImportError, AttributeError):
        pass

# Final fallback to default
if not DATABASE_URL:
    DATABASE_URL = "postgresql+psycopg://user:pass@localhost:5432/serve_agents"

# Create engine with pool_pre_ping for connection health checks
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    poolclass=NullPool,  # Use NullPool for async compatibility
    echo=False  # Set to True for SQL debugging
)

# Create sessionmaker
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
    bind=engine
)


@contextmanager
def get_db_session():
    """
    Context manager for database sessions.
    
    Usage:
        with get_db_session() as db:
            # use db session
            pass
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        log.error(f"Database session error: {e}", exc_info=True)
        raise
    finally:
        session.close()

