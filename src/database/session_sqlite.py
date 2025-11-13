from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    AsyncEngine,
)
from sqlalchemy.orm import sessionmaker

from config import get_settings
from database import Base

# Load application settings
settings = get_settings()

# SQLite database URL
SQLITE_DATABASE_URL = f"sqlite+aiosqlite:///{settings.PATH_TO_DB}"

# Create async engine with type annotation
sqlite_engine: AsyncEngine = create_async_engine(
    SQLITE_DATABASE_URL,
    echo=settings.DB_ECHO  # echo controlled via settings for flexibility
)

# Create async session factory
AsyncSQLiteSessionLocal = sessionmaker(  # type: ignore
    bind=sqlite_engine,
    class_=AsyncSession,
    expire_on_commit=False
)


async def get_sqlite_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Provide an asynchronous database session.

    This function is intended for use as a FastAPI dependency.
    Ensures the session is properly closed after use.

    :return: Async generator yielding an AsyncSession instance
    """
    async with AsyncSQLiteSessionLocal() as session:
        yield session


@asynccontextmanager
async def get_sqlite_db_contextmanager() -> AsyncGenerator[AsyncSession, None]:
    """
    Provide an asynchronous database session using a context manager.

    This function is useful in scripts or testing scenarios
    where `async with` syntax is preferred.

    Note: Both `get_sqlite_db` and `get_sqlite_db_contextmanager` return
    the same session; this duplication is intentional for flexibility.

    :return: Async generator yielding an AsyncSession instance
    """
    async with AsyncSQLiteSessionLocal() as session:
        yield session


async def reset_sqlite_database() -> None:
    """
    Reset the SQLite database by dropping and recreating all tables.

    Warning: This action deletes all data. Useful for tests or development resets.

    :return: None
    """
    async with sqlite_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
