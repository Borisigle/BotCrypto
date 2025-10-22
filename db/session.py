from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .config import DatabaseConfig, get_database_config

_engine: Optional[Engine] = None
_session_factory: Optional[sessionmaker[Session]] = None
_engine_config: Optional[DatabaseConfig] = None


def _engine_needs_rebuild(config: DatabaseConfig) -> bool:
    global _engine, _engine_config
    if _engine is None or _engine_config is None:
        return True
    return _engine_config != config


def get_engine(config: Optional[DatabaseConfig] = None) -> Engine:
    """Return a module-wide SQLAlchemy engine, creating it on demand."""

    global _engine, _engine_config, _session_factory

    cfg = config or get_database_config()
    if _engine_needs_rebuild(cfg):
        _engine = create_engine(
            cfg.url,
            echo=cfg.echo,
            pool_size=cfg.pool_size,
            max_overflow=cfg.max_overflow,
            pool_timeout=cfg.pool_timeout,
            pool_pre_ping=True,
            connect_args=dict(cfg.connect_args),
        )
        _engine_config = cfg
        _session_factory = None
    assert _engine is not None  # for type checkers
    return _engine


def get_session_factory(config: Optional[DatabaseConfig] = None) -> sessionmaker[Session]:
    """Return a session factory bound to the configured engine."""

    global _session_factory

    cfg = config or get_database_config()
    if _session_factory is None or _engine_needs_rebuild(cfg):
        engine = get_engine(cfg)
        _session_factory = sessionmaker(
            bind=engine,
            autoflush=False,
            expire_on_commit=False,
            class_=Session,
        )
    return _session_factory


def get_session(config: Optional[DatabaseConfig] = None) -> Session:
    """Instantiate a new SQLAlchemy session."""

    factory = get_session_factory(config)
    return factory()


@contextmanager
def session_scope(config: Optional[DatabaseConfig] = None) -> Iterator[Session]:
    """Provide a transactional scope around a series of operations."""

    session = get_session(config)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
