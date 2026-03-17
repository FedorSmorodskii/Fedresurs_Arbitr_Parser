from __future__ import annotations

import datetime as dt

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from src.db.models import Base


def get_engine(db_url: str) -> Engine:
    connect_args = {}
    if db_url.startswith("sqlite"):
        connect_args = {"check_same_thread": False, "timeout": 30}
        engine = create_engine(
            db_url,
            future=True,
            pool_pre_ping=True,
            connect_args=connect_args,
            poolclass=NullPool,
        )

        @event.listens_for(engine, "connect")
        def _sqlite_pragmas(dbapi_connection, connection_record):
            cur = dbapi_connection.cursor()
            try:
                cur.execute("PRAGMA journal_mode=WAL;")
                cur.execute("PRAGMA synchronous=NORMAL;")
                cur.execute("PRAGMA busy_timeout=30000;")
            finally:
                cur.close()

        return engine

    return create_engine(db_url, future=True, pool_pre_ping=True, connect_args=connect_args)


def ensure_schema(engine: Engine) -> None:
    if engine.url.drivername.startswith("sqlite"):
        _sqlite_migrate_history_schema(engine)
    Base.metadata.create_all(engine)


def make_session_factory(engine: Engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def _sqlite_table_columns(engine: Engine, table_name: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table_name});")).mappings().all()
    return {str(r["name"]) for r in rows if "name" in r}


def _sqlite_has_table(engine: Engine, table_name: str) -> bool:
    with engine.connect() as conn:
        v = conn.execute(
            text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:name LIMIT 1;"),
            {"name": table_name},
        ).scalar()
    return v is not None


def _sqlite_migrate_history_schema(engine: Engine) -> None:
    if _sqlite_has_table(engine, "fedresurs_bankruptcy"):
        cols = _sqlite_table_columns(engine, "fedresurs_bankruptcy")
        if "parsed_on" not in cols:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE fedresurs_bankruptcy RENAME TO fedresurs_bankruptcy__old;"))
            Base.metadata.tables["fedresurs_bankruptcy"].create(bind=engine)
            with engine.begin() as conn:
                today = dt.datetime.now(dt.timezone.utc).date().isoformat()
                conn.execute(
                    text(
                        """
                        INSERT INTO fedresurs_bankruptcy
                          (inn, person_guid, case_number, last_publish_date, parsed_on, parsed_at, status, error)
                        SELECT
                          inn,
                          person_guid,
                          case_number,
                          last_publish_date,
                          COALESCE(date(parsed_at), :today),
                          COALESCE(parsed_at, CURRENT_TIMESTAMP),
                          COALESCE(status, 'ok'),
                          error
                        FROM fedresurs_bankruptcy__old;
                        """
                    ),
                    {"today": today},
                )
                conn.execute(text("DROP TABLE fedresurs_bankruptcy__old;"))

    if _sqlite_has_table(engine, "arbitr_bankruptcy"):
        cols = _sqlite_table_columns(engine, "arbitr_bankruptcy")
        if "parsed_on" not in cols:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE arbitr_bankruptcy RENAME TO arbitr_bankruptcy__old;"))
            Base.metadata.tables["arbitr_bankruptcy"].create(bind=engine)
            with engine.begin() as conn:
                today = dt.datetime.now(dt.timezone.utc).date().isoformat()
                conn.execute(
                    text(
                        """
                        INSERT INTO arbitr_bankruptcy
                          (case_number, last_date, document_name, parsed_on, parsed_at, status, error)
                        SELECT
                          case_number,
                          last_date,
                          document_name,
                          COALESCE(date(parsed_at), :today),
                          COALESCE(parsed_at, CURRENT_TIMESTAMP),
                          COALESCE(status, 'ok'),
                          error
                        FROM arbitr_bankruptcy__old;
                        """
                    ),
                    {"today": today},
                )
                conn.execute(text("DROP TABLE arbitr_bankruptcy__old;"))

