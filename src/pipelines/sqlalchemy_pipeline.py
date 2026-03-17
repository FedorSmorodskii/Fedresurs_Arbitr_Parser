from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.db.models import ArbitrBankruptcy, FedresursBankruptcy
from src.db.session import get_engine, make_session_factory


FINAL_STATUSES = {"ok", "no_legal_cases", "not_found", "no_publications"}


def _dialect_insert(engine: Engine):
    """
    Return a dialect-specific INSERT that supports `on_conflict_do_update`.

    - SQLite: sqlalchemy.dialects.sqlite.insert
    - PostgreSQL: sqlalchemy.dialects.postgresql.insert
    """
    name = engine.dialect.name.lower()
    if name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as dialect_insert

        return dialect_insert
    if name in {"postgresql", "postgres"}:
        from sqlalchemy.dialects.postgresql import insert as dialect_insert

        return dialect_insert
    raise RuntimeError(f"Unsupported DB dialect for upsert: {engine.dialect.name}")


def already_processed_inns(engine: Engine) -> set[str]:
    today = dt.datetime.now(dt.timezone.utc).date()
    with Session(engine) as session:
        q = (
            select(FedresursBankruptcy.inn)
            .where(FedresursBankruptcy.status.in_(FINAL_STATUSES))
            .where(FedresursBankruptcy.parsed_on == today)
        )
        return set(session.execute(q).scalars().all())


def already_processed_case_numbers(engine: Engine) -> set[str]:
    today = dt.datetime.now(dt.timezone.utc).date()
    with Session(engine) as session:
        q = (
            select(ArbitrBankruptcy.case_number)
            .where(ArbitrBankruptcy.status.in_(FINAL_STATUSES))
            .where(ArbitrBankruptcy.parsed_on == today)
        )
        return set(session.execute(q).scalars().all())


class SQLAlchemyPipeline:
    def __init__(self, db_url: str) -> None:
        self.db_url = db_url
        self.engine: Engine | None = None
        self.session_factory = None

    @classmethod
    def from_crawler(cls, crawler):
        db_url = crawler.settings.get("DB_URL")
        if not db_url:
            raise ValueError("DB_URL must be set in Scrapy settings")
        return cls(db_url=str(db_url))

    def open_spider(self, spider):
        self.engine = get_engine(self.db_url)
        self.session_factory = make_session_factory(self.engine)

    def close_spider(self, spider):
        self.engine = None
        self.session_factory = None

    def process_item(self, item: dict[str, Any], spider):
        if not self.engine or not self.session_factory:
            raise RuntimeError("Pipeline not initialized")

        now = dt.datetime.now(dt.timezone.utc)
        parsed_on = now.date()
        insert = _dialect_insert(self.engine)

        last_pub = item.get("last_publish_date")
        last_publish_str: str | None
        if isinstance(last_pub, dt.datetime):
            last_publish_str = last_pub.date().strftime("%d.%m.%Y")
        elif isinstance(last_pub, dt.date):
            last_publish_str = last_pub.strftime("%d.%m.%Y")
        elif last_pub:
            try:
                parsed = dt.datetime.fromisoformat(str(last_pub))
                last_publish_str = parsed.date().strftime("%d.%m.%Y")
            except Exception:
                last_publish_str = str(last_pub)
        else:
            last_publish_str = None

        payload = {
            "inn": item.get("inn"),
            "person_guid": item.get("person_guid"),
            "case_number": item.get("case_number"),
            "last_publish_date": last_publish_str,
            "parsed_on": parsed_on,
            "parsed_at": now,
            "status": item.get("status") or "ok",
            "error": item.get("error"),
        }

        stmt = insert(FedresursBankruptcy).values(**payload)
        stmt = stmt.on_conflict_do_update(
            index_elements=["inn", "parsed_on"],
            set_={
                "person_guid": stmt.excluded.person_guid,
                "case_number": stmt.excluded.case_number,
                "last_publish_date": stmt.excluded.last_publish_date,
                "parsed_on": stmt.excluded.parsed_on,
                "parsed_at": stmt.excluded.parsed_at,
                "status": stmt.excluded.status,
                "error": stmt.excluded.error,
            },
        )

        with self.session_factory() as session:
            session.execute(stmt)
            session.commit()

        return item


class ArbitrSQLAlchemyPipeline:
    def __init__(self, db_url: str) -> None:
        self.db_url = db_url
        self.engine: Engine | None = None
        self.session_factory = None

    @classmethod
    def from_crawler(cls, crawler):
        db_url = crawler.settings.get("DB_URL")
        if not db_url:
            raise ValueError("DB_URL must be set in Scrapy settings")
        return cls(db_url=str(db_url))

    def open_spider(self, spider):
        self.engine = get_engine(self.db_url)
        self.session_factory = make_session_factory(self.engine)

    def close_spider(self, spider):
        self.engine = None
        self.session_factory = None

    def process_item(self, item: dict[str, Any], spider):
        if not self.engine or not self.session_factory:
            raise RuntimeError("Pipeline not initialized")

        now = dt.datetime.now(dt.timezone.utc)
        parsed_on = now.date()
        insert = _dialect_insert(self.engine)

        payload = {
            "case_number": item.get("case_number"),
            "last_date": item.get("last_date"),
            "document_name": item.get("document_name"),
            "parsed_on": parsed_on,
            "parsed_at": now,
            "status": item.get("status") or "ok",
            "error": item.get("error"),
        }

        stmt = insert(ArbitrBankruptcy).values(**payload)
        stmt = stmt.on_conflict_do_update(
            index_elements=["case_number", "parsed_on"],
            set_={
                "last_date": stmt.excluded.last_date,
                "document_name": stmt.excluded.document_name,
                "parsed_on": stmt.excluded.parsed_on,
                "parsed_at": stmt.excluded.parsed_at,
                "status": stmt.excluded.status,
                "error": stmt.excluded.error,
            },
        )

        with self.session_factory() as session:
            session.execute(stmt)
            session.commit()

        return item


