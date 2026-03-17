from __future__ import annotations

import datetime as dt

from sqlalchemy import Date, DateTime, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class FedresursBankruptcy(Base):
    __tablename__ = "fedresurs_bankruptcy"
    __table_args__ = (
        UniqueConstraint("inn", "parsed_on", name="uq_fedresurs_inn_parsed_on"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    inn: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    person_guid: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)

    case_number: Mapped[str | None] = mapped_column(String(64), nullable=True)

    last_publish_date: Mapped[str | None] = mapped_column(String(10), nullable=True)

    parsed_on: Mapped[dt.date] = mapped_column(
        Date,
        nullable=False,
        default=lambda: dt.datetime.now(dt.timezone.utc).date(),
        index=True,
    )
    parsed_at: Mapped[dt.datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=lambda: dt.datetime.now(dt.timezone.utc),
    )

    status: Mapped[str] = mapped_column(String(32), nullable=False, default="ok", index=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class ArbitrBankruptcy(Base):
    __tablename__ = "arbitr_bankruptcy"
    __table_args__ = (
        UniqueConstraint("case_number", "parsed_on", name="uq_arbitr_case_number_parsed_on"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    case_number: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    last_date: Mapped[str | None] = mapped_column(String(10), nullable=True)
    document_name: Mapped[str | None] = mapped_column(Text, nullable=True)

    parsed_on: Mapped[dt.date] = mapped_column(
        Date,
        nullable=False,
        default=lambda: dt.datetime.now(dt.timezone.utc).date(),
        index=True,
    )
    parsed_at: Mapped[dt.datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=lambda: dt.datetime.now(dt.timezone.utc),
    )

    status: Mapped[str] = mapped_column(String(32), nullable=False, default="ok", index=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

