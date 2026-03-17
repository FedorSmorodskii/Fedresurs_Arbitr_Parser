from __future__ import annotations

import argparse
import logging
from pathlib import Path

from scrapy.crawler import CrawlerProcess

from src.db.session import ensure_schema, get_engine
from src.input.xlsx import load_case_numbers_from_xlsx
from src.pipelines.sqlalchemy_pipeline import already_processed_case_numbers
from src.settings import scrapy_settings
from src.spiders.arbitr_spider import ArbitrBankruptcySpider


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Arbitr bankruptcy parser (local)")
    p.add_argument("--input", required=False, help="Path to .xlsx with case numbers")
    p.add_argument(
        "--case",
        action="append",
        default=None,
        help="Case number (repeatable). Example: --case А32-28873/2024",
    )
    p.add_argument(
        "--db",
        default="sqlite:///data/app.sqlite",
        help="SQLAlchemy DB URL (default: sqlite:///data/app.sqlite)",
    )
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--download-delay", type=float, default=0.5)
    p.add_argument("--retry-times", type=int, default=6)
    p.add_argument("--download-timeout", type=int, default=30)
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--log-file", default=None)
    p.add_argument(
        "--resume",
        action="store_true",
        help="Skip case numbers already processed (present in DB with final status for today)",
    )
    return p.parse_args()


def _collect_cases_from_args(args: argparse.Namespace) -> list[str]:
    cases: list[str] = []
    seen: set[str] = set()

    if args.case:
        for raw in args.case:
            parts = str(raw).replace(";", ",").split(",")
            for part in parts:
                value = part.strip()
                if value and value not in seen:
                    seen.add(value)
                    cases.append(value)

    if args.input:
        input_path = Path(args.input)
        for value in load_case_numbers_from_xlsx(input_path):
            if value and value not in seen:
                seen.add(value)
                cases.append(value)

    return cases


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))

    case_numbers = _collect_cases_from_args(args)
    if not case_numbers:
        raise SystemExit("Provide --case and/or --input with at least one case number")

    engine = get_engine(args.db)
    ensure_schema(engine)

    if args.resume:
        processed = already_processed_case_numbers(engine)
        case_numbers = [c for c in case_numbers if c not in processed]

    if not case_numbers:
        logging.info("No case numbers to process (empty input or fully resumed).")
        return

    settings = scrapy_settings(
        concurrency=args.concurrency,
        download_delay=args.download_delay,
        retry_times=args.retry_times,
        download_timeout=args.download_timeout,
        log_level=args.log_level,
        log_file=args.log_file,
    )
    settings["DB_URL"] = args.db
    settings["COOKIES_ENABLED"] = False
    settings["ITEM_PIPELINES"] = {
        "src.pipelines.sqlalchemy_pipeline.ArbitrSQLAlchemyPipeline": 300,
    }

    process = CrawlerProcess(settings=settings)
    process.crawl(ArbitrBankruptcySpider, case_numbers=case_numbers)
    process.start()


if __name__ == "__main__":
    main()

