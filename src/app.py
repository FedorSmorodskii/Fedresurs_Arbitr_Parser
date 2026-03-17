from __future__ import annotations

import argparse
import logging
from pathlib import Path

from scrapy.crawler import CrawlerProcess

from src.db.session import ensure_schema, get_engine
from src.input.xlsx import load_inns_from_xlsx, normalize_inn
from src.pipelines.sqlalchemy_pipeline import already_processed_inns
from src.settings import scrapy_settings
from src.spiders.fedresurs_spider import FedresursBankruptcySpider


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fedresurs bankruptcy parser (local)")
    p.add_argument("--input", required=False, help="Path to .xlsx with INNs")
    p.add_argument(
        "--inn",
        action="append",
        default=None,
        help="INN value (repeatable). Example: --inn 231138771115",
    )
    p.add_argument("--column", default=None, help="Excel column name with INN")
    p.add_argument(
        "--db",
        default="sqlite:///data/app.sqlite",
        help="SQLAlchemy DB URL (default: sqlite:///data/app.sqlite)",
    )
    p.add_argument("--concurrency", type=int, default=18)
    p.add_argument("--download-delay", type=float, default=0.1)
    p.add_argument("--retry-times", type=int, default=6)
    p.add_argument("--download-timeout", type=int, default=30)
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--log-file", default=None)
    p.add_argument(
        "--resume",
        action="store_true",
        help="Skip INNs already processed (present in DB with final status)",
    )
    return p.parse_args()


def _collect_inns_from_args(args: argparse.Namespace) -> list[str]:
    inns: list[str] = []
    seen: set[str] = set()

    if args.inn:
        for raw in args.inn:
            parts = str(raw).replace(";", ",").replace(" ", ",").split(",")
            for part in parts:
                inn = normalize_inn(part)
                if inn and inn not in seen:
                    seen.add(inn)
                    inns.append(inn)

    if args.input:
        input_path = Path(args.input)
        for inn in load_inns_from_xlsx(input_path, column=args.column):
            if inn not in seen:
                seen.add(inn)
                inns.append(inn)

    return inns


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))

    inns = _collect_inns_from_args(args)
    if not inns:
        raise SystemExit("Provide --inn and/or --input with at least one INN")

    engine = get_engine(args.db)
    ensure_schema(engine)

    if args.resume:
        processed = already_processed_inns(engine)
        inns = [inn for inn in inns if inn not in processed]

    if not inns:
        logging.info("No INNs to process (empty input or fully resumed).")
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

    process = CrawlerProcess(settings=settings)
    process.crawl(FedresursBankruptcySpider, inns=inns)
    process.start()


if __name__ == "__main__":
    main()

