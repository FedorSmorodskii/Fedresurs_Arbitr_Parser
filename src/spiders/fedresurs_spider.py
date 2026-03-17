from __future__ import annotations

import datetime as dt
from typing import Any

import scrapy
from dateutil import parser as date_parser


class FedresursBankruptcySpider(scrapy.Spider):
    name = "fedresurs_bankruptcy"
    allowed_domains = ["fedresurs.ru"]

    def __init__(self, inns: list[str], *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.inns = inns
        self.proxy = kwargs.get("proxy")

    def start_requests(self):
        for inn in self.inns:
            url = (
                "https://fedresurs.ru/backend/persons"
                f"?searchString={inn}&limit=15&offset=0&isActive=true"
            )
            yield scrapy.Request(
                url=url,
                method="GET",
                callback=self.parse_persons,
                errback=self.errback,
                meta={
                    "inn": inn,
                    "referer": f"https://fedresurs.ru/entities?searchString={inn}",
                },
                dont_filter=True,
            )

    def parse_persons(self, response: scrapy.http.Response):
        inn = response.meta["inn"]
        if response.status != 200:
            yield from self._schedule_companies_search(inn)
            return
        data = response.json()
        found = int(data.get("found") or 0)
        page_data = data.get("pageData") or []
        if found <= 0 or not page_data:
            yield from self._schedule_companies_search(inn)
            return

        guid = page_data[0].get("guid")
        if not guid:
            yield from self._schedule_companies_search(inn)
            return

        url = f"https://fedresurs.ru/backend/persons/{guid}/bankruptcy"
        yield scrapy.Request(
            url=url,
            method="GET",
            callback=self.parse_bankruptcy,
            errback=self.errback,
            meta={
                "inn": inn,
                "person_guid": guid,
                "referer": f"https://fedresurs.ru/persons/{guid}",
            },
            dont_filter=True,
        )

    def _schedule_companies_search(self, inn: str):
        url = (
            "https://fedresurs.ru/backend/companies"
            f"?searchString={inn}&limit=15&offset=0&isActive=true"
        )
        yield scrapy.Request(
            url=url,
            method="GET",
            callback=self.parse_companies,
            errback=self.errback,
            meta={
                "inn": inn,
                "referer": f"https://fedresurs.ru/entities?searchString={inn}",
            },
            dont_filter=True,
        )

    def parse_companies(self, response: scrapy.http.Response):
        inn = response.meta["inn"]
        if response.status != 200:
            yield {
                "inn": inn,
                "person_guid": None,
                "case_number": None,
                "last_publish_date": None,
                "status": "error",
                "error": f"http_company_{response.status}",
            }
            return

        data = response.json()
        found = int(data.get("found") or 0)
        page_data = data.get("pageData") or []
        if found <= 0 or not page_data:
            # ни физлицо, ни компания по этому ИНН не найдены
            yield {
                "inn": inn,
                "person_guid": None,
                "case_number": None,
                "last_publish_date": None,
                "status": "not_found",
                "error": None,
            }
            return

        guid = page_data[0].get("guid")
        if not guid:
            yield {
                "inn": inn,
                "person_guid": None,
                "case_number": None,
                "last_publish_date": None,
                "status": "not_found",
                "error": "missing_company_guid",
            }
            return

        url = f"https://fedresurs.ru/backend/companies/{guid}/bankruptcy"
        yield scrapy.Request(
            url=url,
            method="GET",
            callback=self.parse_bankruptcy,
            errback=self.errback,
            meta={
                "inn": inn,
                "person_guid": guid,
                "referer": f"https://fedresurs.ru/companies/{guid}",
            },
            dont_filter=True,
        )

    def parse_bankruptcy(self, response: scrapy.http.Response):
        inn = response.meta["inn"]
        guid = response.meta["person_guid"]
        if response.status != 200:
            yield {
                "inn": inn,
                "person_guid": guid,
                "case_number": None,
                "last_publish_date": None,
                "status": "error",
                "error": f"http_{response.status}",
            }
            return
        data = response.json()

        legal_cases = data.get("legalCases") or []
        if not legal_cases:
            yield {
                "inn": inn,
                "person_guid": guid,
                "case_number": None,
                "last_publish_date": None,
                "status": "no_legal_cases",
                "error": None,
            }
            return

        best_case_number: str | None = None
        best_dt: dt.datetime | None = None

        for case in legal_cases:
            case_number = case.get("number")
            pubs = case.get("lastPublications") or []
            for pub in pubs:
                raw = pub.get("datePublish")
                if not raw:
                    continue
                try:
                    pub_dt = date_parser.isoparse(raw)
                except Exception:
                    continue
                if best_dt is None or pub_dt > best_dt:
                    best_dt = pub_dt
                    best_case_number = case_number

        yield {
            "inn": inn,
            "person_guid": guid,
            "case_number": best_case_number,
            "last_publish_date": best_dt,
            "status": "ok" if best_dt or best_case_number else "no_publications",
            "error": None,
        }

    def errback(self, failure):
        request = failure.request
        inn = request.meta.get("inn")
        guid = request.meta.get("person_guid")
        yield {
            "inn": inn,
            "person_guid": guid,
            "case_number": None,
            "last_publish_date": None,
            "status": "error",
            "error": repr(failure.value),
        }

