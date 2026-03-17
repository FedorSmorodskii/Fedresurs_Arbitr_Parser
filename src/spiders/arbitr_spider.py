from __future__ import annotations

import re
import time
from typing import Any

import scrapy
from parsel import Selector


class ArbitrBankruptcySpider(scrapy.Spider):
    name = "arbitr_bankruptcy"
    allowed_domains = ["kad.arbitr.ru"]
    _DATE_RE = re.compile(r"\b\d{2}\.\d{2}\.\d{4}\b")

    def __init__(self, case_numbers: list[str], *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.case_numbers = case_numbers
        self.proxy = kwargs.get("proxy")

    def start_requests(self):
        url = "https://kad.arbitr.ru/Kad/SearchInstances"
        for case_number in self.case_numbers:
            body = {
                "Page": 1,
                "Count": 25,
                "Courts": [],
                "DateFrom": None,
                "DateTo": None,
                "Sides": [],
                "Judges": [],
                "CaseNumbers": [case_number],
                "WithVKSInstances": False,
            }
            headers = {
                "Cookie": (
                    "__ddg9_=45.249.106.121; "
                    "__ddg1_=Ewjwv5u7wGgGtjCmPwPW; "
                    "ASP.NET_SessionId=rsqmsycbgcvhzjsnlo1heldd; "
                    "CUID=5eeed59b-2075-4e13-8a80-28e3eed97a13:nNwEjfXEs1A8kXc5HO5ViA==; "
                    "_ga=GA1.2.809341800.1773722509; "
                    "_gid=GA1.2.1641331757.1773722509; "
                    "pr_fp=026d909d5920c5552d658cadc621325eac14a48b06a555eaffc9732d902722d6; "
                    "_ym_uid=1773722513137858614; "
                    "_ym_d=1773722513; "
                    "_ym_isad=2; "
                    "tmr_lvid=43d0c8e26fe36276c3032759e29f6a65; "
                    "tmr_lvidTS=1773722514226; "
                    "domain_sid=gps07F1WlfMv7ODEWvFFp%3A1773722518331; "
                    "wasm=53bc69d560d077b15c1f5a7e165f39e8; "
                    "rcid=e3b28477-0095-48fc-8dbd-84d0665dc181; "
                    "KadLVCards=%d0%9032-28873%2f2024; "
                    "_ga_Q2V7P901XE=GS2.2.s1773722510$o1$g1$t1773722538$j32$l0$h0; "
                    "tmr_detect=0%7C1773722545002; "
                    "_ga_9582CL89Y6=GS2.2.s1773722509$o1$g1$t1773722546$j23$l0$h0; "
                    "__ddg10_=1773722547; "
                    "__ddg8_=PN7CzzSIvgdfPLhP"
                ),
                "Accept": "*/*",
                "Content-Type": "application/json",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/142.0.0.0 Safari/537.36"
                ),
                "Origin": "https://kad.arbitr.ru",
                "Referer": "https://kad.arbitr.ru/",
                "X-Requested-With": "XMLHttpRequest",
                "X-Date-Format": "iso",
            }
            yield scrapy.Request(
                url=url,
                method="POST",
                callback=self.parse_search,
                errback=self.errback,
                body=self._json_dumps(body),
                headers=headers,
                meta={
                    "case_number": case_number,
                    "referer": "https://kad.arbitr.ru/",
                },
                dont_filter=True,
            )

    def parse_search(self, response: scrapy.http.Response):
        case_number = response.meta["case_number"]
        if response.status != 200:
            yield {
                "case_number": case_number,
                "last_date": None,
                "document_name": None,
                "status": "error",
                "error": f"http_search_{response.status}",
            }
            return

        selector = Selector(text=response.text)
        href = selector.xpath('//a[@class="num_case"]/@href').get()
        if not href:
            yield {
                "case_number": case_number,
                "last_date": None,
                "document_name": None,
                "status": "not_found",
                "error": "no_case_link",
            }
            return

        if href.startswith("http"):
            card_url = href
        else:
            card_url = response.urljoin(href)

        headers = {
            "Cookie": (
                "__ddg1_=Ewjwv5u7wGgGtjCmPwPW; "
                "ASP.NET_SessionId=rsqmsycbgcvhzjsnlo1heldd; "
                "CUID=5eeed59b-2075-4e13-8a80-28e3eed97a13:nNwEjfXEs1A8kXc5HO5ViA==; "
                "_ga=GA1.2.809341800.1773722509; "
                "_gid=GA1.2.1641331757.1773722509; "
                "pr_fp=026d909d5920c5552d658cadc621325eac14a48b06a555eaffc9732d902722d6; "
                "_ym_uid=1773722513137858614; "
                "_ym_d=1773722513; "
                "_ym_isad=2; "
                "tmr_lvid=43d0c8e26fe36276c3032759e29f6a65; "
                "tmr_lvidTS=1773722514226; "
                "domain_sid=gps07F1WlfMv7ODEWvFFp%3A1773722518331; "
                "KadLVCards=%d0%9032-28873%2f2024; "
                "_ga_Q2V7P901XE=GS2.2.s1773722510$o1$g1$t1773722538$j32$l0$h0; "
                "tmr_detect=0%7C1773722545002; "
                "_ga_9582CL89Y6=GS2.2.s1773722509$o1$g1$t1773722797$j60$l0$h0; "
                "__ddg9_=45.249.106.121; "
                "wasm=53bc69d560d077b15c1f5a7e165f39e8; "
                "__ddg8_=hmLdPvWxlAE69T9k; "
                "__ddg10_=1773724244; "
                "rcid=c6c63f31-f30e-4928-a7f1-750399d54395"
            ),
            "Cache-Control": "max-age=0",
            "Accept-Language": "ru-RU,ru;q=0.9",
            "Origin": "https://kad.arbitr.ru",
            "Content-Type": "application/x-www-form-urlencoded",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/142.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8,"
                "application/signed-exchange;v=b3;q=0.7"
            ),
            "Referer": card_url,
        }

        yield scrapy.Request(
            url=card_url,
            method="POST",
            callback=self.parse_card,
            errback=self.errback,
            body="RecaptchaToken=undefined",
            headers=headers,
            meta={
                "case_number": case_number,
                "referer": response.url,
            },
            dont_filter=True,
        )

    def parse_card(self, response: scrapy.http.Response):
        case_number = response.meta["case_number"]
        if response.status != 200:
            body_preview = response.text[:500].replace("\n", " ").replace("\r", " ").strip()
            yield {
                "case_number": case_number,
                "last_date": None,
                "document_name": None,
                "status": "error",
                "error": f"http_card_{response.status}: {body_preview}" if body_preview else f"http_card_{response.status}",
            }
            return

        selector = Selector(text=response.text)
        last_date = selector.xpath('normalize-space((//span[@class="b-reg-date"])[1])').get()
        document_name = selector.xpath(
            'normalize-space((//h2[contains(@class,"b-case-result")]/a)[1])'
        ).get()

        if not last_date or not document_name:
            instance_id = selector.xpath('normalize-space((//input[@class="js-instanceId"])[1]/@value)').get()
            case_id = self._extract_case_id_from_url(response.url)
            if instance_id and case_id:
                docs_url = self._build_instance_documents_url(instance_id=instance_id, case_id=case_id)
                yield scrapy.Request(
                    url=docs_url,
                    method="GET",
                    callback=self.parse_instance_documents,
                    errback=self.errback,
                    headers=self._instance_documents_headers(referer=response.url),
                    meta={
                        "case_number": case_number,
                        "referer": response.url,
                    },
                    dont_filter=True,
                )
                return

        status = "ok"
        error: str | None = None
        if not last_date or not document_name:
            status = "no_data"
            if not last_date and not document_name:
                error = "no_date_and_document"
            elif not last_date:
                error = "no_date"
            else:
                error = "no_document"

        yield {
            "case_number": case_number,
            "last_date": last_date or None,
            "document_name": document_name or None,
            "status": status,
            "error": error,
        }

    def parse_instance_documents(self, response: scrapy.http.Response):
        case_number = response.meta["case_number"]
        if response.status != 200:
            body_preview = response.text[:500].replace("\n", " ").replace("\r", " ").strip()
            yield {
                "case_number": case_number,
                "last_date": None,
                "document_name": None,
                "status": "error",
                "error": f"http_instance_docs_{response.status}: {body_preview}" if body_preview else f"http_instance_docs_{response.status}",
            }
            return

        try:
            data = response.json()
        except Exception as e:
            body_preview = response.text[:500].replace("\n", " ").replace("\r", " ").strip()
            yield {
                "case_number": case_number,
                "last_date": None,
                "document_name": None,
                "status": "error",
                "error": f"instance_docs_json_error: {e!r}: {body_preview}",
            }
            return

        first_doc = self._find_first_document_item(data)
        if not isinstance(first_doc, dict):
            yield {
                "case_number": case_number,
                "last_date": None,
                "document_name": None,
                "status": "no_data",
                "error": "instance_docs_empty",
            }
            return

        last_date = self._extract_date_from_item(first_doc)
        document_name = self._extract_document_name_from_item(first_doc)

        status = "ok"
        error: str | None = None
        if not last_date or not document_name:
            status = "no_data"
            if not last_date and not document_name:
                error = "no_date_and_document"
            elif not last_date:
                error = "no_date"
            else:
                error = "no_document"

        yield {
            "case_number": case_number,
            "last_date": last_date,
            "document_name": document_name,
            "status": status,
            "error": error,
        }

    def errback(self, failure):
        request = failure.request
        case_number = request.meta.get("case_number")
        yield {
            "case_number": case_number,
            "last_date": None,
            "document_name": None,
            "status": "error",
            "error": repr(failure.value),
        }

    @staticmethod
    def _json_dumps(payload: dict[str, Any]) -> str:
        import json

        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def _extract_case_id_from_url(url: str) -> str | None:
        # https://kad.arbitr.ru/Card/<caseId>
        marker = "/Card/"
        if marker not in url:
            return None
        case_id = url.split(marker, 1)[1].split("?", 1)[0].strip().strip("/")
        return case_id or None

    @staticmethod
    def _build_instance_documents_url(*, instance_id: str, case_id: str) -> str:
        ts = int(time.time() * 1000)
        return (
            "https://kad.arbitr.ru/Kad/InstanceDocumentsPage"
            f"?_={ts}&id={instance_id}&caseId={case_id}&perPage=30&page=1"
        )

    @staticmethod
    def _instance_documents_headers(*, referer: str) -> dict[str, str]:
        return {
            "Cookie": (
                "__ddg1_=Ewjwv5u7wGgGtjCmPwPW; "
                "ASP.NET_SessionId=rsqmsycbgcvhzjsnlo1heldd; "
                "CUID=5eeed59b-2075-4e13-8a80-28e3eed97a13:nNwEjfXEs1A8kXc5HO5ViA==; "
                "_ga=GA1.2.809341800.1773722509; "
                "_gid=GA1.2.1641331757.1773722509; "
                "pr_fp=026d909d5920c5552d658cadc621325eac14a48b06a555eaffc9732d902722d6; "
                "_ym_uid=1773722513137858614; "
                "_ym_d=1773722513; "
                "_ym_isad=2; "
                "tmr_lvid=43d0c8e26fe36276c3032759e29f6a65; "
                "tmr_lvidTS=1773722514226; "
                "domain_sid=gps07F1WlfMv7ODEWvFFp%3A1773722518331; "
                "__ddg9_=45.249.106.121; "
                "wasm=53bc69d560d077b15c1f5a7e165f39e8; "
                "rcid=c5c3bb38-1b38-4f01-82a3-5bd898971e0d; "
                "KadLVCards=%d0%9073-1829%2f2026~%d0%9032-28873%2f2024; "
                "__ddg10_=1773727006; "
                "__ddg8_=Dr01stiRRZCitF3R; "
                "tmr_detect=0%7C1773727012745"
            ),
            "Accept": "application/json, text/javascript, */*",
            "Accept-Language": "ru-RU,ru;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/142.0.0.0 Safari/537.36"
            ),
            "X-Requested-With": "XMLHttpRequest",
            "Referer": referer,
        }

    @classmethod
    def _find_first_item(cls, data: Any) -> dict[str, Any] | None:
        if isinstance(data, list):
            first = data[0] if data else None
            return first if isinstance(first, dict) else None
        if not isinstance(data, dict):
            return None

        for key in ("Items", "items", "Result", "result", "Data", "data"):
            if key not in data:
                continue
            v = data.get(key)
            if isinstance(v, list):
                first = v[0] if v else None
                return first if isinstance(first, dict) else None
            if isinstance(v, dict):
                nested = cls._find_first_item(v)
                if nested:
                    return nested

        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v[0]

        return None

    @classmethod
    def _find_first_document_item(cls, data: Any) -> dict[str, Any] | None:
        items = None
        if isinstance(data, dict):
            result = data.get("Result") if isinstance(data.get("Result"), dict) else None
            if result and isinstance(result.get("Items"), list):
                items = result.get("Items")
            elif isinstance(data.get("Items"), list):
                items = data.get("Items")
        elif isinstance(data, list):
            items = data

        if not isinstance(items, list) or not items:
            return None

        for it in items:
            if not isinstance(it, dict):
                continue
            if it.get("IsAct") is True:
                return it
            file_name = (it.get("FileName") or "").strip()
            original = (it.get("OriginalActFileName") or "").strip()
            if file_name or original:
                return it

        first = items[0]
        return first if isinstance(first, dict) else None

    @classmethod
    def _extract_date_from_item(cls, item: dict[str, Any]) -> str | None:
        display = item.get("DisplayDate") or item.get("displayDate")
        if display:
            s = str(display)
            m = cls._DATE_RE.search(s)
            if m:
                return m.group(0)

        for key in (
            "Date",
            "date",
            "DocumentDate",
            "documentDate",
            "RegDate",
            "regDate",
            "DisplayDate",
            "displayDate",
        ):
            v = item.get(key)
            if not v:
                continue
            s = str(v)
            m = cls._DATE_RE.search(s)
            if m:
                return m.group(0)
        for v in item.values():
            if not v:
                continue
            m = cls._DATE_RE.search(str(v))
            if m:
                return m.group(0)
        return None

    @staticmethod
    def _extract_document_name_from_item(item: dict[str, Any]) -> str | None:
        content_types = item.get("ContentTypes") or item.get("contentTypes")
        if isinstance(content_types, list):
            parts = [str(x).strip() for x in content_types if str(x).strip()]
            if parts:
                return " ".join(parts).strip()

        for key in (
            "DocumentName",
            "documentName",
            "Name",
            "name",
            "Title",
            "title",
            "Text",
            "text",
            "Description",
            "description",
        ):
            v = item.get(key)
            if v:
                s = str(v).strip()
                if s:
                    return s
        return None

