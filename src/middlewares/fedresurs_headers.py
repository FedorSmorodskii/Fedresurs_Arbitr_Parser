from __future__ import annotations

from dataclasses import dataclass

from scrapy.http import Request


@dataclass(frozen=True)
class FedresursHeadersMiddleware:
    def process_request(self, request: Request, spider):
        referer = request.meta.get("referer")
        if referer and "Referer" not in request.headers:
            request.headers["Referer"] = referer

        proxy = request.meta.get("proxy")
        if not proxy:
            proxy = getattr(spider, "proxy", None)
        if proxy:
            request.meta["proxy"] = proxy

