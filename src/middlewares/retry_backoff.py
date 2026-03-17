from __future__ import annotations

import random
from typing import Any

from scrapy.downloadermiddlewares.retry import RetryMiddleware, get_retry_request
from scrapy.exceptions import NotConfigured
from twisted.internet import reactor
from twisted.internet.task import deferLater


class RetryBackoffDelayMiddleware:
    def process_request(self, request, spider):
        delay = request.meta.get("backoff_delay")
        try:
            delay_s = float(delay) if delay is not None else 0.0
        except (TypeError, ValueError):
            delay_s = 0.0

        if delay_s <= 0:
            return None

        return deferLater(reactor, delay_s, lambda: None)


class ExponentialBackoffRetryMiddleware(RetryMiddleware):
    def __init__(self, settings):
        super().__init__(settings)
        if not settings.getbool("RETRY_ENABLED"):
            raise NotConfigured

        self.backoff_base = float(settings.getfloat("RETRY_BACKOFF_BASE", 0.5))
        self.backoff_max = float(settings.getfloat("RETRY_BACKOFF_MAX", 20.0))
        self.backoff_jitter = float(settings.getfloat("RETRY_BACKOFF_JITTER", 0.2))

    def _retry(self, request, reason: str | Exception | type[Exception]):
        assert self.crawler.spider
        spider = self.crawler.spider

        new_request = get_retry_request(
            request,
            reason=reason,
            spider=spider,
            max_retry_times=request.meta.get("max_retry_times", self.max_retry_times),
            priority_adjust=request.meta.get("priority_adjust", self.priority_adjust),
        )
        if not new_request:
            return None

        retry_times = int(new_request.meta.get("retry_times") or 1)
        delay = self.backoff_base * (2 ** max(0, retry_times - 1))
        delay = min(delay, self.backoff_max)
        if self.backoff_jitter > 0:
            delay *= random.uniform(1.0 - self.backoff_jitter, 1.0 + self.backoff_jitter)

        new_request.meta["backoff_delay"] = max(0.0, float(delay))
        return new_request

