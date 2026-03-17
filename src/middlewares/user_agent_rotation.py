from __future__ import annotations

import random

from scrapy.http import Request


class UserAgentRotationMiddleware:
    def __init__(self, user_agent_list: list[str]) -> None:
        cleaned: list[str] = []
        for ua in user_agent_list or []:
            s = str(ua).strip()
            if s:
                cleaned.append(s)
        self.user_agent_list = cleaned

    @classmethod
    def from_crawler(cls, crawler):
        uas = crawler.settings.getlist("USER_AGENT_LIST")
        return cls(user_agent_list=list(uas))

    def process_request(self, request: Request, spider):
        ua = request.meta.get("user_agent")
        if ua:
            request.headers["User-Agent"] = str(ua)
            return

        if not self.user_agent_list:
            return

        request.headers["User-Agent"] = random.choice(self.user_agent_list)

