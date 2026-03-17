from __future__ import annotations


def scrapy_settings(*, concurrency: int = 18, download_delay: float = 0.1, retry_times: int = 6,
    download_timeout: int = 30, log_level: str = "INFO", log_file: str | None = None) -> dict:
    settings: dict = {
        "ROBOTSTXT_OBEY": False,
        "COOKIES_ENABLED": True,
        "TELNETCONSOLE_ENABLED": False,
        "LOG_LEVEL": log_level,
        "CONCURRENT_REQUESTS": max(1, int(concurrency)),
        "DOWNLOAD_DELAY": max(0.0, float(download_delay)),
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": max(0.0, float(download_delay)),
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1.0,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": max(0, int(retry_times)),
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504],
        "RETRY_BACKOFF_BASE": 0.5,
        "RETRY_BACKOFF_MAX": 20.0,
        "RETRY_BACKOFF_JITTER": 0.2,
        "DOWNLOAD_TIMEOUT": max(1, int(download_timeout)),
        "HTTPERROR_ALLOW_ALL": True,
        "USER_AGENT_LIST": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/142.0.0.0 Safari/537.36",
        ],
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ru-RU,ru;q=0.9",
        },
        "DNS_TIMEOUT": 20,
        "DOWNLOAD_MAXSIZE": 0,
        "ITEM_PIPELINES": {
            "src.pipelines.sqlalchemy_pipeline.SQLAlchemyPipeline": 300,
        },
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,
            "src.middlewares.retry_backoff.RetryBackoffDelayMiddleware": 541,
            "src.middlewares.retry_backoff.ExponentialBackoffRetryMiddleware": 550,
            "src.middlewares.user_agent_rotation.UserAgentRotationMiddleware": 420,
            "src.middlewares.fedresurs_headers.FedresursHeadersMiddleware": 543,
        },
    }
    if log_file:
        settings["LOG_FILE"] = log_file
    return settings

