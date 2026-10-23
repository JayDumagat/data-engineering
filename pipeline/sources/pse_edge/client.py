"""Paced, connection-pooled HTTP transport for PSE EDGE."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import logging
import math
import os
import threading
import time
from typing import Any, Callable, Iterable, TypeVar

from scrapling.fetchers import FetcherSession

from pipeline.core.concurrency import bounded_map


LOGGER = logging.getLogger(__name__)
ItemT = TypeVar("ItemT")
ResultT = TypeVar("ResultT")


def _configure_scrapling_logger() -> None:
    scrapling_logger = logging.getLogger("scrapling")
    scrapling_logger.setLevel(logging.WARNING)
    scrapling_logger.propagate = True
    for handler in tuple(scrapling_logger.handlers):
        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
            scrapling_logger.removeHandler(handler)


class PSEHTTPError(RuntimeError):
    """An upstream HTTP response that should not be retried."""


class PSEHTTPClient:
    """Fetch concurrently with pooled sessions and source-configurable pacing."""

    RETRYABLE_STATUSES = {408, 425, 429}

    def __init__(
        self,
        worker_limit: int | None = None,
        *,
        base_url: str = "https://www.pse.com.ph",
        user_agent: str = "Mozilla/5.0 (compatible; PSEPublicResearch/1.0)",
        concurrency_env: str = "PSE_EDGE_CONCURRENCY",
        delay_env: str = "PSE_REQUEST_DELAY_SECONDS",
        request_delay_seconds: float | None = None,
    ) -> None:
        default_delay = "0.75" if delay_env == "PSE_WEBSITE_REQUEST_DELAY_SECONDS" else "0.5"
        try:
            delay = float(
                os.getenv(delay_env, default_delay)
                if request_delay_seconds is None
                else request_delay_seconds
            )
            if not math.isfinite(delay) or delay < 0:
                raise ValueError("request delay must be a finite non-negative number")
        except ValueError:
            LOGGER.warning("Invalid %s; using %s seconds", delay_env, default_delay)
            delay = float(default_delay)
        self.delay = max(0.0, delay)
        self.max_workers = _configured_concurrency(worker_limit, concurrency_env)
        self.last_request_at = 0.0
        self._pace_lock = threading.Lock()
        self._session_lock = threading.Lock()
        self._local = threading.local()
        self._session_managers: list[Any] = []
        self._executor: ThreadPoolExecutor | None = None
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent

    def __enter__(self) -> PSEHTTPClient:
        _configure_scrapling_logger()
        self._executor = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="pse-http",
        )
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> Any:
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._executor = None
        with self._session_lock:
            managers, self._session_managers = self._session_managers, []
        for manager in managers:
            manager.__exit__(exc_type, exc_value, traceback)
        return False

    def map(
        self,
        function: Callable[[ItemT], ResultT],
        items: Iterable[ItemT],
    ) -> list[ResultT]:
        if self._executor is None:
            raise RuntimeError("PSEHTTPClient must be used inside a with block")
        return list(
            bounded_map(
                self._executor,
                function,
                items,
                max_pending=self.max_workers * 2,
            )
        )

    def get(self, url: str, headers: dict[str, str] | None = None) -> Any:
        return self._request("GET", url, headers=headers)

    def get_dynamic(
        self,
        url: str,
        *,
        wait_selector: str,
        page_action: Callable[[Any], Any] | None = None,
    ) -> Any:
        """Fetch one JavaScript-rendered source page with bounded retries."""
        if self._executor is None:
            raise RuntimeError("PSEHTTPClient must be used inside a with block")

        from scrapling.fetchers import DynamicFetcher

        last_error: Exception | None = None
        for attempt in range(3):
            self._pace()
            try:
                response = DynamicFetcher.fetch(
                    url,
                    useragent=self.user_agent,
                    timeout=60_000,
                    wait=500,
                    disable_resources=True,
                    wait_selector=wait_selector,
                    wait_selector_state="visible",
                    google_search=False,
                    page_action=page_action,
                )
                status = int(getattr(response, "status", 200) or 200)
                if status >= 400:
                    raise RuntimeError(f"HTTP {status} for {url}")
                return response
            except Exception as exc:
                last_error = exc
                if attempt < 2:
                    wait_seconds = 2**attempt
                    LOGGER.warning(
                        "Retrying dynamic PSE request: url=%s attempt=%d wait_seconds=%d error=%s",
                        url,
                        attempt + 2,
                        wait_seconds,
                        exc,
                    )
                    time.sleep(wait_seconds)

        raise RuntimeError(f"Could not render {url}: {last_error}") from last_error

    def post(
        self,
        url: str,
        *,
        form_body: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        if form_body is not None and json_body is not None:
            raise ValueError("A request cannot send both form and JSON bodies")
        return self._request(
            "POST",
            url,
            form_body=form_body,
            json_body=json_body,
            headers=headers,
        )

    def _pace(self) -> None:
        with self._pace_lock:
            wait_for = self.delay - (time.monotonic() - self.last_request_at)
            if wait_for > 0:
                time.sleep(wait_for)
            self.last_request_at = time.monotonic()

    def _get_session(self) -> Any:
        session = getattr(self._local, "session", None)
        if session is not None:
            return session

        manager = FetcherSession(
            impersonate="chrome",
            timeout=40,
            retries=0,
            headers={
                "User-Agent": self.user_agent,
                "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": self.base_url + "/",
            },
        )
        session = manager.__enter__()
        self._local.session_manager = manager
        self._local.session = session
        with self._session_lock:
            self._session_managers.append(manager)
        return session

    def _request(
        self,
        method: str,
        url: str,
        form_body: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        if self._executor is None:
            raise RuntimeError("PSEHTTPClient must be used inside a with block")

        session = self._get_session()
        last_error: Exception | None = None
        for attempt in range(3):
            self._pace()
            try:
                if method == "GET":
                    response = session.get(url, headers=headers) if headers else session.get(url)
                elif form_body is not None:
                    response = session.post(
                        url,
                        data=form_body,
                        headers={
                            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                            "X-Requested-With": "XMLHttpRequest",
                            **(headers or {}),
                        },
                    )
                elif json_body is None:
                    response = session.post(url)
                else:
                    response = session.post(
                        url,
                        json=json_body,
                        headers={"Content-Type": "application/json", **(headers or {})},
                    )

                status = int(getattr(response, "status", 0) or 0)
                if status >= 400:
                    error = RuntimeError(f"HTTP {status} for {url}")
                    if status not in self.RETRYABLE_STATUSES and status < 500:
                        raise PSEHTTPError(str(error))
                    last_error = error
                else:
                    return response
            except PSEHTTPError:
                raise
            except Exception as exc:
                last_error = exc

            if attempt < 2:
                wait_seconds = 2**attempt
                LOGGER.warning(
                    "Retrying PSE request: method=%s url=%s attempt=%d wait_seconds=%d error=%s",
                    method,
                    url,
                    attempt + 2,
                    wait_seconds,
                    last_error,
                )
                time.sleep(wait_seconds)

        raise RuntimeError(f"Could not fetch {url}: {last_error}") from last_error


class PSEEdgeClient(PSEHTTPClient):
    """PSE EDGE transport using its own endpoint and configuration names."""

    def __init__(
        self,
        worker_limit: int | None = None,
        *,
        request_delay_seconds: float | None = None,
    ) -> None:
        super().__init__(
            worker_limit=worker_limit,
            base_url=os.getenv("PSE_EDGE_BASE_URL", "https://edge.pse.com.ph"),
            user_agent=os.getenv(
                "PSE_EDGE_USER_AGENT",
                "Mozilla/5.0 (compatible; PSEEdgeResearch/1.0; +https://edge.pse.com.ph/)",
            ),
            request_delay_seconds=request_delay_seconds,
        )


def _configured_concurrency(worker_limit: int | None = None, env_name: str = "PSE_EDGE_CONCURRENCY") -> int:
    default_workers = 4 if env_name == "PSE_WEBSITE_CONCURRENCY" else 6
    try:
        configured = min(16, max(1, int(os.getenv(env_name, str(default_workers)))))
    except ValueError:
        LOGGER.warning("Invalid %s; using %d workers", env_name, default_workers)
        configured = default_workers
    if worker_limit is not None:
        return min(configured, max(1, int(worker_limit)))
    return configured
