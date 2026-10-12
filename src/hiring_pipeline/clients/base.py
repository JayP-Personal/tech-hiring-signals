"""Shared behavior for job board API clients.

Each platform client only says how to build its URL. Fetching, retries, rate
limiting and error handling live here, so every platform behaves the same.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USER_AGENT = "tech-hiring-signals (portfolio research project)"
RETRY_STATUSES = (429, 500, 502, 503, 504)


@dataclass(frozen=True)
class Snapshot:
    """One fetch of one board, shaped like a row of RAW.JOB_BOARD_SNAPSHOTS.

    The loader adds run_id; everything else comes from the fetch.
    """

    board_id: str
    platform: str
    board_slug: str
    fetched_at: datetime
    http_status: int | None  # None if the request never completed
    payload: Any  # parsed JSON; None unless the fetch succeeded

    @property
    def ok(self) -> bool:
        return self.payload is not None


def make_session(retries: int = 3, backoff_factor: float = 2.0) -> requests.Session:
    """Session that retries rate limits and server errors with exponential backoff.

    Honors Retry-After headers on 429 and 503 responses.
    """
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=RETRY_STATUSES,
        allowed_methods=("GET",),
        raise_on_status=False,  # return the last response instead of raising
    )
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


class BoardClient(ABC):
    """Fetches one company's job board from a platform's public API.

    fetch() never raises for a single board's failure: problems are recorded
    in the returned Snapshot, so one bad board can't stop a daily run.
    """

    platform: ClassVar[str]
    timeout_seconds: ClassVar[float] = 30.0

    def __init__(
        self,
        session: requests.Session | None = None,
        min_interval: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.session = session or make_session()
        self.min_interval = min_interval  # seconds between requests
        self._clock = clock
        self._sleep = sleep
        self._last_request: float | None = None

    # LEARNED! add abstractmethod to a parent class method to ensure
    # guarantee any child classes contains the necessary method
    @abstractmethod
    def board_url(self, slug: str) -> str:
        """Public API URL for one company's board."""

    def fetch(self, slug: str) -> Snapshot:
        self._throttle()
        fetched_at = datetime.now(UTC)
        try:
            response = self.session.get(
                self.board_url(slug), timeout=self.timeout_seconds
            )
        except requests.RequestException:
            return self._snapshot(slug, fetched_at, None, None)

        payload = None
        if response.ok:
            try:
                payload = response.json()
            except ValueError:  # 200 but not JSON: keep the status, no payload
                payload = None
        return self._snapshot(slug, fetched_at, response.status_code, payload)

    def _throttle(self) -> None:
        """Wait so requests are at least min_interval seconds apart."""
        now = self._clock()
        if self._last_request is not None:
            wait = self.min_interval - (now - self._last_request)
            if wait > 0:
                self._sleep(wait)
                now += wait
        self._last_request = now

    def _snapshot(
        self, slug: str, fetched_at: datetime, status: int | None, payload: Any
    ) -> Snapshot:
        return Snapshot(
            board_id=f"{self.platform}:{slug}",
            platform=self.platform,
            board_slug=slug,
            fetched_at=fetched_at,
            http_status=status,
            payload=payload,
        )
