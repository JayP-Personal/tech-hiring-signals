"""Ashby job board client.

API docs: https://developers.ashbyhq.com/docs/public-job-posting-api
"""

from __future__ import annotations

from hiring_pipeline.clients.base import BoardClient


class AshbyClient(BoardClient):
    platform = "ashby"

    def board_url(self, slug: str) -> str:
        # includeCompensation=true adds structured salary ranges where the
        # company publishes them.
        return (
            f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
            "?includeCompensation=true"
        )
