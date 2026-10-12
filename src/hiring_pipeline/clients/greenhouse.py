"""Greenhouse job board client.

API docs: https://developers.greenhouse.io/job-board.html
"""

from __future__ import annotations

from hiring_pipeline.clients.base import BoardClient


class GreenhouseClient(BoardClient):
    platform = "greenhouse"

    def board_url(self, slug: str) -> str:
        # content=true includes each posting's full description, needed for
        # skill extraction later.
        return f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
