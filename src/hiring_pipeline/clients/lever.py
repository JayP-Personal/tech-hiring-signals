"""Lever job board client.

API docs: https://github.com/lever/postings-api
"""

from __future__ import annotations

from hiring_pipeline.clients.base import BoardClient


class LeverClient(BoardClient):
    platform = "lever"

    def board_url(self, slug: str) -> str:
        # Returns a bare JSON list of every published posting, with plain-text
        # descriptions included. (EU-hosted boards use api.eu.lever.co; none of
        # the tracked companies need it yet.)
        return f"https://api.lever.co/v0/postings/{slug}?mode=json"
