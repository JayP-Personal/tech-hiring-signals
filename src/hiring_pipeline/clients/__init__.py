"""Job board API clients, one per platform."""

from hiring_pipeline.clients.ashby import AshbyClient
from hiring_pipeline.clients.base import BoardClient, Snapshot, make_session
from hiring_pipeline.clients.greenhouse import GreenhouseClient
from hiring_pipeline.clients.lever import LeverClient

CLIENTS: dict[str, type[BoardClient]] = {
    client.platform: client for client in (GreenhouseClient, LeverClient, AshbyClient)
}


def get_client(platform: str, **kwargs) -> BoardClient:
    """Client for a platform name from the companies seed."""
    try:
        return CLIENTS[platform](**kwargs)
    except KeyError:
        msg = f"No client for platform {platform!r}; known: {sorted(CLIENTS)}"
        raise ValueError(msg) from None


__all__ = [
    "CLIENTS",
    "AshbyClient",
    "BoardClient",
    "GreenhouseClient",
    "LeverClient",
    "Snapshot",
    "get_client",
    "make_session",
]
