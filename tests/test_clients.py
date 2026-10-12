from datetime import UTC

import pytest
import requests
import responses

from hiring_pipeline.clients import (
    CLIENTS,
    AshbyClient,
    GreenhouseClient,
    LeverClient,
    get_client,
    make_session,
)

GH_URL = "https://boards-api.greenhouse.io/v1/boards/acme/jobs?content=true"


@pytest.fixture
def client():
    """Greenhouse client with no waiting: no rate-limit pause, no retry backoff."""
    return GreenhouseClient(session=make_session(backoff_factor=0), min_interval=0)


# --- successful and failed fetches ---------------------------------------


@responses.activate
def test_fetch_returns_snapshot_with_payload(client):
    payload = {"jobs": [{"id": 1, "title": "Data Engineer"}]}
    responses.get(GH_URL, json=payload)

    snap = client.fetch("acme")

    assert snap.board_id == "greenhouse:acme"
    assert snap.platform == "greenhouse"
    assert snap.board_slug == "acme"
    assert snap.http_status == 200
    assert snap.payload == payload
    assert snap.ok
    assert snap.fetched_at.tzinfo == UTC


@responses.activate
def test_fetch_requests_full_descriptions(client):
    responses.get(GH_URL, json={"jobs": []})
    client.fetch("acme")
    assert "content=true" in responses.calls[0].request.url


@responses.activate
def test_fetch_identifies_the_project(client):
    responses.get(GH_URL, json={"jobs": []})
    client.fetch("acme")
    assert "tech-hiring-signals" in responses.calls[0].request.headers["User-Agent"]


@responses.activate
def test_missing_board_is_recorded_not_raised(client):
    responses.get(GH_URL, status=404, json={"status": 404})
    snap = client.fetch("acme")
    assert snap.http_status == 404
    assert snap.payload is None
    assert not snap.ok


@responses.activate
def test_connection_failure_is_recorded_not_raised(client):
    responses.get(GH_URL, body=requests.ConnectionError("network down"))
    snap = client.fetch("acme")
    assert snap.http_status is None
    assert snap.payload is None


@responses.activate
def test_non_json_response_keeps_status_without_payload(client):
    responses.get(GH_URL, body="<html>maintenance</html>", status=200)
    snap = client.fetch("acme")
    assert snap.http_status == 200
    assert snap.payload is None


# --- retries -------------------------------------------------------------


@responses.activate
def test_server_error_is_retried(client):
    responses.get(GH_URL, status=503)
    responses.get(GH_URL, json={"jobs": []})
    snap = client.fetch("acme")
    assert snap.http_status == 200
    assert len(responses.calls) == 2


@responses.activate
def test_gives_up_after_retries_and_records_last_status():
    client = GreenhouseClient(
        session=make_session(retries=2, backoff_factor=0), min_interval=0
    )
    for _ in range(3):
        responses.get(GH_URL, status=500)
    snap = client.fetch("acme")
    assert snap.http_status == 500
    assert snap.payload is None
    assert len(responses.calls) == 3  # first try + 2 retries


# --- rate limiting -------------------------------------------------------


class FakeClock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


@responses.activate
def test_requests_are_spaced_by_min_interval():
    responses.get(GH_URL, json={"jobs": []})
    clock = FakeClock()
    client = GreenhouseClient(
        session=make_session(backoff_factor=0),
        min_interval=1.0,
        clock=clock,
        sleep=clock.sleep,
    )

    client.fetch("acme")  # first request: no wait
    clock.now += 0.25  # a quarter second passes
    client.fetch("acme")  # must wait the remaining 0.75s

    assert clock.sleeps == [pytest.approx(0.75)]


@responses.activate
def test_no_wait_when_enough_time_has_passed():
    responses.get(GH_URL, json={"jobs": []})
    clock = FakeClock()
    client = GreenhouseClient(
        session=make_session(backoff_factor=0),
        min_interval=1.0,
        clock=clock,
        sleep=clock.sleep,
    )
    client.fetch("acme")
    clock.now += 5
    client.fetch("acme")
    assert clock.sleeps == []


# --- client lookup -------------------------------------------------------


@pytest.mark.parametrize(
    ("platform", "expected"),
    [("greenhouse", GreenhouseClient), ("lever", LeverClient), ("ashby", AshbyClient)],
)
def test_get_client_by_platform_name(platform, expected):
    assert isinstance(get_client(platform, min_interval=0), expected)


def test_every_seed_platform_has_a_client():
    assert set(CLIENTS) == {"greenhouse", "lever", "ashby"}


def test_get_client_rejects_unknown_platform():
    with pytest.raises(ValueError, match="workday"):
        get_client("workday")


# --- Lever and Ashby -----------------------------------------------------

LEVER_URL = "https://api.lever.co/v0/postings/acme?mode=json"
ASHBY_URL = (
    "https://api.ashbyhq.com/posting-api/job-board/acme?includeCompensation=true"
)


@responses.activate
def test_lever_stores_list_payload():
    postings = [{"id": "a1", "text": "Analytics Engineer"}]
    responses.get(LEVER_URL, json=postings)
    snap = LeverClient(session=make_session(backoff_factor=0), min_interval=0).fetch(
        "acme"
    )
    assert snap.board_id == "lever:acme"
    assert snap.payload == postings  # Lever returns a bare list, kept as-is


@responses.activate
def test_lever_missing_board():
    responses.get(LEVER_URL, status=404, json={"ok": False})
    snap = LeverClient(session=make_session(backoff_factor=0), min_interval=0).fetch(
        "acme"
    )
    assert snap.http_status == 404
    assert snap.payload is None


@responses.activate
def test_ashby_requests_compensation():
    payload = {"jobs": [{"title": "Data Scientist", "compensation": {}}]}
    responses.get(ASHBY_URL, json=payload)
    snap = AshbyClient(session=make_session(backoff_factor=0), min_interval=0).fetch(
        "acme"
    )
    assert snap.board_id == "ashby:acme"
    assert snap.payload == payload
    assert "includeCompensation=true" in responses.calls[0].request.url
