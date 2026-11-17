import gzip
import json
from datetime import UTC, datetime

import pytest

from hiring_pipeline.clients import Snapshot
from hiring_pipeline.ingest import (
    Board,
    collect,
    copy_run_sql,
    group_by_platform,
    read_boards,
    stage_path,
    summarize,
    to_record,
    write_jsonl_gz,
)

FETCHED_AT = datetime(2026, 10, 16, 13, 0, tzinfo=UTC)


def snapshot(slug="acme", status=200, payload=None):
    return Snapshot(
        f"greenhouse:{slug}", "greenhouse", slug, FETCHED_AT, status, payload
    )


@pytest.fixture
def seed(tmp_path):
    path = tmp_path / "companies.csv"
    path.write_text(
        "board_id,company_name,platform,board_slug,stratum,source_list,added_on,status\n"
        "greenhouse:acme,Acme,greenhouse,acme,early,YC,2026-10-10,active\n"
        "lever:globex,Globex,lever,globex,public,S&P,2026-10-10,active\n"
        "ashby:initech,Initech,ashby,initech,growth,Forbes,2026-10-10,inactive\n",
        encoding="utf-8",
    )
    return path


def test_read_boards_skips_inactive(seed):
    assert read_boards(seed) == [
        Board("greenhouse:acme", "greenhouse", "acme"),
        Board("lever:globex", "lever", "globex"),
    ]


def test_group_by_platform(seed):
    groups = group_by_platform(read_boards(seed))
    assert set(groups) == {"greenhouse", "lever"}
    assert [b.board_slug for b in groups["lever"]] == ["globex"]


def test_collect_fetches_every_board():
    class FakeClient:
        def __init__(self):
            self.slugs = []

        def fetch(self, slug):
            self.slugs.append(slug)
            return snapshot(slug)

    client = FakeClient()
    boards = [
        Board("greenhouse:a", "greenhouse", "a"),
        Board("greenhouse:b", "greenhouse", "b"),
    ]
    result = collect(boards, client)
    assert client.slugs == ["a", "b"]
    assert [s.board_slug for s in result] == ["a", "b"]


def test_to_record_matches_raw_table_columns():
    record = to_record(snapshot(payload={"jobs": []}), "scheduled__2026-10-16")
    assert record == {
        "board_id": "greenhouse:acme",
        "platform": "greenhouse",
        "board_slug": "acme",
        "run_id": "scheduled__2026-10-16",
        "fetched_at": "2026-10-16T13:00:00+00:00",
        "http_status": 200,
        "payload": {"jobs": []},
    }


def test_failed_fetch_record_has_null_payload():
    record = to_record(snapshot(status=None), "run")
    assert record["http_status"] is None
    assert record["payload"] is None


def test_write_jsonl_gz_round_trips(tmp_path):
    records = [
        to_record(snapshot("a", payload={"jobs": [{"title": "Ingénieur"}]}), "r")
    ]
    path = write_jsonl_gz(records, tmp_path / "out.json.gz")
    with gzip.open(path, "rt", encoding="utf-8") as f:
        lines = f.read().splitlines()
    assert [json.loads(line) for line in lines] == records


def test_stage_path_is_safe_for_airflow_run_ids():
    run_id = "scheduled__2026-10-16T13:00:00+00:00"
    assert stage_path(run_id) == "scheduled__2026-10-16T13_00_00_00_00"


def test_copy_sql_reads_only_this_runs_folder():
    sql = copy_run_sql("manual__2026-10-16T13:00:00+00:00")
    assert "/manual__2026-10-16T13_00_00_00_00/" in sql
    assert "strip_null_value($1:payload)" in sql


def test_summarize_counts_failures():
    snaps = [snapshot("a", payload={"jobs": []}), snapshot("b", status=404)]
    assert summarize(snaps) == {"boards": 2, "ok": 1, "failed": 1}
