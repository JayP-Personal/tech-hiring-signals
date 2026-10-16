"""Collect job board snapshots and prepare them for loading into Snowflake.

Everything here is plain Python with no Airflow or Snowflake dependency, so it
can be unit tested. The DAG wires these steps together.

Load path: snapshots -> gzipped JSON-lines file -> Snowflake stage -> COPY INTO.
"""

from __future__ import annotations

import csv
import gzip
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from hiring_pipeline.clients import BoardClient, Snapshot

RAW_TABLE = "tech_hiring_db.raw.job_board_snapshots"
STAGE = "tech_hiring_db.raw.job_board_stage"


@dataclass(frozen=True)
class Board:
    board_id: str
    platform: str
    board_slug: str


def read_boards(seed_path: Path) -> list[Board]:
    """Active boards from the companies seed."""
    with seed_path.open(newline="", encoding="utf-8") as f:
        return [
            Board(row["board_id"], row["platform"], row["board_slug"])
            for row in csv.DictReader(f)
            if row["status"] == "active"
        ]


def group_by_platform(boards: list[Board]) -> dict[str, list[Board]]:
    groups: dict[str, list[Board]] = defaultdict(list)
    for board in boards:
        groups[board.platform].append(board)
    return dict(groups)


def collect(boards: list[Board], client: BoardClient) -> list[Snapshot]:
    """Fetch each board in turn. The client handles spacing and retries."""
    return [client.fetch(board.board_slug) for board in boards]


def to_record(snapshot: Snapshot, run_id: str) -> dict:
    """One JSON-serializable row for the raw table."""
    return {
        "board_id": snapshot.board_id,
        "platform": snapshot.platform,
        "board_slug": snapshot.board_slug,
        "run_id": run_id,
        "fetched_at": snapshot.fetched_at.isoformat(),
        "http_status": snapshot.http_status,
        "payload": snapshot.payload,
    }


def write_jsonl_gz(records: list[dict], path: Path) -> Path:
    """Write one JSON object per line, gzipped, ready for a Snowflake PUT."""
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return path


def stage_path(run_id: str) -> str:
    """Folder on the stage for one run. Airflow run ids contain ':' and '+'."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def summarize(snapshots: list[Snapshot]) -> dict[str, int]:
    return {
        "boards": len(snapshots),
        "ok": sum(s.ok for s in snapshots),
        "failed": sum(not s.ok for s in snapshots),
    }


# --- SQL ------------------------------------------------------------------
# Re-running a run first deletes its rows, then reloads every file for it, so
# a day is never loaded twice.


def delete_run_sql() -> str:
    return f"delete from {RAW_TABLE} where run_id = %(run_id)s"


def copy_run_sql(run_id: str) -> str:
    # STRIP_NULL_VALUE turns a JSON null payload (failed fetch) into SQL NULL.
    return f"""
        copy into {RAW_TABLE}
            (board_id, platform, board_slug, run_id, fetched_at, http_status, payload)
        from (
            select
                $1:board_id::varchar,
                $1:platform::varchar,
                $1:board_slug::varchar,
                $1:run_id::varchar,
                $1:fetched_at::timestamp_tz,
                $1:http_status::number,
                strip_null_value($1:payload)
            from @{STAGE}/{stage_path(run_id)}/
        )
        file_format = (type = json)
        force = true
    """
