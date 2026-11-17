"""Daily collection of job board snapshots into Snowflake.

One mapped task per platform fetches that platform's boards (each platform is
rate-limited separately, and the three run in parallel), writes them to a
gzipped JSON-lines file and uploads it to a Snowflake stage. A final task loads
the run's files into RAW.JOB_BOARD_SNAPSHOTS with COPY INTO.

Only small summaries pass between tasks; the payloads go through the stage.
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, get_current_context, task

from hiring_pipeline import ingest
from hiring_pipeline.clients import get_client

log = logging.getLogger(__name__)

SNOWFLAKE_CONN_ID = "snowflake_default"
SEED_PATH = Path(
    os.environ.get("COMPANIES_SEED_PATH", "/usr/local/airflow/dbt/seeds/companies.csv")
)


# LEARNING ITEM: What is @dag?

# KEY IDEA: Parse time vs run time

# Airflow’s scheduler re-reads (parses) every file in your
#   dags/ folder every 30 seconds or so.
# When it parses this file, it does not fetch any job boards.
# It only builds a description of the work: what tasks exist, how they connect,
# and when to run them.
# The actual work happens later, when a worker executes a task.

# @dag(...) plus def collect_job_boards(): defines a DAG factory:
#   a function that, when called, builds the DAG.
# collect_job_boards() at the very bottom of the file calls that factory.
# That call is what registers the DAG with Airflow.
# Without it, the file parses fine but no DAG shows up in the UI.


@dag(
    # Unique name in the UI, the CLI, and the metadata database.
    dag_id="collect_job_boards",
    # Cron expression: in UTC: minute, hour, day-of-month, month, day-of-week
    schedule="45 12 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    catchup=False,  # postings can't be fetched for past days
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["ingest"],
)
def collect_job_boards():
    # Inside the function, the @task functions are only declared at parse time.
    # Their bodies run later, on a worker.
    @task
    def list_platforms() -> list[str]:
        # Cheap and easy. Opens a small CSV
        boards = ingest.read_boards(SEED_PATH)
        groups = ingest.group_by_platform(boards)
        log.info("Boards per platform: %s", {p: len(b) for p, b in groups.items()})
        return sorted(groups)

    @task
    def fetch_platform(platform: str) -> dict:
        run_id = get_current_context()["run_id"]
        boards = ingest.group_by_platform(ingest.read_boards(SEED_PATH))[platform]
        snapshots = ingest.collect(boards, get_client(platform))
        summary = ingest.summarize(snapshots)
        log.info("%s: %s", platform, summary)

        records = [ingest.to_record(s, run_id) for s in snapshots]
        with tempfile.TemporaryDirectory() as tmp:
            path = ingest.write_jsonl_gz(records, Path(tmp) / f"{platform}.json.gz")
            target = f"@{ingest.STAGE}/{ingest.stage_path(run_id)}/"
            hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
            with hook.get_conn() as conn, conn.cursor() as cur:
                # OVERWRITE so a retried task replaces its own file.
                cur.execute(
                    f"put file://{path} {target} auto_compress=false overwrite=true"
                )
        return {"platform": platform, **summary}

    @task
    def load_snapshots(summaries: list[dict]) -> dict:
        run_id = get_current_context()["run_id"]
        totals = {
            key: sum(s[key] for s in summaries) for key in ("boards", "ok", "failed")
        }
        log.info("Run totals: %s", totals)
        if totals["ok"] == 0:
            msg = "No board returned data; refusing to load an empty run."
            raise RuntimeError(msg)

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(ingest.delete_run_sql(), {"run_id": run_id})
            cur.execute(ingest.copy_run_sql(run_id))
            cur.execute(
                f"select count(*) from {ingest.RAW_TABLE} where run_id = %(run_id)s",
                {"run_id": run_id},
            )
            loaded = cur.fetchone()[0]

        if loaded != totals["boards"]:
            msg = f"Expected {totals['boards']} rows for {run_id}, loaded {loaded}."
            raise RuntimeError(msg)
        log.info("Loaded %s rows for %s", loaded, run_id)
        return {**totals, "loaded": loaded}

    summaries = fetch_platform.expand(platform=list_platforms())
    load_snapshots(summaries)


collect_job_boards()
