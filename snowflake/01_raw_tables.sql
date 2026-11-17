-- Raw landing table for job board snapshots.
--
-- One row per board per fetch attempt, written by the daily Airflow DAG.
-- Rows are only ever inserted, never updated, so the table is the full
-- history of every board as it was returned on each day.
--
-- Failed fetches are recorded too (payload NULL, http_status set), so gaps
-- in collection and boards that stop responding are visible downstream.

use role accountadmin;
use database tech_hiring_db;
use schema raw;

create table if not exists job_board_snapshots (
    board_id varchar not null
    comment 'platform:board_slug, matches the companies seed',
    platform varchar not null
    comment 'greenhouse, lever or ashby',
    board_slug varchar not null
    comment 'company identifier on the platform',
    run_id varchar not null
    comment 'Airflow run that fetched the board; makes reloads idempotent',
    fetched_at timestamp_tz not null
    comment 'when the API request was made (UTC)',
    http_status number
    comment 'HTTP status; NULL if the request never completed',
    payload variant
    comment 'full API response, unmodified; NULL when the fetch failed',
    loaded_at timestamp_tz default current_timestamp()
    comment 'when the row was inserted'
)
comment = 'Daily snapshots of public job board API responses (append-only)';

-- The loader writes (delete is only for re-running a failed run); dbt reads.
grant select, insert, delete on table job_board_snapshots to role loader;
grant select on table job_board_snapshots to role transformer;

-- Internal stage for per-run files. The DAG uploads one gzipped JSON-lines
-- file per platform per run, then loads them with COPY INTO.
create stage if not exists job_board_stage
    file_format = (type = json)
    comment = 'Per-run JSON-lines files of job board snapshots';

grant read, write on stage job_board_stage to role loader;