from datetime import datetime

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

CONN_ID = "snowflake_default"
DBT_BIN = "/usr/local/airflow/dbt_venv/bin/dbt"
DBT_PROJECT = "/usr/local/airflow/dbt"


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["smoke"],
    default_args={"retries": 2},
)
def stack_smoke():
    @task
    def import_package() -> str:
        import hiring_pipeline

        location = str(list(hiring_pipeline.__path__))
        print(f"hiring_pipeline loaded from {location}")
        return location

    @task
    def check_snowflake_session() -> dict:
        from hiring_pipeline.smoke import check_snowflake_session

        conn = SnowflakeHook(snowflake_conn_id=CONN_ID).get_conn()
        try:
            info = check_snowflake_session(conn)
        finally:
            conn.close()

        print(info)
        if info["warehouse"] is None:
            raise ValueError("No active warehouse: check role grants and conn config")
        if info["role"] != "LOADER":
            raise ValueError(f"Expected role LOADER, got {info['role']}")
        return info

    @task
    def write_and_read() -> int:
        from hiring_pipeline.smoke import write_snowflake_smoke_row

        conn = SnowflakeHook(snowflake_conn_id=CONN_ID).get_conn()
        try:
            count = write_snowflake_smoke_row(conn, source="airflow")
        finally:
            conn.close()

        print(f"Rows written by Airflow so far: {count}")
        return count

    @task.bash
    def dbt_build() -> str:
        return f"{DBT_BIN} build --select smoke_check --project-dir {DBT_PROJECT}"

    imported = import_package()
    imported >> check_snowflake_session() >> write_and_read() >> dbt_build()


stack_smoke()
