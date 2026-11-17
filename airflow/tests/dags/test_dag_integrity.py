"""Every DAG must import cleanly. Run inside the Airflow container: astro dev pytest"""

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


def test_collect_job_boards_is_loaded(dagbag):
    dag = dagbag.get_dag("collect_job_boards")
    assert dag is not None
    assert {t.task_id for t in dag.tasks} == {
        "list_platforms",
        "fetch_platform",
        "load_snapshots",
    }
