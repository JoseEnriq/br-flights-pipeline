"""Daily run of the br-flights pipeline: check ANAC's files, then rebuild what changed.

The ingests compare each file's hash with the stored state, so a day without
news ends after the downloads, with no Athena query. When any ingest stores a
new version, the raw partitions are registered and dbt rebuilds the layers.
"""

from datetime import timedelta

import boto3
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

PROJECT = "/opt/br-flights"
PYTHON = "/opt/pipeline-venv/bin/python"
DBT = "/opt/pipeline-venv/bin/dbt"
TIMEZONE = "America/Sao_Paulo"

INGESTS = ["ingest_vra", "ingest_punctuality", "ingest_stats"]

ALERT_TOPIC = "br-flights-alerts"


def notify_failure(context) -> None:
    """Email the alert topic's subscribers through SNS when a task fails after its last retry."""
    ti = context["ti"]
    # The ARN is built at run time so the account number stays out of the repository
    account = boto3.client("sts").get_caller_identity()["Account"]
    boto3.client("sns").publish(
        TopicArn=f"arn:aws:sns:us-east-1:{account}:{ALERT_TOPIC}",
        Subject=f"br-flights: {ti.task_id} failed",
        Message=(
            f"Task {ti.task_id} of DAG {ti.dag_id} failed after {ti.try_number} tries.\n"
            f"Run: {ti.run_id}\n"
            f"Error: {str(context.get('exception'))[:1000]}\n"
            "Logs: Airflow web UI, port 8080"
        ),
    )


@dag(
    dag_id="br_flights",
    # 20h: when the machine running Airflow is likely to be on; a missed day is covered by the
    # next run, since every ingest checks the 13 latest months
    schedule=CronTriggerTimetable("0 20 * * *", timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 11, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10), "on_failure_callback": notify_failure},
    params={"force_stats": Param(False, type="boolean", description="Check the statistics file even if today is not Sunday")},
    tags=["br-flights"],
    doc_md=__doc__,
)
def br_flights():
    ingest_vra = BashOperator(task_id="ingest_vra", bash_command=f"cd {PROJECT} && {PYTHON} pipeline/ingest.py")
    ingest_punctuality = BashOperator(
        task_id="ingest_punctuality", bash_command=f"cd {PROJECT} && {PYTHON} pipeline/ingest_punctuality.py"
    )
    ingest_stats = BashOperator(task_id="ingest_stats", bash_command=f"cd {PROJECT} && {PYTHON} pipeline/ingest_stats.py")

    # The statistics file is 360 MB and holds monthly figures: once a week is enough.
    # Skipping only the next task leaves the decision on the others to their trigger rules.
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def is_stats_day(params=None) -> bool:
        return params["force_stats"] or pendulum.now(TIMEZONE).day_of_week == pendulum.SUNDAY

    # Each ingest prints "stored=<n>" as its last line, which BashOperator keeps as
    # its return value; a skipped ingest has none. "none_failed" lets this task run
    # when the statistics ingest was skipped.
    @task.short_circuit(trigger_rule="none_failed")
    def anything_stored(ti=None) -> bool:
        lines = [ti.xcom_pull(task_ids=task_id) for task_id in INGESTS]
        stored = sum(int(line.split("=")[1]) for line in lines if line and line.startswith("stored="))
        print(f"new or changed files: {stored}")
        return stored > 0

    repair_partitions = BashOperator(
        task_id="repair_partitions", bash_command=f"cd {PROJECT} && {PYTHON} pipeline/repair_partitions.py"
    )

    # Build files stay inside the container, apart from the ones dbt writes on the host
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROJECT}/dbt && {DBT} build --profiles-dir . --target-path /tmp/dbt/target --log-path /tmp/dbt/logs",
    )

    is_stats_day() >> ingest_stats
    [ingest_vra, ingest_punctuality, ingest_stats] >> anything_stored() >> repair_partitions >> dbt_build


br_flights()
