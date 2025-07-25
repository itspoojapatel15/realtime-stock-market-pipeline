"""
Airflow DAG: Daily batch reconciliation and aggregation.

Runs after market close to:
1. Compute authoritative daily OHLCV from raw trades
2. Reconcile streaming vs batch results
3. Alert on discrepancies
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def run_daily_aggregation(**context):
    from batch.daily_aggregation import compute_daily_summary
    from datetime import date, timedelta

    target = date.today() - timedelta(days=1)
    result = compute_daily_summary(target)
    context["ti"].xcom_push(key="agg_result", value=result)
    return result


def run_reconciliation(**context):
    from batch.daily_reconciliation import reconcile_daily
    from datetime import date, timedelta

    target = date.today() - timedelta(days=1)
    report = reconcile_daily(target)
    context["ti"].xcom_push(key="recon_report", value=report)

    if report["high_severity_count"] > 0:
        raise ValueError(
            f"Reconciliation found {report['high_severity_count']} high-severity discrepancies"
        )

    return report


def alert_on_failure(**context):
    import httpx

    ti = context["ti"]
    report = ti.xcom_pull(task_ids="reconcile_stream_vs_batch", key="recon_report")

    if not report:
        return

    webhook_url = context["var"]["value"].get("slack_webhook_url", "")
    if not webhook_url:
        return

    httpx.post(
        webhook_url,
        json={
            "text": (
                f":warning: *Stock Pipeline Reconciliation Failed*\n"
                f"Date: {report['date']}\n"
                f"Discrepancies: {len(report['discrepancies'])}\n"
                f"High Severity: {report['high_severity_count']}"
            )
        },
        timeout=10,
    )


with DAG(
    dag_id="stock_daily_batch",
    default_args=default_args,
    description="Daily batch aggregation and stream/batch reconciliation",
    schedule_interval="0 21 * * 1-5",  # 9 PM ET, weekdays (after market close)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["stock", "batch", "reconciliation"],
) as dag:

    aggregate = PythonOperator(
        task_id="compute_daily_aggregation",
        python_callable=run_daily_aggregation,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt_project && dbt run --select marts",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/dbt_project && dbt test",
    )

    reconcile = PythonOperator(
        task_id="reconcile_stream_vs_batch",
        python_callable=run_reconciliation,
    )

    alert = PythonOperator(
        task_id="alert_on_discrepancies",
        python_callable=alert_on_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    aggregate >> dbt_run >> dbt_test >> reconcile >> alert
