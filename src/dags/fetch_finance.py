# -*- coding: utf-8 -*-
"""DAG: amazingdata_fetch_finance — 工作日 05:00 依次拉取三张财务报表。"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

_DOCKER_BASE = (
    "/usr/local/bin/docker run --rm "
    "--user 1026:100 "
    "-v ./data:./data "
    "-v ./sdk_cache:./sdk_cache "
    "-v /volume1/amazingdata/logs:/app/logs "
    "-e AD_HOST -e AD_PORT -e AD_USERNAME -e AD_PASSWORD "
    "-e OUTPUT_DIR=./data "
    "-e SDK_CACHE_DIR=./sdk_cache "
    "-e PYTHONPATH=/app/src "
    "-e HOME=/tmp -e NUMBA_CACHE_DIR=/tmp/numba_cache "
    "amazingdata-fetcher:latest "
    "python3 -m amazingdata_fetcher fetch finance --statement {statement}"
)

with DAG(
    dag_id="amazingdata_fetch_finance",
    default_args={
        "owner": "rollandchen",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "email_on_failure": False,
    },
    schedule="0 5 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "finance", "daily"],
    description="工作日 05:00 依次拉取三张财务报表（各自独立 docker run）",
) as dag:
    t1 = BashOperator(
        task_id="fetch_balance_sheet",
        bash_command=_DOCKER_BASE.format(statement="balance_sheet"),
        execution_timeout=timedelta(hours=3),
    )
    t2 = BashOperator(
        task_id="fetch_cash_flow",
        bash_command=_DOCKER_BASE.format(statement="cash_flow"),
        execution_timeout=timedelta(hours=3),
    )
    t3 = BashOperator(
        task_id="fetch_income",
        bash_command=_DOCKER_BASE.format(statement="income"),
        execution_timeout=timedelta(hours=3),
    )
    t1 >> t2 >> t3
