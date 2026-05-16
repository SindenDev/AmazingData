# -*- coding: utf-8 -*-
"""DAG: amazingdata_fetch_equity — 工作日 03:30 拉取股本结构与分红数据。"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

_DOCKER_CMD = (
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
    "python3 -m amazingdata_fetcher fetch equity"
)

with DAG(
    dag_id="amazingdata_fetch_equity",
    default_args={
        "owner": "rollandchen",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "email_on_failure": False,
        "execution_timeout": timedelta(hours=2),
    },
    schedule="30 3 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "equity", "daily"],
    description="工作日 03:30 拉取 equity_structure_history 和 equity_dividend_history",
) as dag:
    BashOperator(task_id="fetch_equity", bash_command=_DOCKER_CMD)
