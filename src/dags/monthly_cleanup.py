# -*- coding: utf-8 -*-
"""DAG: amazingdata_monthly_cleanup — 每月 2 日 01:00 合并日 K 线文件。"""

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
    "python3 -m amazingdata_fetcher cleanup"
)

with DAG(
    dag_id="amazingdata_monthly_cleanup",
    default_args={
        "owner": "rollandchen",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "email_on_failure": False,
        "execution_timeout": timedelta(hours=2),
    },
    schedule="0 1 2 * *",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "cleanup", "monthly"],
    description="每月 2 日 01:00 合并日 K 线文件为月度 history 文件",
) as dag:
    BashOperator(task_id="monthly_cleanup", bash_command=_DOCKER_CMD)
