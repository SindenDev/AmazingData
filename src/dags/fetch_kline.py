# -*- coding: utf-8 -*-
"""DAG: amazingdata_fetch_kline — 工作日 15:45 起依次拉取 etf / index / stock 日 K 线。"""

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
    "python3 -m amazingdata_fetcher fetch kline --type {ktype}; "
    "exit 0"
)

with DAG(
    dag_id="amazingdata_fetch_kline",
    default_args={
        "owner": "rollandchen",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "email_on_failure": False,
        "execution_timeout": timedelta(hours=2),
    },
    schedule="45 15 * * 1-5",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    max_active_runs=1,
    tags=["amazingdata", "kline", "daily"],
    description="工作日 15:45 起依次拉取 etf / index / stock 日 K 线",
) as dag:
    t_etf = BashOperator(
        task_id="fetch_kline_etf",
        bash_command=_DOCKER_BASE.format(ktype="etf"),
        execution_timeout=timedelta(hours=1),
    )
    t_index = BashOperator(
        task_id="fetch_kline_index",
        bash_command=_DOCKER_BASE.format(ktype="index"),
        execution_timeout=timedelta(hours=1),
    )
    t_stock = BashOperator(
        task_id="fetch_kline_stock",
        bash_command=_DOCKER_BASE.format(ktype="stock"),
        execution_timeout=timedelta(hours=2),
    )
    t_etf >> t_index >> t_stock
