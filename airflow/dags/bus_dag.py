from __future__ import annotations
import importlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# sentiment giữ nguyên
from dags.code.sentiment.worker import run_sentiment
# ✅ Thêm: lấy Spark session từ spark_config
from dags.code.spark_session.spark_config import get_spark_session as get_spark


def _lazy_attr(mod, attr):
    return getattr(importlib.import_module(mod), attr)

# === CALLABLES ĐÚNG TÊN HÀM ===
def crawl_tickets_callable():
    return _lazy_attr("dags.code.crawl.crawl_ticket", "crwl_ticket")()

def crawl_facilities_callable():
    return _lazy_attr("dags.code.crawl.crawl_facility", "crwl_facility")()

def crawl_reviews_callable():
    return _lazy_attr("dags.code.crawl.crawl_reviews", "crwl_reviews")()

def convert_callable(file_type):
    return _lazy_attr("dags.code.convert.to_brz", "convert")(file_type=file_type)

# ✅ Sửa: các hàm silver cần spark
def ticket_to_silver_callable():
    spark = get_spark(app_name="to_silver_ticket")
    try:
        func = _lazy_attr("dags.code.convert.to_silver", "ticket_to_silver")
        return func(spark)
    finally:
        try:
            spark.stop()
        except Exception:
            pass

def facility_to_silver_callable():
    spark = get_spark(app_name="to_silver_facility")
    try:
        func = _lazy_attr("dags.code.convert.to_silver", "facility_to_silver")
        return func(spark)
    finally:
        try:
            spark.stop()
        except Exception:
            pass

def review_to_silver_callable():
    spark = get_spark(app_name="to_silver_review")
    try:
        func = _lazy_attr("dags.code.convert.to_silver", "review_to_silver")
        return func(spark)
    finally:
        try:
            spark.stop()
        except Exception:
            pass

def update_gold_callable():
    return _lazy_attr("dags.code.convert.to_gold", "to_gold")()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2025, 5, 11),
}

with DAG(
    dag_id="bus_all_in_one_lakehouse",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=4,
    tags=["lakehouse", "etl", "kltl"],
) as dag:

    with TaskGroup("ticket_pipeline") as ticket_pipeline:
        crawl_ticket = PythonOperator(
            task_id="crawl_ticket",
            python_callable=crawl_tickets_callable,
        )
        convert_ticket = PythonOperator(
            task_id="convert_ticket",
            python_callable=convert_callable,
            op_kwargs={"file_type": "ticket"},
        )
        push_ticket = PythonOperator(
            task_id="ticket_to_silver",
            python_callable=ticket_to_silver_callable,
        )
        crawl_ticket >> convert_ticket >> push_ticket

    with TaskGroup("facility_pipeline") as facility_pipeline:
        crawl_facility = PythonOperator(
            task_id="crawl_facility",
            python_callable=crawl_facilities_callable,
        )
        convert_facility = PythonOperator(
            task_id="convert_facility",
            python_callable=convert_callable,
            op_kwargs={"file_type": "facility"},
        )
        push_facility = PythonOperator(
            task_id="facility_to_silver",
            python_callable=facility_to_silver_callable,
        )
        crawl_facility >> convert_facility >> push_facility

    with TaskGroup("review_pipeline") as review_pipeline:
        crawl_review = PythonOperator(
            task_id="crawl_review",
            python_callable=crawl_reviews_callable,
        )
        convert_review = PythonOperator(
            task_id="convert_review",
            python_callable=convert_callable,
            op_kwargs={"file_type": "review"},
        )
        sentiment_step = PythonOperator(
            task_id="run_sentiment",
            python_callable=run_sentiment,
        )
        push_review = PythonOperator(
            task_id="review_to_silver",
            python_callable=review_to_silver_callable,
        )
        crawl_review >> convert_review >> sentiment_step >> push_review

    with TaskGroup("gold_pipeline") as gold_pipeline:
        update_gold = PythonOperator(
            task_id="update_gold",
            python_callable=update_gold_callable,
        )

    (ticket_pipeline >> facility_pipeline >> review_pipeline >> gold_pipeline)