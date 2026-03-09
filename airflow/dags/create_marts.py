from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime as dt, timedelta

POSTGRES_CONN_ID = "postgres_default"
SCHEMA = "marts"


def build_sales_mart():
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.sales_by_day")
    cur.execute(f"""
    CREATE TABLE {SCHEMA}.sales_by_day AS
    SELECT
        DATE(order_time) as order_date,
        COUNT(*) as orders_count,
        SUM(total) as revenue,
        AVG(total) as avg_check
    FROM ods.orders
    GROUP BY DATE(order_time)
    """)
    conn.commit()
    cur.close()


def build_product_mart():
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    cur.execute(f"DROP TABLE IF EXISTS {SCHEMA}.product_stats")
    cur.execute(f"""
    CREATE TABLE {SCHEMA}.product_stats AS
    SELECT
        p.id as product_id,
        p.name,
        p.category,
        COUNT(op.product_ids) as sales_count,
        AVG(r.rating) as avg_rating,
        COUNT(r.rating) as reviews_count
    FROM ods.products p
    LEFT JOIN ods.orders_product_ids op
        ON p.id = op.product_ids::INT
    LEFT JOIN ods.reviews r
        ON p.id = r.product_id
    GROUP BY p.id, p.name, p.category
    """)
    conn.commit()
    cur.close()


default_args = {
    "owner": "alex",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="build_data_marts",
    default_args=default_args,
    description="Создание витрин данных",
    schedule="0 4 * * *",
    start_date=dt(2026, 2, 1),
    catchup=False,
    tags=["marts", "postgres"],
) as dag:

    sales_mart = PythonOperator(
        task_id="build_sales_by_day", python_callable=build_sales_mart
    )

    product_mart = PythonOperator(
        task_id="build_product_stats", python_callable=build_product_mart
    )

    sales_mart >> product_mart
